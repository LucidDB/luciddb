/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/
package net.sf.farrago.ddl;

import java.sql.*;
import java.util.*;
import java.util.logging.*;

import javax.jmi.reflect.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.datatypes.*;
import net.sf.farrago.cwm.keysindexes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.relational.enumerations.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.util.*;
import net.sf.farrago.query.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.sql.*;
import org.eigenbase.util.*;
import org.netbeans.api.mdr.events.*;


/**
 * DdlValidator validates the process of applying a DDL statement to the
 * catalog.  By implementing MDRPreChangeListener, it is able to
 * automatically collect references to all objects modified by the statement.
 * (MDRChangeListener isn't suitable since it's asynchronous.)
 *
 * <p>
 * Generic validation support is implemented in this class, but
 * object-specific rules should be implemented in the Impl classes for the
 * appropriate catalog objects.
 * </p>
 *
 * NOTE:  all validation activity must take place in the same thread in which
 * DdlValidator's constructor is called.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DdlValidator extends FarragoCompoundAllocation
    implements FarragoSessionDdlValidator,
        MDRPreChangeListener
{
    //~ Static fields/initializers --------------------------------------------

    private static final Logger tracer = FarragoTrace.getDdlValidatorTracer();

    /** Symbolic constant used to mark an element being created */
    private static final Integer VALIDATE_CREATION = new Integer(1);

    /** Symbolic constant used to mark an element being updated */
    private static final Integer VALIDATE_MODIFICATION = new Integer(2);

    /** Symbolic constant used to mark an element being deleted */
    private static final Integer VALIDATE_DELETION = new Integer(3);

    /** Symbolic constant used to mark an element being truncated */
    private static final Integer VALIDATE_TRUNCATION = new Integer(4);

    //~ Instance fields -------------------------------------------------------

    /**
     * A public instance of FarragoResource for use in throwing vallidation
     * errors.  The name is intentionally short to keep line length under
     * control.
     */
    public final FarragoResource res;
    private final FarragoSessionStmtValidator stmtValidator;

    /** Queue of excns detected during plannedChange. */
    private DeferredException enqueuedValidationExcn;

    /**
     * Map (from RefAssociation.Class to DropRule) of associations for which
     * special handling is required during DROP.
     */
    private MultiMap dropRules;

    /** Map from parsed object to ParserContext. */
    private final Map parserContextMap;

    /**
     * Map containing scheduled validation actions.  The key is the MofId of
     * the object scheduled for validation; the value is the action type (one
     * of the symbols {@link #VALIDATE_CREATION}, {@link #VALIDATE_DELETION},
     * {@link #VALIDATE_MODIFICATION}, {@link #VALIDATE_TRUNCATION}).
     */
    private Map schedulingMap;

    /**
     * Map of objects in transition between schedulingMap and validatedMap.
     * Content format is same as for schedulingMap.
     */
    private Map transitMap;

    /**
     * Map of object validations which have already taken place.
     * The key is the RefObject itself; the value is the action type.
     */
    private Map validatedMap;

    /**
     * Set of objects which a DROP CASCADE has encountered but not yet
     * processed.
     */
    private Set deleteQueue;

    /**
     * Thread binding to prevent cross-talk.
     */
    private Thread activeThread;

    /**
     * DDL statement being validated.
     */
    private FarragoSessionDdlStmt ddlStmt;

    /**
     * List of handlers to be invoked.
     */
    protected List actionHandlers;

    //~ Constructors ----------------------------------------------------------

    /**
     * Create a new validator.  The validator will listen for repository
     * change events and schedule appropriate validation actions on the
     * affected objects.  Validation is deferred until validate() is called.
     *
     * @param stmtValidator generic stmt validator
     */
    public DdlValidator(FarragoSessionStmtValidator stmtValidator)
    {
        this.stmtValidator = stmtValidator;
        stmtValidator.addAllocation(this);

        // NOTE jvs 25-Jan-2004:  Use LinkedHashXXX everywhere, since order
        // matters in some cases.
        schedulingMap = new LinkedHashMap();
        validatedMap = new LinkedHashMap();
        parserContextMap = new LinkedHashMap();
        deleteQueue = new LinkedHashSet();

        dropRules = new MultiMap();

        // When a table is dropped, all indexes on the table should also be
        // implicitly dropped.
        addDropRule(
            getRepos().indexPackage.getIndexSpansClass(),
            new DropRule("spannedClass", null,
                ReferentialRuleTypeEnum.IMPORTED_KEY_CASCADE));

        // Dependencies can never be dropped without CASCADE, but with
        // CASCADE, they go away (a special case later on takes care of
        // cascading to the dependent object as well).
        addDropRule(
            getRepos().corePackage.getDependencySupplier(),
            new DropRule("supplier", null,
                ReferentialRuleTypeEnum.IMPORTED_KEY_RESTRICT));

        // When a dependency gets dropped, take its owner (the client)
        // down with it.
        addDropRule(
            getRepos().corePackage.getElementOwnership(),
            new DropRule("ownedElement", CwmDependency.class,
                ReferentialRuleTypeEnum.IMPORTED_KEY_CASCADE));

        // Without CASCADE, a schema can only be dropped when it is empty.
        // This is not true for other namespaces (e.g. a table's constraints
        // are dropped implicitly), so we specify the superInterface filter.
        addDropRule(
            getRepos().corePackage.getElementOwnership(),
            new DropRule("namespace", CwmSchema.class,
                ReferentialRuleTypeEnum.IMPORTED_KEY_RESTRICT));

        // MDR pre-change instance creation events are useless, since they
        // don't refer to the new instance.  Instead, we rely on the
        // fact that it's pretty much guaranteed that the new object will have
        // attributes or associations set (though someone will probably come up
        // with a pathological case eventually).
        activeThread = Thread.currentThread();
        getRepos().getMdrRepos().addListener(this,
            InstanceEvent.EVENT_INSTANCE_DELETE
                | AttributeEvent.EVENTMASK_ATTRIBUTE
                | AssociationEvent.EVENTMASK_ASSOCIATION);

        res = FarragoResource.instance();
    }

    //~ Methods ---------------------------------------------------------------

    // implement FarragoSessionDdlValidator
    public List defineHandlers()
    {
        List list = new ArrayList();
        list.add(new DdlHandler(this));
        return list;
    }
    
    // implement FarragoSessionDdlValidator
    public FarragoSessionStmtValidator getStmtValidator()
    {
        return stmtValidator;
    }

    // implement FarragoSessionDdlValidator
    public FarragoRepos getRepos()
    {
        return stmtValidator.getRepos();
    }

    // implement FarragoSessionDdlValidator
    public FennelDbHandle getFennelDbHandle()
    {
        return stmtValidator.getFennelDbHandle();
    }

    // implement FarragoSessionDdlValidator
    public FarragoTypeFactory getTypeFactory()
    {
        return stmtValidator.getTypeFactory();
    }

    // implement FarragoSessionDdlValidator
    public FarragoSessionIndexMap getIndexMap()
    {
        return stmtValidator.getIndexMap();
    }

    // implement FarragoSessionDdlValidator
    public FarragoDataWrapperCache getDataWrapperCache()
    {
        return stmtValidator.getDataWrapperCache();
    }

    // implement FarragoSessionDdlValidator
    public FarragoSession newReentrantSession()
    {
        return getInvokingSession().cloneSession();
    }

    // implement FarragoSessionDdlValidator
    public FarragoSession getInvokingSession()
    {
        return stmtValidator.getSession();
    }

    // implement FarragoSessionDdlValidator
    public void releaseReentrantSession(FarragoSession session)
    {
        session.closeAllocation();
    }

    // implement FarragoSessionDdlValidator
    public FarragoSessionParser getParser()
    {
        return stmtValidator.getParser();
    }

    // implement FarragoSessionDdlValidator
    public FarragoSessionVariables getSessionVariables()
    {
        return stmtValidator.getSessionVariables();
    }

    // implement FarragoSessionDdlValidator
    public boolean isDeletedObject(RefObject refObject)
    {
        return testObjectStatus(refObject, VALIDATE_DELETION);
    }

    // implement FarragoSessionDdlValidator
    public boolean isCreatedObject(RefObject refObject)
    {
        return testObjectStatus(refObject, VALIDATE_CREATION);
    }

    private boolean testObjectStatus(
        RefObject refObject,
        Integer status)
    {
        return (schedulingMap.get(refObject.refMofId()) == status)
            || (validatedMap.get(refObject) == status)
            || ((transitMap != null)
            && (transitMap.get(refObject.refMofId()) == status));
    }

    // implement FarragoSessionDdlValidator
    public boolean isDropRestrict()
    {
        return ddlStmt.isDropRestricted();
    }

    /**
     * Determine whether a catalog object has had its visibility set yet.
     * NOTE: this should remain private and only be called prior to
     * executeStorage().
     *
     * @param modelElement object in question
     *
     * @return true if refObject isn't visible yet
     */
    private boolean isNewObject(CwmModelElement modelElement)
    {
        return modelElement.getVisibility() != VisibilityKindEnum.VK_PUBLIC;
    }

    // implement FarragoSessionDdlValidator
    public String getParserPosString(RefObject obj)
    {
        FarragoSessionParserPosition parserContext =
            (FarragoSessionParserPosition) parserContextMap.get(obj);
        if (parserContext == null) {
            return null;
        }
        return parserContext.toString();
    }

    // implement FarragoSessionDdlValidator
    public void setSchemaObjectName(
        CwmSchema schema,
        CwmModelElement schemaElement,
        SqlIdentifier qualifiedName)
    {
        String schemaName = null;
        assert (qualifiedName.names.length > 0);
        assert (qualifiedName.names.length < 3);

        // TODO:  support catalog names
        if (qualifiedName.names.length == 2) {
            schemaElement.setName(qualifiedName.names[1]);
            schemaName = qualifiedName.names[0];
        } else {
            schemaElement.setName(qualifiedName.names[0]);
            if (schema == null) {
                if (getSessionVariables().schemaName == null) {
                    throw res.newValidatorNoDefaultSchema();
                }
                schemaName = getSessionVariables().schemaName;
            }
        }
        if (schema == null) {
            schema =
                stmtValidator.findSchema(new SqlIdentifier(schemaName, null));
        }
        schema.getOwnedElement().add(schemaElement);
    }

    // implement MDRChangeListener
    public void change(MDRChangeEvent event)
    {
        // don't care
    }

    // implement MDRPreChangeListener
    public void changeCancelled(MDRChangeEvent event)
    {
        // don't care
    }

    // override FarragoCompoundAllocation
    public void closeAllocation()
    {
        stopListening();
        super.closeAllocation();
    }

    // implement FarragoSessionDdlValidator
    public void executeStorage()
    {
        // catalog updates which occur during execution should not result in
        // further validation
        stopListening();

        List deletionList = new ArrayList();
        Iterator mapIter = validatedMap.entrySet().iterator();
        while (mapIter.hasNext()) {
            Map.Entry mapEntry = (Map.Entry) mapIter.next();
            RefObject obj = (RefObject) mapEntry.getKey();
            Object action = mapEntry.getValue();

            if (obj instanceof CwmModelElement) {
                CwmModelElement modelElement = (CwmModelElement) obj;

                // We use the visibility attribute to distinguish new objects
                // from existing objects.  Here's the transition.
                modelElement.setVisibility(VisibilityKindEnum.VK_PUBLIC);
            }

            if (obj instanceof CwmStructuralFeature) {
                // Set some mandatory but irrelevant attributes.
                CwmStructuralFeature feature = (CwmStructuralFeature) obj;

                feature.setChangeability(ChangeableKindEnum.CK_CHANGEABLE);
            }

            JmiUtil.assertConstraints(obj);

            if (action == VALIDATE_DELETION) {
                clearDependencySuppliers(obj);
                RefFeatured container = obj.refImmediateComposite();
                if (container != null) {
                    Object containerAction = validatedMap.get(container);
                    if (containerAction == VALIDATE_DELETION) {
                        // container is also being deleted; don't try
                        // deleting this contained object--depending on order,
                        // the attempt could cause an excn
                    } else {
                        // container is not being deleted
                        deletionList.add(obj);
                    }
                } else {
                    // top-level object
                    deletionList.add(obj);
                }
            }
            if (!(obj instanceof CwmModelElement)) {
                continue;
            }
            CwmModelElement element = (CwmModelElement) obj;
            if (action == VALIDATE_CREATION) {
                invokeHandler(element, "executeCreation");
            } else if (action == VALIDATE_DELETION) {
                invokeHandler(element, "executeDrop");
            } else if (action == VALIDATE_TRUNCATION) {
                invokeHandler(element, "executeTruncation");
            } else if (action == VALIDATE_MODIFICATION) {
                invokeHandler(element, "executeModification");
            } else {
                assert (false);
            }
        }

        // now we can finally consummate any requested deletions, since we're
        // all done referencing the objects
        Collections.reverse(deletionList);
        Iterator deletionIter = deletionList.iterator();
        while (deletionIter.hasNext()) {
            RefObject refObj = (RefObject) deletionIter.next();
            if (tracer.isLoggable(Level.FINE)) {
                tracer.fine("really deleting " + refObj);
            }
            refObj.refDelete();
        }
    }

    // implement MDRPreChangeListener
    public void plannedChange(MDRChangeEvent event)
    {
        if (tracer.isLoggable(Level.FINE)) {
            tracer.fine(event.toString());
        }
        if (activeThread != Thread.currentThread()) {
            // REVIEW:  This isn't going to be good enough if we have
            // reentrant DDL.
            // ignore events from other threads
            return;
        }

        // NOTE:  do not throw exceptions from this method, because MDR will
        // swallow them!  Instead, use enqueueValidationExcn(), and the
        // exception will be thrown when validate() is called.
        if (event.getType() == InstanceEvent.EVENT_INSTANCE_DELETE) {
            RefObject obj = (RefObject) (event.getSource());
            clearDependencySuppliers(obj);
            scheduleDeletion(obj);
        } else if (event instanceof AttributeEvent) {
            RefObject obj = (RefObject) (event.getSource());
            scheduleModification(obj);
        } else if (event instanceof AssociationEvent) {
            AssociationEvent associationEvent = (AssociationEvent) event;
            if (tracer.isLoggable(Level.FINE)) {
                tracer.fine("end name = " + associationEvent.getEndName());
            }
            scheduleModification(associationEvent.getFixedElement());
            if (associationEvent.getOldElement() != null) {
                scheduleModification(associationEvent.getNewElement());
            }
            if (associationEvent.getNewElement() != null) {
                scheduleModification(associationEvent.getNewElement());
            }
            if (event.getType() == AssociationEvent.EVENT_ASSOCIATION_REMOVE) {
                RefAssociation refAssoc =
                    (RefAssociation) associationEvent.getSource();
                List rules = dropRules.getMulti(refAssoc.getClass());
                Iterator ruleIter = rules.iterator();
                while (ruleIter.hasNext()) {
                    DropRule rule = (DropRule) ruleIter.next();
                    if ((rule != null)
                            && rule.endName.equals(
                                associationEvent.getEndName())) {
                        fireDropRule(
                            rule,
                            associationEvent.getFixedElement(),
                            associationEvent.getOldElement());
                    }
                }
            }
        } else {
            // REVIEW:  when I turn this on, I see notifications for
            // CreateInstanceEvent, which is supposed to be masked out.
            // Probably an MDR bug.

            /*
               tracer.warning(
                   "Unexpected event "+event.getClass().getName()
                   + ", type="+event.getType());
               assert(false);
            */
        }
    }

    // implement FarragoSessionDdlValidator
    public void scheduleTruncation(CwmModelElement modelElement)
    {
        assert (!validatedMap.containsKey(modelElement));
        assert (!schedulingMap.containsKey(modelElement.refMofId()));
        schedulingMap.put(
            modelElement.refMofId(),
            VALIDATE_TRUNCATION);
        mapParserPosition(modelElement);
    }

    // implement FarragoSessionDdlValidator
    public void validate(FarragoSessionDdlStmt ddlStmt)
    {
        this.ddlStmt = ddlStmt;
        ddlStmt.preValidate(this);
        checkValidationExcnQueue();

        actionHandlers = defineHandlers();
        
        if (ddlStmt instanceof DdlDropStmt) {
            // Process deletions until a fixpoint is reached, using MDR events
            // to implement RESTRICT/CASCADE.
            while (!deleteQueue.isEmpty()) {
                RefObject refObj = (RefObject) deleteQueue.iterator().next();
                deleteQueue.remove(refObj);
                if (!schedulingMap.containsKey(refObj.refMofId())) {
                    if (tracer.isLoggable(Level.FINE)) {
                        tracer.fine("probe deleting " + refObj);
                    }
                    refObj.refDelete();
                }
            }

            // In order to validate deletion, we need the objects to exist, but
            // they've already been deleted.  So rollback the deletion now, but
            // we still remember the objects encountered above.
            rollbackDeletions();

            // REVIEW:  This may need to get more complicated in the future.
            // Like, if deletions triggered modifications to some referencing
            // object which needs to have its update validation run AFTER the
            // deletion takes place, not before.
        }

        while (!schedulingMap.isEmpty()) {
            checkValidationExcnQueue();

            // Swap in a new map so new scheduling calls aren't handled until
            // the next round.
            transitMap = schedulingMap;
            schedulingMap = new LinkedHashMap();

            boolean progress = false;
            Iterator mapIter = transitMap.entrySet().iterator();
            while (mapIter.hasNext()) {
                Map.Entry mapEntry = (Map.Entry) mapIter.next();
                RefObject obj =
                    (RefObject) getRepos().getMdrRepos().getByMofId((String) mapEntry
                            .getKey());
                Object action = mapEntry.getValue();

                // mark this object as already validated so it doesn't slip
                // back in by updating itself
                validatedMap.put(obj, action);

                if (!(obj instanceof CwmModelElement)) {
                    progress = true;
                    continue;
                }
                CwmModelElement element = (CwmModelElement) obj;
                try {
                    validateAction(element, action);
                    if (element.getVisibility() == null) {
                        // mark this new element as validated
                        element.setVisibility(VisibilityKindEnum.VK_PRIVATE);
                    }
                    progress = true;
                } catch (FarragoUnvalidatedDependencyException ex) {
                    // Something hit an unvalidated dependency; we'll have
                    // to retry this object later.
                    validatedMap.remove(obj);
                    schedulingMap.put(
                        obj.refMofId(),
                        action);
                }
            }
            transitMap = null;
            if (!progress) {
                // Every single object hit a
                // FarragoUnvalidatedDependencyException.  This implies a
                // cycle.  TODO:  identify the cycle in the exception.
                throw res.newValidatorSchemaDependencyCycle();
            }
        }

        // one last time
        checkValidationExcnQueue();
    }

    private void clearDependencySuppliers(RefObject refObj)
    {
        // REVIEW: We have to break dependencies explicitly
        // before object deletion, or all kinds of nasty internal
        // MDR errors result.  Should find out if that's expected
        // behavior.
        if (refObj instanceof CwmDependency) {
            CwmDependency dependency = (CwmDependency) refObj;
            dependency.getSupplier().clear();
        }
    }

    // implement FarragoSessionDdlValidator
    public void validateUniqueNames(
        CwmModelElement container,
        Collection collection,
        boolean includeType)
    {
        Map nameMap = new LinkedHashMap();
        Iterator iter = collection.iterator();
        while (iter.hasNext()) {
            CwmModelElement element = (CwmModelElement) iter.next();
            String name = element.getName();
            if (name == null) {
                continue;
            }

            // TODO:  implement includeType, since SQL standard says object
            // names within a schema are distinguished by type.  However, it's
            // tricky, since some types (e.g. table and view) should be treated
            // as the same.  So, need a set of special-case base classes.
            CwmModelElement other = (CwmModelElement) nameMap.get(name);
            if (other != null) {
                if (isNewObject(other) && isNewObject(element)) {
                    // clash between two new objects being defined
                    // simultaneously
                    // REVIEW: error message assumes collection is sorted by
                    // definition order, which may not be guaranteed?
                    throw res.newValidatorDuplicateNames(
                        getRepos().getLocalizedObjectName(
                            null,
                            element.getName(),
                            element.refClass()),
                        getRepos().getLocalizedObjectName(
                            container,
                            container.refClass()),
                        getParserPosString(element),
                        getParserPosString(other));
                } else {
                    CwmModelElement newElement;
                    CwmModelElement oldElement;
                    if (isNewObject(other)) {
                        newElement = other;
                        oldElement = element;
                    } else {
                        // TODO:  remove this later when RENAME is supported
                        assert (isNewObject(element));
                        newElement = element;
                        oldElement = other;
                    }

                    // new object clashes with existing object
                    throw res.newValidatorNameInUse(
                        getRepos().getLocalizedObjectName(
                            null,
                            oldElement.getName(),
                            oldElement.refClass()),
                        getRepos().getLocalizedObjectName(
                            container,
                            container.refClass()),
                        getParserPosString(newElement));
                }
            }
            nameMap.put(name, element);
        }
    }

    // implement FarragoSessionDdlValidator
    public CwmDependency createDependency(
        CwmNamespace client,
        Collection suppliers,
        String kind)
    {
        CwmDependency dependency = getRepos().newCwmDependency();
        dependency.setName(client.getName() + "$DEP");
        dependency.setKind(kind);

        Iterator iter = suppliers.iterator();
        while (iter.hasNext()) {
            Object supplier = iter.next();
            dependency.getSupplier().add(supplier);
        }

        // NOTE:  The client owns the dependency, so their lifetimes are coeval.
        // We don't use the DependencyClient association at all because it
        // causes all kinds of weird problems.  That's why we have to
        // restrict clients to being namespaces.  Ugh.
        client.getOwnedElement().add(dependency);

        return dependency;
    }

    // implement FarragoSessionDdlValidator
    public void discardDataWrapper(CwmModelElement wrapper)
    {
        stmtValidator.getSharedDataWrapperCache().discard(wrapper.refMofId());
    }

    // implement FarragoSessionDdlValidator
    public void setViewText(
        CwmView view,
        SqlNode query)
    {
        FarragoSession session = getInvokingSession();
        String unparseSql = query.toSqlString(
            new SqlDialect(session.getDatabaseMetaData()));
        CwmQueryExpression queryExpr = getRepos().newCwmQueryExpression();
        queryExpr.setLanguage("SQL");
        queryExpr.setBody(unparseSql);
        view.setQueryExpression(queryExpr);
    }

    // implement FarragoSessionDdlValidator
    public void setProcedureText(
        CwmProcedure routine,
        SqlNode body)
    {
        FarragoSession session = getInvokingSession();
        String unparseSql = body.toSqlString(
            new SqlDialect(session.getDatabaseMetaData()));
        CwmProcedureExpression procedureExpr =
            getRepos().newCwmProcedureExpression();
        procedureExpr.setLanguage("SQL");
        procedureExpr.setBody(unparseSql);
        routine.setBody(procedureExpr);
    }
    
    /**
     * Add a new DropRule.
     *
     * @param refAssoc the association to embellish
     * @param dropRule the rule to use for this association
     */
    protected void addDropRule(
        RefAssociation refAssoc,
        DropRule dropRule)
    {
        // NOTE:  use class object because in some circumstances MDR makes
        // up multiple instances of the same association, but doesn't implement
        // equals/hashCode correctly.
        dropRules.putMulti(
            refAssoc.getClass(),
            dropRule);
    }

    private void enqueueValidationExcn(DeferredException excn)
    {
        if (enqueuedValidationExcn != null) {
            // for now, only deal with one at a time
            return;
        }
        enqueuedValidationExcn = excn;
    }

    private void checkValidationExcnQueue()
    {
        if (enqueuedValidationExcn != null) {
            // rollback any deletions so that exception construction can
            // rely on pre-deletion state (e.g. for object identification)
            rollbackDeletions();
            throw enqueuedValidationExcn.getException();
        }
    }

    private void fireDropRule(
        DropRule rule,
        RefObject droppedEnd,
        RefObject otherEnd)
    {
        if ((rule.superInterface != null)
                && !(rule.superInterface.isInstance(droppedEnd))) {
            return;
        }
        ReferentialRuleTypeEnum action = rule.action;
        if (action == ReferentialRuleTypeEnum.IMPORTED_KEY_CASCADE) {
            deleteQueue.add(otherEnd);
            return;
        }
        if (!isDropRestrict()) {
            deleteQueue.add(otherEnd);
            return;
        }

        // NOTE: We can't construct the exception now since the object is
        // deleted.  Instead, defer until after rollback.
        final String mofId = droppedEnd.refMofId();
        enqueueValidationExcn(
            new DeferredException() {
                FarragoException getException()
                {
                    CwmModelElement droppedElement =
                        (CwmModelElement) getRepos().getMdrRepos().getByMofId(mofId);
                    return res.newValidatorDropRestrict(
                        getRepos().getLocalizedObjectName(
                            droppedElement,
                            droppedElement.refClass()));
                }
            });
    }

    private void mapParserPosition(Object obj)
    {
        FarragoSessionParserPosition context =
            getParser().getCurrentPosition();
        if (context == null) {
            return;
        }
        parserContextMap.put(obj, context);
    }

    private void rollbackDeletions()
    {
        // A savepoint or chained transaction would be smoother, but MDR
        // doesn't support those.  Instead, have to restart the txn
        // altogether.  Take advantage of the fact that MDR allows us to retain
        // references across txns.
        getRepos().endReposTxn(true);

        // TODO:  really need a lock to protect us against someone else's DDL
        // here, which could invalidate our schedulingMap.
        getRepos().beginReposTxn(true);
    }

    private void scheduleDeletion(RefObject obj)
    {
        assert (!validatedMap.containsKey(obj));

        // delete overrides anything else
        schedulingMap.put(
            obj.refMofId(),
            VALIDATE_DELETION);
        mapParserPosition(obj);
    }

    private void scheduleModification(RefObject obj)
    {
        if (!(obj instanceof CwmModelElement)) {
            return;
        }
        if (validatedMap.containsKey(obj)) {
            return;
        }
        if (schedulingMap.containsKey(obj.refMofId())) {
            return;
        }
        CwmModelElement element = (CwmModelElement) obj;
        if (isNewObject(element)) {
            schedulingMap.put(
                obj.refMofId(),
                VALIDATE_CREATION);
        } else {
            schedulingMap.put(
                obj.refMofId(),
                VALIDATE_MODIFICATION);
        }
        mapParserPosition(obj);
    }

    private void stopListening()
    {
        if (activeThread != null) {
            getRepos().getMdrRepos().removeListener(this);
            activeThread = null;
        }
    }

    private boolean validateAction(
        CwmModelElement modelElement,
        Object action)
    {
        if (action == VALIDATE_MODIFICATION) {
            boolean fired = invokeHandler(
                modelElement,
                "validateModification");
            if (fired) {
                return true;
            } else {
                action = VALIDATE_CREATION;
            }
        }
        
        if (action == VALIDATE_CREATION) {
            return invokeHandler(
                modelElement,
                "validateDefinition");
        } else if (action == VALIDATE_DELETION) {
            return invokeHandler(
                modelElement,
                "validateDrop");
        } else if (action == VALIDATE_TRUNCATION) {
            return invokeHandler(
                modelElement,
                "validateTruncation");
        } else {
            throw new AssertionError();
        }
    }

    private boolean invokeHandler(
        CwmModelElement modelElement,
        String action)
    {
        for (int i = 0; i < actionHandlers.size(); ++i) {
            Object handler = actionHandlers.get(i);
            boolean handled = ReflectUtil.invokeVisitor(
                handler,
                modelElement,
                CwmModelElement.class,
                action);
            if (handled) {
                return true;
            }
        }
        return false;
    }
    
    //~ Inner Classes ---------------------------------------------------------

    /**
     * DropRule specifies what to do when an association link deletion event
     * is heard.
     */
    protected static class DropRule
    {
        /**
         * A filter on the instance of the end to which the rule applies.  If
         * null, the rule applies to any object.  Otherwise, the object must
         * be an instance of this class.
         */
        final Class superInterface;

        /** What to do when this rule fires: */
        final ReferentialRuleTypeEnum action;

        /**
         * The end to which this rule applies.
         */
        final String endName;

        /**
         * Creates a new DropRule object.
         *
         * @param endName .
         * @param superInterface .
         * @param action .
         */
        public DropRule(
            String endName,
            Class superInterface,
            ReferentialRuleTypeEnum action)
        {
            this.endName = endName;
            this.superInterface = superInterface;
            this.action = action;
        }
    }

    /**
     * DeferredException allows an exception's creation to be deferred.
     * This is needed since it is not possible to correctly construct
     * an exception in certain validation contexts.
     */
    private static abstract class DeferredException
    {
        abstract FarragoException getException();
    }
}


// End DdlValidator.java
