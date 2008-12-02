/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2003-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package net.sf.farrago.ddl;

import java.util.*;
import java.util.logging.*;

import javax.jmi.reflect.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.relational.enumerations.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.security.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.namespace.util.*;
import net.sf.farrago.query.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.enki.mdr.*;
import org.eigenbase.jmi.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;

import org.jgrapht.*;
import org.jgrapht.alg.*;
import org.jgrapht.graph.*;

import org.netbeans.api.mdr.events.*;


/**
 * DdlValidator validates the process of applying a DDL statement to the
 * catalog. By implementing MDRPreChangeListener, it is able to automatically
 * collect references to all objects modified by the statement.
 * (MDRChangeListener isn't suitable since it's asynchronous.)
 *
 * <p>Generic validation support is implemented in this class, but
 * object-specific rules should be implemented in handler classes for the
 * appropriate catalog objects.</p>
 *
 * NOTE: all validation activity must take place in the same thread in which
 * DdlValidator's constructor is called.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DdlValidator
    extends FarragoCompoundAllocation
    implements FarragoSessionDdlValidator,
        MDRPreChangeListener
{
    //~ Static fields/initializers ---------------------------------------------

    private static final Logger tracer = FarragoTrace.getDdlValidatorTracer();

    //~ Enums ------------------------------------------------------------------

    private static enum ValidatedOp
    {
        CREATION, MODIFICATION, DELETION, TRUNCATION
    }

    //~ Instance fields --------------------------------------------------------

    private final FarragoSessionStmtValidator stmtValidator;

    /**
     * Queue of excns detected during plannedChange.
     */
    private DeferredException enqueuedValidationExcn;

    /**
     * Map (from RefAssociation.Class to FarragoSessionDdlDropRule) of
     * associations for which special handling is required during DROP.
     */
    private MultiMap<Class<?>, FarragoSessionDdlDropRule> dropRules;

    /**
     * Map from catalog object to SqlParserPos for beginning of definition.
     */
    private final Map<Object, SqlParserPos> parserContextMap;

    /**
     * Map from catalog object to SqlParserPos for offset of body.
     */
    private final Map<RefObject, SqlParserPos> parserOffsetMap;

    /**
     * Map from catalog object to associated SQL definition (not all objects
     * have these).
     */
    private final Map<RefObject, SqlNode> sqlMap;

    /**
     * Map containing scheduled validation actions. The key is the MofId of the
     * object scheduled for validation; the value is a simple object containing
     * a ValidatedOp and the object's RefClass.
     */
    private Map<String, SchedulingDetail> schedulingMap;

    /**
     * Map of objects in transition between schedulingMap and validatedMap.
     * Content format is same as for schedulingMap.
     */
    private Map<String, SchedulingDetail> transitMap;

    /**
     * Map of object validations which have already taken place. The key is the
     * RefObject itself; the value is the action type.
     */
    private Map<RefObject, ValidatedOp> validatedMap;

    /**
     * Set of objects which a DROP CASCADE has encountered but not yet
     * processed.
     */
    private Set<RefObject> deleteQueue;

    /**
     * Thread binding to prevent cross-talk.
     */
    private Thread activeThread;

    /**
     * DDL statement being validated.
     */
    protected FarragoSessionDdlStmt ddlStmt;

    /**
     * List of handlers to be invoked.
     */
    protected List<DdlHandler> actionHandlers;

    /**
     * Object to be replaced if CREATE OR REPLACE
     */
    private CwmModelElement replacementTarget;

    /**
     * Set of objects to be revalidated (CREATE OR REPLACE)
     */
    private Set<CwmModelElement> revalidateQueue;

    private String timestamp;

    protected final ReflectiveVisitDispatcher<DdlHandler, CwmModelElement>
        dispatcher =
        ReflectUtil.createDispatcher(DdlHandler.class, CwmModelElement.class);

    private FemAuthId systemUserAuthId;
    private FemAuthId currentUserAuthId;

    private boolean usePreviewDelete;
    
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new validator. The validator will listen for repository change
     * events and schedule appropriate validation actions on the affected
     * objects. Validation is deferred until validate() is called.
     *
     * @param stmtValidator generic stmt validator
     */
    public DdlValidator(FarragoSessionStmtValidator stmtValidator)
    {
        this.stmtValidator = stmtValidator;
        stmtValidator.addAllocation(this);

        if (getRepos().getEnkiMdrRepos().supportsPreviewRefDelete()) {
            usePreviewDelete = true;
        }
        
        // NOTE jvs 25-Jan-2004:  Use LinkedHashXXX, since order
        // matters for these.
        schedulingMap = 
            new LinkedHashMap<String, SchedulingDetail>();
        validatedMap = new LinkedHashMap<RefObject, ValidatedOp>();
        deleteQueue = new LinkedHashSet<RefObject>();

        parserContextMap = new HashMap<Object, SqlParserPos>();
        parserOffsetMap = new HashMap<RefObject, SqlParserPos>();
        sqlMap = new HashMap<RefObject, SqlNode>();
        revalidateQueue = new HashSet<CwmModelElement>();

        // NOTE:  dropRules are populated implicitly as action handlers
        // are set up below.
        dropRules = new MultiMap<Class<?>, FarragoSessionDdlDropRule>();

        // Build up list of action handlers.
        actionHandlers = new ArrayList<DdlHandler>();

        // First, install action handlers for all installed model
        // extensions.
        for (
            FarragoSessionModelExtension ext
            : stmtValidator.getSession().getModelExtensions())
        {
            ext.defineDdlHandlers(this, actionHandlers);
        }

        // Then, install action handlers specific to this personality.
        stmtValidator.getSession().getPersonality().defineDdlHandlers(
            this,
            actionHandlers);

        startListening();
    }

    //~ Methods ----------------------------------------------------------------

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
        return getInvokingSession().cloneSession(
            stmtValidator.getSessionVariables());
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
    public boolean isDeletedObject(RefObject refObject)
    {
        return testObjectStatus(refObject, ValidatedOp.DELETION);
    }

    // implement FarragoSessionDdlValidator
    public boolean isCreatedObject(RefObject refObject)
    {
        return testObjectStatus(refObject, ValidatedOp.CREATION);
    }

    private boolean testObjectStatus(
        RefObject refObject,
        ValidatedOp status)
    {
        return (getObjectStatusFromMap(schedulingMap, refObject) == status)
            || (validatedMap.get(refObject) == status)
            || ((transitMap != null)
                && (getObjectStatusFromMap(transitMap, refObject) == status));
    }
    
    private ValidatedOp getObjectStatusFromMap(
        Map<String, SchedulingDetail> map, RefObject refObject)
    {
        SchedulingDetail detail = map.get(refObject.refMofId());
        if (detail == null) {
            return null;
        }
        
        return detail.validatedOp;
    }

    // implement FarragoSessionDdlValidator
    public boolean isDropRestrict()
    {
        return ddlStmt.isDropRestricted();
    }

    /**
     * Determines whether a catalog object has had its visibility set yet. NOTE:
     * this should remain private and only be called prior to executeStorage().
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
        SqlParserPos parserContext = getParserPos(obj);
        if (parserContext == null) {
            return null;
        }
        return parserContext.toString();
    }

    private SqlParserPos getParserPos(RefObject obj)
    {
        return parserContextMap.get(obj);
    }

    // implement FarragoSessionDdlValidator
    public String setParserOffset(
        RefObject obj,
        SqlParserPos pos,
        String body)
    {
        int n = 0;
        char c;
        int column = pos.getColumnNum();
        int line = pos.getLineNum();
        while (n < body.length()
                 && Character.isWhitespace((c = body.charAt(n)))) {
            ++n;
            if (c == '\n'
                || (c == '\r'
                && (n == 0 || body.charAt(n - 1) != '\n')))
            {
                ++line;
                column = 1;
            } else {
                ++column;
            }
        }
        pos =
            new SqlParserPos(
                line, column, pos.getEndLineNum(), pos.getEndColumnNum());
        parserOffsetMap.put(obj, pos);
        return body.substring(n);
    }

    // implement FarragoSessionDdlValidator
    public SqlParserPos getParserOffset(RefObject obj)
    {
        return parserOffsetMap.get(obj);
    }

    // implement FarragoSessionDdlValidator
    public void setSqlDefinition(
        RefObject obj,
        SqlNode sqlNode)
    {
        sqlMap.put(obj, sqlNode);
    }

    // implement FarragoSessionDdlValidator
    public SqlNode getSqlDefinition(RefObject obj)
    {
        SqlNode sqlNode = sqlMap.get(obj);
        SqlParserPos parserContext = getParserPos(obj);
        if (parserContext != null) {
            stmtValidator.setParserPosition(parserContext);
        }
        return sqlNode;
    }

    // implement FarragoSessionDdlValidator
    public void setSchemaObjectName(
        CwmModelElement schemaElement,
        SqlIdentifier qualifiedName)
    {
        SqlIdentifier schemaName;
        assert (qualifiedName.names.length > 0);
        assert (qualifiedName.names.length < 4);

        if (qualifiedName.names.length == 3) {
            schemaElement.setName(qualifiedName.names[2]);
            schemaName =
                new SqlIdentifier(
                    new String[] {
                        qualifiedName.names[0], qualifiedName.names[1]
                    },
                    SqlParserPos.ZERO);
        } else if (qualifiedName.names.length == 2) {
            schemaElement.setName(qualifiedName.names[1]);
            schemaName =
                new SqlIdentifier(qualifiedName.names[0], SqlParserPos.ZERO);
        } else {
            schemaElement.setName(qualifiedName.names[0]);
            if (stmtValidator.getSessionVariables().schemaName == null) {
                throw FarragoResource.instance().ValidatorNoDefaultSchema.ex();
            }
            schemaName =
                new SqlIdentifier(
                    stmtValidator.getSessionVariables().schemaName,
                    SqlParserPos.ZERO);
        }
        CwmSchema schema = stmtValidator.findSchema(schemaName);
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

    /**
     * Tests if DDL statement is CREATE OR REPLACE.
     *
     * @return true if statement is CREATE OR REPLACE
     */
    public boolean isReplace()
    {
        if (ddlStmt instanceof DdlCreateStmt) {
            return (((DdlCreateStmt) ddlStmt).getReplaceOptions().isReplace());
        }
        return false;
    }

    /**
     * Returns RENAME TO argument of CREATE OR REPLACE.
     *
     * @return new name of object, or null if this is not a CREATE OR REPLACE or
     * RENAME TO.
     */
    private SqlIdentifier getNewName()
    {
        if (ddlStmt instanceof DdlCreateStmt) {
            return (((DdlCreateStmt) ddlStmt).getReplaceOptions().getNewName());
        }
        return null;
    }

    /**
     * Tests if DDL statement is CREATE OR REPLACE and the target object (to be
     * replaced) is of the specified type.
     *
     * @param object Object type to test for
     *
     * @return true if CREATE OR REPLACE and replacing an object of this type
     */
    public boolean isReplacingType(CwmModelElement object)
    {
        if (ddlStmt instanceof DdlCreateStmt) {
            DdlCreateStmt createStmt = (DdlCreateStmt) ddlStmt;
            if (createStmt.getReplaceOptions().isReplace()) {
                CwmModelElement e = createStmt.getModelElement();
                if (e != null) {
                    String newClassName =
                        object.refClass().refMetaObject().refGetValue(
                            "name").toString();
                    String targetClassName =
                        e.refClass().refMetaObject().refGetValue(
                            "name").toString();
                    return newClassName.equals(targetClassName);
                }
            }
        }
        return false;
    }

    /**
     * Delete the existing object of a CREATE OR REPLACE statement, saving its
     * dependencies for later revalidation.
     */
    public void deleteReplacementTarget(CwmModelElement newElement)
    {
        if (replacementTarget != null) {
            Set<CwmModelElement> deps = getDependencies(replacementTarget);

            // also revalidate dependencies of children (owned by rootElement)
            if (replacementTarget instanceof CwmNamespace) {
                Collection<CwmModelElement> children =
                    ((CwmNamespace) replacementTarget).getOwnedElement();
                if (children != null) {
                    for (CwmModelElement e : children) {
                        deps.addAll(getDependencies(e));
                    }
                }
            } else if (replacementTarget instanceof FemLabel) {
                deps.addAll(((FemLabel) replacementTarget).getAlias());
                try {
                    // remove stats associated with the replaced label
                    FarragoCatalogUtil.removeObsoleteStatistics(
                        (FemLabel) replacementTarget,
                        getRepos(),
                        usePreviewDelete);
                } catch (Exception ex) {
                    throw Util.newInternal(
                        ex,
                        "error removing replaced label's statistics: " +
                        ex.getMessage());
                }
            }
            
            scheduleRevalidation(deps);

            SqlIdentifier newName = getNewName();
            if (newName != null) {
                newElement.setName(newName.getSimple());
            }

            stopListening();

            // special cases - FemBaseColumnSet, FemDataServer, FemLabel
            for (CwmModelElement e : deps) {
                if (e instanceof FemBaseColumnSet) {
                    // must reset server to newElement
                    ((FemBaseColumnSet) e).setServer(
                        (FemDataServer) newElement);
                } else if (e instanceof FemDataServer) {
                    ((FemDataServer) e).setWrapper((FemDataWrapper) newElement);
                } else if (e instanceof FemLabel) {
                    ((FemLabel) e).setParentLabel((FemLabel) newElement);
                }
            }

            // preserve owned elements
            List<CwmModelElement> ownedElements = null;
            if (replacementTarget instanceof CwmNamespace) {
                ownedElements = new ArrayList<CwmModelElement>();
                Collection<CwmModelElement> c =
                    ((CwmNamespace) replacementTarget).getOwnedElement();
                ownedElements.addAll(c);

                // remove ownership to avoid cascade delete
                c.clear();
            }
            replaceDependencies(replacementTarget, newElement);

            startListening();

            replacementTarget.refDelete();
            while (!deleteQueue.isEmpty()) {
                RefObject refObj = deleteQueue.iterator().next();
                deleteQueue.remove(refObj);
                if (!schedulingMap.containsKey(refObj.refMofId())) {
                    if (tracer.isLoggable(Level.FINE)) {
                        tracer.fine("probe deleting " + refObj);
                    }
                    refObj.refDelete();
                }
            }

            // restore owned elements
            if (ownedElements != null) {
                Collection<CwmModelElement> c =
                    ((CwmNamespace) newElement).getOwnedElement();
                c.addAll(ownedElements);
            }
        }
    }

    // implement FarragoSessionDdlValidator
    public void executeStorage()
    {
        // catalog updates which occur during execution should not result in
        // further validation
        stopListening();

        List<RefObject> deletionList = new ArrayList<RefObject>();
        for (
            Map.Entry<RefObject, ValidatedOp> mapEntry
            : validatedMap.entrySet())
        {
            RefObject obj = mapEntry.getKey();
            ValidatedOp action = mapEntry.getValue();

            if (action != ValidatedOp.DELETION) {
                if (obj instanceof CwmStructuralFeature) {
                    // Set some mandatory but irrelevant attributes.
                    CwmStructuralFeature feature = (CwmStructuralFeature) obj;
    
                    feature.setChangeability(ChangeableKindEnum.CK_CHANGEABLE);
                }
            } else {
                RefFeatured container = obj.refImmediateComposite();
                if (container != null) {
                    ValidatedOp containerAction = validatedMap.get(container);
                    if (containerAction == ValidatedOp.DELETION) {
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
            if (action == ValidatedOp.CREATION) {
                invokeHandler(element, "executeCreation");
            } else if (action == ValidatedOp.DELETION) {
                invokeHandler(element, "executeDrop");
            } else if (action == ValidatedOp.TRUNCATION) {
                invokeHandler(element, "executeTruncation");
            } else if (action == ValidatedOp.MODIFICATION) {
                invokeHandler(element, "executeModification");
            } else {
                assert (false);
            }
        }

        // Now mark objects as visible and update their timestamps; we defer
        // this until here so that storage handlers above can use object
        // visibility attribute to distinguish new objects.
        for (
            Map.Entry<RefObject, ValidatedOp> mapEntry
            : validatedMap.entrySet())
        {
            RefObject obj = mapEntry.getKey();
            ValidatedOp action = mapEntry.getValue();

            if (!(obj instanceof CwmModelElement)) {
                continue;
            }
            CwmModelElement element = (CwmModelElement) obj;

            if (action != ValidatedOp.DELETION) {
                updateObjectTimestamp(element);
            }
        }

        // now we can finally consummate any requested deletions, since we're
        // all done referencing the objects
        Collections.reverse(deletionList);
        getRepos().getEnkiMdrRepos().delete(deletionList);

        // verify repository integrity post-delete
        for (
            Map.Entry<RefObject, ValidatedOp> mapEntry
            : validatedMap.entrySet())
        {
            RefObject obj = mapEntry.getKey();
            ValidatedOp action = mapEntry.getValue();
            if (action != ValidatedOp.DELETION) {
                checkJmiConstraints(obj);
            }
        }
    }

    private void checkJmiConstraints(RefObject obj)
    {
        JmiObjUtil.setMandatoryPrimitiveDefaults(obj);
        List<FarragoReposIntegrityErr> errs = getRepos().verifyIntegrity(obj);
        if (!errs.isEmpty()) {
            throw Util.newInternal(
                "Repository integrity check failed on object update:  "
                + errs.toString());
        }
    }

    private void updateObjectTimestamp(CwmModelElement element)
    {
        boolean isNew = isNewObject(element);
        if (isNew) {
            // Retrieve and cache the system and current user FemAuthId 
            // objects rather than looking them up every time.
            if (systemUserAuthId == null) {
                systemUserAuthId = 
                    FarragoCatalogUtil.getAuthIdByName(
                        getRepos(), FarragoCatalogInit.SYSTEM_USER_NAME);
            }
            
            String currentUserName = 
                getInvokingSession().getSessionVariables().currentUserName;
            if (currentUserAuthId == null) {
                currentUserAuthId =
                    FarragoCatalogUtil.getAuthIdByName(
                        getRepos(), currentUserName);
            }
            
            // Define a pseudo-grant representing the element's relationship to
            // its creator.
            FarragoCatalogUtil.newCreationGrant(
                getRepos(),
                systemUserAuthId,
                currentUserAuthId,
                element);
        }

        // We use the visibility attribute to distinguish new objects
        // from existing objects.  From here on, no longer treat
        // this element as new.
        element.setVisibility(VisibilityKindEnum.VK_PUBLIC);

        if (!(element instanceof FemAnnotatedElement)) {
            return;
        }

        // Update attributes maintained for annotated elements.
        FemAnnotatedElement annotatedElement = (FemAnnotatedElement) element;
        FarragoCatalogUtil.updateAnnotatedElement(
            annotatedElement,
            obtainTimestamp(),
            isNew);
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
            scheduleDeletion(obj);
        } else if (event instanceof AttributeEvent) {
            checkStringLength((AttributeEvent)event);
            RefObject obj = (RefObject) (event.getSource());
            scheduleModification(obj);
        } else if (event instanceof AssociationEvent) {
            AssociationEvent associationEvent = (AssociationEvent) event;
            if (tracer.isLoggable(Level.FINE)) {
                tracer.fine("end name = " + associationEvent.getEndName());
            }
            boolean touchEnds = true;
            DependencySupplier depSupplier =
                getRepos().getCorePackage().getDependencySupplier();
            RefAssociation refAssoc =
                (RefAssociation) associationEvent.getSource();
            if (refAssoc.equals(depSupplier)) {
                // REVIEW jvs 3-Jun-2008:  make a special case for
                // the supplier end of dependencies.  For example,
                // when we create a view which depends on a table,
                // there's no need to revalidate the table at
                // that time.
                touchEnds = false;
            }
            if (touchEnds) {
                scheduleModification(associationEvent.getFixedElement());
                if (associationEvent.getOldElement() != null) {
                    scheduleModification(associationEvent.getNewElement());
                }
                if (associationEvent.getNewElement() != null) {
                    scheduleModification(associationEvent.getNewElement());
                }
            }
            if (event.getType() == AssociationEvent.EVENT_ASSOCIATION_REMOVE) {
                // MDR's event loses some information, namely which end the
                // association is being removed from.  event.getEndName() is
                // chosen arbitrarily.  To work around this, we use our
                // knowledge of which side has already been scheduled for
                // deleted to infer what we need.  (TODO jvs 9-Aug-2008: once
                // we've completely migrated off of MDR, enhance enki to
                // deliver the full information as part of the event.)

                // By default, or if we can't do any better, rely on MDR.
                String endName = associationEvent.getEndName();
                RefObject droppedEnd = associationEvent.getFixedElement();
                RefObject otherEnd = associationEvent.getOldElement();

                if (!isDeletedObject(droppedEnd) && isDeletedObject(otherEnd)) {
                    // Swap ends.
                    RefObject tmp = droppedEnd;
                    droppedEnd = otherEnd;
                    otherEnd = tmp;
                    JmiAssocEdge assocEdge =
                        getRepos().getModelGraph().getEdgeForRefAssoc(refAssoc);
                    if (endName.equals(assocEdge.getSourceEnd().getName())) {
                        endName = assocEdge.getTargetEnd().getName();
                    } else {
                        assert(endName.equals(
                                   assocEdge.getTargetEnd().getName()));
                        endName = assocEdge.getSourceEnd().getName();
                    }
                }
                
                List<FarragoSessionDdlDropRule> rules =
                    dropRules.getMulti(refAssoc.getClass());
                for (FarragoSessionDdlDropRule rule : rules) {
                    if ((rule != null)
                        && rule.getEndName().equals(endName))
                    {
                        fireDropRule(
                            rule,
                            droppedEnd,
                            otherEnd);
                    }
                }
            }
        } else {
            // REVIEW:  when I turn this on, I see notifications for
            // CreateInstanceEvent, which is supposed to be masked out.
            // Probably an MDR bug.

            /*
               tracer.warning(    "Unexpected event "+event.getClass().getName()
               + ", type="+event.getType()); assert(false);
             */
        }
    }

    // implement FarragoSessionDdlValidator
    public void validate(FarragoSessionDdlStmt ddlStmt)
    {
        this.ddlStmt = ddlStmt;
        
        if (isReplace()) {
            usePreviewDelete = false;
        }
        
        ddlStmt.preValidate(this);
        checkValidationExcnQueue();

        if (ddlStmt instanceof DdlDropStmt) {
            checkInUse(ddlStmt.getModelElement().refMofId());

            // Process deletions until a fixpoint is reached, using MDR events
            // to implement RESTRICT/CASCADE.
            while (!deleteQueue.isEmpty()) {
                RefObject refObj = deleteQueue.iterator().next();
                checkInUse(refObj.refMofId());

                deleteQueue.remove(refObj);
                if (!schedulingMap.containsKey(refObj.refMofId())) {
                    if (tracer.isLoggable(Level.FINE)) {
                        tracer.fine("probe deleting " + refObj);
                    }
                    
                    deleteObject(refObj);
                }
            }

            if (!usePreviewDelete) {
                // In order to validate deletion, we need the objects to exist,
                // but they've already been deleted.  So rollback the deletion
                // now, but we still remember the objects encountered above.
                rollbackDeletions();
            }
            
            // REVIEW:  This may need to get more complicated in the future.
            // Like, if deletions triggered modifications to some referencing
            // object which needs to have its update validation run AFTER the
            // deletion takes place, not before.
        }

        if (isReplace()) {
            replacementTarget = findDuplicate(ddlStmt.getModelElement());

            if (replacementTarget != null) {
                if (stmtValidator.getDdlLockManager().isObjectInUse(
                        replacementTarget.refMofId()))
                {
                    throw FarragoResource.instance()
                    .ValidatorReplacedObjectInUse.ex(
                        getRepos().getLocalizedObjectName(
                            null,
                            replacementTarget.getName(),
                            replacementTarget.refClass()));
                }

                // if this is CREATE OR REPLACE and we've encountered an object
                // to be replaced, save its dependencies for revalidation and
                // delete it.
                deleteReplacementTarget(ddlStmt.getModelElement());
            }
        }

        while (!schedulingMap.isEmpty()) {
            checkValidationExcnQueue();

            // Swap in a new map so new scheduling calls aren't handled until
            // the next round.
            transitMap = schedulingMap;
            schedulingMap = 
                new LinkedHashMap<String, SchedulingDetail>();

            boolean progress = false;
            for (
                Map.Entry<String, SchedulingDetail> mapEntry
                : transitMap.entrySet())
            {
                SchedulingDetail scheduleDetail = mapEntry.getValue();
                
                RefClass cls = scheduleDetail.refClass;
                RefObject obj = 
                    getRepos().getEnkiMdrRepos().getByMofId(
                        mapEntry.getKey(), cls);
                if (obj == null) {
                    progress = progress ||
                        ValidatedOp.DELETION == scheduleDetail.validatedOp;
                    // technically it is progress, and
                    // the object might already have been deleted
                    continue;
                }
                ValidatedOp action = scheduleDetail.validatedOp;

                // mark this object as already validated so it doesn't slip
                // back in by updating itself
                validatedMap.put(obj, action);

                if (!(obj instanceof CwmModelElement)) {
                    progress = true;
                    continue;
                }
                final CwmModelElement element = (CwmModelElement) obj;
                try {
                    validateAction(element, action);
                    if (element.getVisibility() == null) {
                        // mark this new element as validated
                        element.setVisibility(VisibilityKindEnum.VK_PRIVATE);
                    }
                    if ((revalidateQueue != null)
                        && revalidateQueue.contains(element)) {
                        setRevalidationResult(element, null);
                    }
                    progress = true;
                } catch (FarragoUnvalidatedDependencyException ex) {

                    // Something hit an unvalidated dependency; we'll have
                    // to retry this object later.
                    validatedMap.remove(obj);
                    schedulingMap.put(obj.refMofId(), scheduleDetail);
                } catch (EigenbaseException ex) {
                    tracer.info(
                        "Revalidate exception on "
                        + ((CwmModelElement) obj).getName() + ": "
                        + FarragoUtil.exceptionToString(ex));
                    setSourceInContext(ex, getParser().getSourceString());
                    if ((revalidateQueue != null)
                        && revalidateQueue.contains(element))
                    {
                        setRevalidationResult(element, ex);
                    } else {
                        throw ex;
                    }
                }
            }
            transitMap = null;
            if (!progress) {
                // Every single object hit a
                // FarragoUnvalidatedDependencyException.  This implies a
                // cycle.  TODO:  identify the cycle in the exception.
                throw FarragoResource.instance().ValidatorSchemaDependencyCycle
                .ex();
            }
        }

        if (isReplace()) {
            // check for loops in our newly replaced object
            if (containsCycle(
                    ddlStmt.getModelElement()))
            {
                throw FarragoResource.instance().ValidatorSchemaDependencyCycle
                .ex();
            }
        }

        // one last time
        checkValidationExcnQueue();

        // finally, process any deferred privilege checks
        stmtValidator.getPrivilegeChecker().checkAccess();
    }

    private void setSourceInContext(EigenbaseException e, String sourceString)
    {
        if (sourceString != null) {
            Throwable ex = (Throwable)e;
            while (ex != null) {
                if (ex instanceof EigenbaseContextException) {
                    ((EigenbaseContextException)ex).setOriginalStatement(sourceString);
                    break;
                }
                ex = ex.getCause();
            }
        }
    }

    /**
     * Handle an exception encountered during validation of dependencies of an
     * object replaced via CREATE OR REPLACE (a.k.a. revalidation).
     *
     * @param element Catalog object causing revalidation exception
     * @param ex Revalidation exception
     */
    public void setRevalidationResult(
        CwmModelElement element,
        EigenbaseException ex)
    {
        if (ex != null) {
            throw ex;
        }
    }

    // implement FarragoSessionDdlValidator
    public void validateUniqueNames(
        CwmModelElement container,
        Collection<? extends CwmModelElement> collection,
        boolean includeType)
    {
        // Sort the collection by parser position, since we depend on the
        // sort order later and it is NOT guaranteed.
        ArrayList<CwmModelElement> sortedCollection =
            new ArrayList<CwmModelElement>(collection);
        Collections.sort(
            sortedCollection,
            new RefObjectPositionComparator());

        Map<Object, CwmModelElement> nameMap =
            new LinkedHashMap<Object, CwmModelElement>();
        for (CwmModelElement element : sortedCollection) {
            String nameKey = getNameKey(element, includeType);
            if (nameKey == null) {
                continue;
            }

            CwmModelElement other = nameMap.get(nameKey);
            if (other != null) {
                if (isNewObject(other) && isNewObject(element)) {
                    // clash between two new objects being defined
                    // simultaneously
                    // Error message assumes collection is sorted by
                    // definition order, which we guaranteed earlier.
                    throw newPositionalError(
                        element,
                        FarragoResource.instance().ValidatorDuplicateNames.ex(
                            getRepos().getLocalizedObjectName(
                                null,
                                element.getName(),
                                element.refClass()),
                            getRepos().getLocalizedObjectName(container),
                            getParserPosString(other)));
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
                    throw newPositionalError(
                        newElement,
                        FarragoResource.instance().ValidatorNameInUse.ex(
                            getRepos().getLocalizedObjectName(
                                null,
                                oldElement.getName(),
                                oldElement.refClass()),
                            getRepos().getLocalizedObjectName(container)));
                }
            }
            nameMap.put(nameKey, element);
        }
    }

    private String getNameKey(
        CwmModelElement element,
        boolean includeType)
    {
        String name = element.getName();
        if (name == null) {
            return null;
        }
        if (!includeType) {
            return name;
        }
        Class<?> typeClass;

        // TODO jvs 25-Feb-2005:  provide a mechanism for generalizing these
        // special cases
        if (element instanceof CwmNamedColumnSet) {
            typeClass = CwmNamedColumnSet.class;
        } else if (element instanceof CwmCatalog) {
            typeClass = CwmCatalog.class;
        } else {
            try {
                typeClass =
                    JmiObjUtil.getJavaInterfaceForRefObject(
                        element.refClass());
            }
            catch(ClassNotFoundException e) {
                throw Util.newInternal(e);
            }
        }
        return typeClass.toString() + ":" + name;
    }

    // implement FarragoSessionDdlValidator
    public <T extends CwmModelElement> CwmDependency createDependency(
        CwmNamespace client,
        Collection<T> suppliers)
    {
        String depName = client.getName() + "$DEP";
        CwmDependency dependency =
            FarragoCatalogUtil.getModelElementByNameAndType(
                client.getOwnedElement(),
                depName,
                CwmDependency.class);

        if (dependency == null) {
            dependency = getRepos().newCwmDependency();
            dependency.setName(depName);
            dependency.setKind("GenericDependency");

            // NOTE: The client owns the dependency, so their lifetimes are
            // coeval.  That's why we restrict clients to being namespaces.
            client.getOwnedElement().add(dependency);
            dependency.getClient().add(client);
        }

        for (T supplier : suppliers) {
            dependency.getSupplier().add(supplier);
        }

        return dependency;
    }

    // implement FarragoSessionDdlValidator
    public void discardDataWrapper(CwmModelElement wrapper)
    {
        stmtValidator.getSharedDataWrapperCache().discard(wrapper.refMofId());
    }

    // implement FarragoSessionDdlValidator
    public void setCreatedSchemaContext(FemLocalSchema schema)
    {
        FarragoSessionVariables sessionVariables =
            getStmtValidator().getSessionVariables();

        sessionVariables.catalogName = schema.getNamespace().getName();
        sessionVariables.schemaName = schema.getName();

        // convert from List<FemSqlpathElement> to List<SqlIdentifier>
        List<SqlIdentifier> list = new ArrayList<SqlIdentifier>();
        for (Object o : schema.getPathElement()) {
            FemSqlpathElement element = (FemSqlpathElement) o;
            SqlIdentifier id =
                new SqlIdentifier(
                    new String[] {
                        element.getSearchedSchemaCatalogName(),
                        element.getSearchedSchemaName()
                    },
                    SqlParserPos.ZERO);
            list.add(id);
        }
        sessionVariables.schemaSearchPath = Collections.unmodifiableList(list);
    }

    // implement FarragoSessionDdlValidator
    public EigenbaseException newPositionalError(
        RefObject refObj,
        SqlValidatorException ex)
    {
        SqlParserPos parserContext = getParserPos(refObj);
        if (parserContext == null) {
            return new EigenbaseException(
                ex.getMessage(),
                ex.getCause());
        }
        String msg = parserContext.toString();
        EigenbaseContextException contextExcn =
            FarragoResource.instance().ValidatorPositionContext.ex(msg, ex);
        contextExcn.setPosition(
            parserContext.getLineNum(),
            parserContext.getColumnNum(),
            parserContext.getEndLineNum(),
            parserContext.getEndColumnNum());
        return contextExcn;
    }

    // implement FarragoSessionDdlValidator
    public void deleteObject(RefObject obj)
    {
        if (usePreviewDelete) {
            getRepos().getEnkiMdrRepos().previewRefDelete(obj);
        } else {
            obj.refDelete();
        }
    }
    
    // implement FarragoSessionDdlValidator
    public void defineDropRule(
        RefAssociation refAssoc,
        FarragoSessionDdlDropRule dropRule)
    {
        // NOTE:  use class object because in some circumstances MDR makes up
        // multiple instances of the same association, but doesn't implement
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
        FarragoSessionDdlDropRule rule,
        RefObject droppedEnd,
        RefObject otherEnd)
    {
        if ((rule.getSuperInterface() != null)
            && !(rule.getSuperInterface().isInstance(droppedEnd)))
        {
            return;
        }
        ReferentialRuleTypeEnum action = rule.getAction();
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
                EigenbaseException getException()
                {
                    CwmModelElement droppedElement =
                        (CwmModelElement) getRepos().getMdrRepos().getByMofId(
                            mofId);
                    return FarragoResource.instance().ValidatorDropRestrict.ex(
                        getRepos().getLocalizedObjectName(
                            droppedElement));
                }
            });
    }

    private void mapParserPosition(RefObject obj)
    {
        SqlParserPos context = getParser().getCurrentPosition();
        if (context == null) {
            return;
        }
        parserContextMap.put(obj, context);
    }

    private void rollbackDeletions()
    {
        // A savepoint or chained transaction would be smoother, but MDR doesn't
        // support those.  Instead, have to restart the txn altogether.  Take
        // advantage of the fact that MDR allows us to retain references across
        // txns.
        getRepos().endReposTxn(true);

        // Restart the session, which is safe because we pass objects across
        // the session boundary via MOF ID.
        getRepos().endReposSession();

        getRepos().beginReposSession();

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
            new SchedulingDetail(
                ValidatedOp.DELETION, obj.refClass()));
        mapParserPosition(obj);
    }

    private void scheduleModification(RefObject obj)
    {
        if (obj == null) {
            return;
        }
        if (validatedMap.containsKey(obj)) {
            return;
        }
        if (schedulingMap.containsKey(obj.refMofId())) {
            return;
        }
        if (!(obj instanceof CwmModelElement)) {
            // NOTE jvs 12-June-2005:  record it so that we can run
            // integrity verification on it during executeStorage()
            schedulingMap.put(
                obj.refMofId(),
                new SchedulingDetail(
                    ValidatedOp.MODIFICATION, obj.refClass()));
            return;
        }
        CwmModelElement element = (CwmModelElement) obj;
        if (isNewObject(element)) {
            schedulingMap.put(
                obj.refMofId(),
                new SchedulingDetail(ValidatedOp.CREATION, obj.refClass()));
        } else {
            schedulingMap.put(
                obj.refMofId(),
                new SchedulingDetail(
                    ValidatedOp.MODIFICATION, obj.refClass()));
        }
        mapParserPosition(obj);
    }

    private void checkStringLength(AttributeEvent event)
    {
        RefObject obj = (RefObject)event.getSource();
        final String attrName = event.getAttributeName();

        RefClass refClass = obj.refClass();

        javax.jmi.model.Attribute attr =
            JmiObjUtil.getNamedFeature(
                refClass, javax.jmi.model.Attribute.class, attrName, true);
        if (attr == null) {
            throw Util.newInternal(
                "did not find attribute '" + attrName + "'");
        }

        final int maxLength = JmiObjUtil.getMaxLength(refClass, attr);
        if (maxLength == Integer.MAX_VALUE) {
            // Marked unlimited or not string type.
            return;
        }

        Object val = event.getNewElement();
        if (val == null) {
            return;
        }

        if (val instanceof Collection) {
            boolean allPass = true;
            for(Object v: (Collection<?>)val) {
                if (v == null) {
                    continue;
                }

                int length = v.toString().length();

                if (length > maxLength) {
                    allPass = false;
                    break;
                }
            }

            if (allPass) {
                return;
            }
        } else {
            int length = ((String)val).length();

            if (length <= maxLength) {
                return;
            }
        }

        final String name = getRepos().getLocalizedClassName(refClass);

        enqueueValidationExcn(
            new DeferredException() {
                EigenbaseException getException()
                {
                    return FarragoResource.instance().ValidatorInvalidObjectAttributeDefinition.ex(
                        name,
                        attrName,
                        maxLength);
                }
            });
    }

    private void startListening()
    {
        // MDR pre-change instance creation events are useless, since they don't
        // refer to the new instance.  Instead, we rely on the fact that it's
        // pretty much guaranteed that the new object will have attributes or
        // associations set (though someone will probably come up with a
        // pathological case eventually).
        activeThread = Thread.currentThread();
        getRepos().getMdrRepos().addListener(
            this,
            InstanceEvent.EVENT_INSTANCE_DELETE
            | AttributeEvent.EVENTMASK_ATTRIBUTE
            | AssociationEvent.EVENTMASK_ASSOCIATION);
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
        ValidatedOp action)
    {
        stmtValidator.setParserPosition(null);
        if (action == ValidatedOp.CREATION) {
            return invokeHandler(modelElement, "validateDefinition");
        } else if (action == ValidatedOp.MODIFICATION) {
            return invokeHandler(modelElement, "validateModification");
        } else if (action == ValidatedOp.DELETION) {
            return invokeHandler(modelElement, "validateDrop");
        } else if (action == ValidatedOp.TRUNCATION) {
            return invokeHandler(modelElement, "validateTruncation");
        } else {
            throw new AssertionError();
        }
    }

    // called by DdlStmt.postCommit(): dispatches by reflection
    void handlePostCommit(CwmModelElement modelElement, String command)
    {
        invokeHandler(modelElement, "postCommit" + command);
    }

    protected boolean invokeHandler(
        CwmModelElement modelElement,
        String action)
    {
        for (DdlHandler handler : actionHandlers) {

            boolean handled =
                dispatcher.invokeVisitor(
                    handler,
                    modelElement,
                    action);
            if (handled) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check transitive closure of dependencies of an element for cycles.
     *
     * @param rootElement Starting element for dependency search
     *
     * @return true if cycle is found
     */
    private boolean containsCycle(
        CwmModelElement rootElement)
    {
        Set<CwmModelElement> visited = new HashSet<CwmModelElement>();
        Set<CwmModelElement> queue = new HashSet<CwmModelElement>();
        DirectedGraph<CwmModelElement, DefaultEdge> graph =
            new DefaultDirectedGraph<CwmModelElement, DefaultEdge>(
                DefaultEdge.class);

        DependencySupplier depSupplier =
            getRepos().getCorePackage().getDependencySupplier();

        // First, build the graph
        queue.add(rootElement);
        while (!queue.isEmpty()) {
            CwmModelElement element = queue.iterator().next();
            queue.remove(element);
            if (visited.contains(element)) {
                continue;
            }
            graph.addVertex(element);
            visited.add(element);
            Collection<?> deps = depSupplier.getSupplierDependency(element);

            if (deps != null) {
                for (Object o : deps) {
                    if (o instanceof CwmDependency) {
                        CwmDependency dep = (CwmDependency) o;
                        Collection<CwmModelElement> c = dep.getClient();
                        for (CwmModelElement e : c) {
                            queue.add(e);
                            graph.addVertex(e);
                            graph.addEdge(element, e);
                        }
                    }
                }
            }
        }

        // Then, check for cycles (TODO jvs 19-Oct-2006: now that we're using
        // JGraphT, we can do better error reporting on names of objects
        // participating in the cycle.)
        return new CycleDetector<CwmModelElement, DefaultEdge>(
            graph).detectCycles();
    }

    private void scheduleRevalidation(Set<CwmModelElement> elements)
    {
        for (CwmModelElement e : elements) {
            if (!revalidateQueue.contains(e)) {
                revalidateQueue.add(e);

                // REVIEW jvs 3-Nov-2006:  Probably we should
                // discriminate revalidation from both
                // ValidatedOp.CREATION and ValidatedOp.MODIFICATION.

                //REVIEW: unless we regenerate this dependency's SQL
                //and reparse, how would we get the SqlParserPos.
                //set to a dummy value for now.
                this.setParserOffset(
                    e,
                    new SqlParserPos(1, 0),
                    "");
                schedulingMap.put(
                    e.refMofId(),
                    new SchedulingDetail(ValidatedOp.CREATION, e.refClass()));
            }
        }
    }

    // implement FarragoSessionDdlValidator
    public Set<CwmModelElement> getDependencies(CwmModelElement rootElement)
    {
        Set<CwmModelElement> result = new HashSet<CwmModelElement>();

        DependencySupplier s =
            getRepos().getCorePackage().getDependencySupplier();
        for (Object o : s.getSupplierDependency(rootElement)) {
            if (o instanceof CwmDependency) {
                CwmDependency dep = (CwmDependency) o;
                Collection<CwmModelElement> c = dep.getClient();
                for (CwmModelElement e : c) {
                    result.add(e);
                }
            }
        }
        return result;
    }

    public void fixupView(
        FemLocalView view,
        FarragoSessionAnalyzedSql analyzedSql)
    {
        // Add CAST( VAR/CHAR/BINARY(0) to VAR/CHAR/BINARY(1) )
        List<FemViewColumn> columnList =
            Util.cast(view.getFeature(), FemViewColumn.class);
        boolean updateSql = false;
        if (columnList.size() > 0) {
            StringBuilder buf = new StringBuilder("SELECT");
            int k = 0;
            for (RelDataTypeField field : analyzedSql.resultType.getFields()) {
                String targetType = null;
                SqlTypeName sqlType = field.getType().getSqlTypeName();
                SqlTypeFamily typeFamily =
                    SqlTypeFamily.getFamilyForSqlType(sqlType);
                if ((typeFamily == SqlTypeFamily.CHARACTER)
                    || (typeFamily == SqlTypeFamily.BINARY))
                {
                    if (field.getType().getPrecision() == 0) {
                        // Can't have precision of 0
                        // Add cast so there is precision of 1
                        targetType = sqlType.name() + "(1)";
                        updateSql = true;
                    }
                }
                FemViewColumn viewColumn = columnList.get(k);
                if (k > 0) {
                    buf.append(", ");
                }
                if (targetType == null) {
                    SqlUtil.eigenbaseDialect.quoteIdentifier(
                        buf,
                        field.getName());
                } else {
                    buf.append(" CAST(");
                    SqlUtil.eigenbaseDialect.quoteIdentifier(
                        buf,
                        field.getName());
                    buf.append(" AS ");
                    buf.append(targetType);
                    buf.append(")");
                }
                buf.append(" AS ");
                SqlUtil.eigenbaseDialect.quoteIdentifier(
                    buf,
                    viewColumn.getName());
                k++;
            }
            buf.append(" FROM (").append(analyzedSql.canonicalString).append(
                ")");

            if (updateSql) {
                analyzedSql.canonicalString = buf.toString();
            }
        }
    }

    /**
     * Removes dependency associations on oldElement so that it may be deleted
     * without cascading side effects. Reassign these dependencies to
     * newElement. Assumes MDR change listener isn't active.
     *
     * @param oldElement Element to remove dependencies from
     * @param newElement Element to add dependencies to
     */
    private void replaceDependencies(
        CwmModelElement oldElement,
        CwmModelElement newElement)
    {
        assert (activeThread == null);

        DependencySupplier depSupplier =
            getRepos().getCorePackage().getDependencySupplier();

        Collection<CwmDependency> supplierDependencies =
            new ArrayList<CwmDependency>(
                depSupplier.getSupplierDependency(oldElement));
        for(CwmDependency dep: supplierDependencies) {
            depSupplier.remove(oldElement, dep);
            depSupplier.add(newElement, dep);
        }

        DependencyClient depClient =
            getRepos().getCorePackage().getDependencyClient();
        Collection<CwmDependency> clientDependencies =
            new ArrayList<CwmDependency>(
                depClient.getClientDependency(oldElement));
        for(CwmDependency dep: clientDependencies) {
            depClient.remove(oldElement, dep);
            depClient.add(newElement, dep);
        }
    }

    /**
     * Searches an element's namespace's owned elements and returns the first
     * duplicate found (by name, type).
     *
     * @param target Target of search
     *
     * @return CwmModelElement found, or null if not found
     */
    private CwmModelElement findDuplicate(CwmModelElement target)
    {
        String name = target.getName();
        RefClass type = target.refClass();

        CwmNamespace ns = target.getNamespace();
        if (ns != null) {
            Collection<?> c = ns.getOwnedElement();
            if (c != null) {
                for (Object o : c) {
                    CwmModelElement element = (CwmModelElement) o;
                    if (element.equals(target)) {
                        continue;
                    }
                    if (!element.getName().equals(name)) {
                        continue;
                    }
                    if (element.refIsInstanceOf(
                            type.refMetaObject(),
                            true))
                    {
                        return element;
                    }
                }
            }
        }
        return null;
    }

    // implement FarragoSessionDdlValidator
    public String obtainTimestamp()
    {
        // NOTE jvs 5-Nov-2006:  Use a single consistent timestamp for
        // all objects involved in the same DDL transaction (FRG-126).
        if (timestamp == null) {
            timestamp = FarragoCatalogUtil.createTimestamp();
        }
        return timestamp;
    }

    public void validateViewColumnList(Collection<?> collection)
    {
        // intentionally empty
    }

    /**
     * Checks if an object is in use by a running statement. If so, throw a
     * deferred exception. Used only in the DROP case. CREATE OR REPLACE throws
     * a different exception.
     *
     * @param mofId Object MOFID being dropped
     */
    private void checkInUse(final String mofId)
    {
        if (stmtValidator.getDdlLockManager().isObjectInUse(mofId)) {
            enqueueValidationExcn(
                new DeferredException() {
                    EigenbaseException getException()
                    {
                        CwmModelElement droppedElement =
                            (CwmModelElement) getRepos().getMdrRepos()
                                                        .getByMofId(mofId);
                        throw FarragoResource.instance()
                        .ValidatorDropObjectInUse.ex(
                            getRepos().getLocalizedObjectName(
                                droppedElement));
                    }
                });
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * DeferredException allows an exception's creation to be deferred. This is
     * needed since it is not possible to correctly construct an exception in
     * certain validation contexts.
     */
    private static abstract class DeferredException
    {
        abstract EigenbaseException getException();
    }

    /**
     * RefObjectPositionComparator compares RefObjects based on their position
     * within the owning DdlValidator's
     * {@link net.sf.farrago.ddl.DdlValidator#parserContextMap}.  Handles
     * the case where position information is not available (all comparisons
     * return equality) and even the unlikely case where only partial position
     * information is available (RefObjects with positions compare before those
     * without).
     */
    private class RefObjectPositionComparator
        implements Comparator<RefObject>
    {
        public int compare(RefObject o1, RefObject o2)
        {
            SqlParserPos pos1 = getParserPos(o1);
            SqlParserPos pos2 = getParserPos(o2);

            if (pos1 == null) {
                if (pos2 == null) {
                    return 0;
                }

                return 1;
            } else if (pos2 == null) {
                return -1;
            } else {
                int c = pos1.getLineNum() - pos2.getLineNum();
                if (c != 0) {
                    return c;
                }

                c = pos1.getColumnNum() - pos2.getColumnNum();
                if (c != 0) {
                    return c;
                }

                c = pos1.getEndLineNum() - pos2.getEndLineNum();
                if (c != 0) {
                    return c;
                }

                return pos1.getEndColumnNum() - pos2.getEndColumnNum();
            }
        }
    }
    
    private static class SchedulingDetail
    {
        private final ValidatedOp validatedOp;
        private final RefClass refClass;
        
        private SchedulingDetail(ValidatedOp validatedOp, RefClass refClass)
        {
            this.validatedOp = validatedOp;
            this.refClass = refClass;
        }
    }
}

// End DdlValidator.java
