/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package org.eigenbase.jmi;

import java.util.*;
import java.util.logging.*;

import javax.jmi.model.*;
import javax.jmi.reflect.*;

import org.eigenbase.trace.*;
import org.eigenbase.util.*;

import org.netbeans.api.mdr.*;
import org.netbeans.api.mdr.events.*;


/**
 * JmiChangeSet manages the process of applying changes to a JMI repository
 * (currently relying on MDR specifics).
 *
 * <p>TODO jvs 16-Nov-2005: I factored a lot of this out of DdlValidator, but I
 * didn't update DdlValidator because Jason is working on CREATE OR REPLACE in
 * there. Once things settle down, make DdlValidator rely on JmiChangeSet
 * instead of duplicating it.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class JmiChangeSet
    implements MDRPreChangeListener
{

    //~ Static fields/initializers ---------------------------------------------

    private static final Logger tracer = EigenbaseTrace.getJmiChangeSetTracer();

    //~ Instance fields --------------------------------------------------------

    /**
     * Queue of excns detected during plannedChange.
     */
    private DeferredException enqueuedValidationExcn;

    /**
     * Map (from RefAssociation.Class to JmiDeletionRule) of associations for
     * which special handling is required during deletion.
     */
    private MultiMap<Class<? extends Object>, JmiDeletionRule> deletionRules;

    /**
     * Map containing scheduled validation actions. The key is the MofId of the
     * object scheduled for validation; the value is the action type.
     */
    private Map<String, JmiValidationAction> schedulingMap;

    /**
     * Map of objects in transition between schedulingMap and validatedMap.
     * Content format is same as for schedulingMap.
     */
    private Map<String, JmiValidationAction> transitMap;

    /**
     * Map of object validations which have already taken place. The key is the
     * RefObject itself; the value is the action type.
     */
    private Map<RefObject, JmiValidationAction> validatedMap;

    /**
     * Set of objects which a recursive deletion has encountered but not yet
     * processed.
     */
    private final Set<RefObject> deleteQueue;

    /**
     * Thread binding to prevent cross-talk.
     */
    private Thread activeThread;

    private final JmiChangeDispatcher dispatcher;

    private boolean singleLevelCascade;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new JmiChangeSet which will listen for repository change events
     * and schedule appropriate validation actions on the affected objects.
     * Validation is deferred until validate() is called.
     *
     * @param dispatcher implementation of {@link JmiChangeDispatcher} to use
     */
    public JmiChangeSet(JmiChangeDispatcher dispatcher)
    {
        this.dispatcher = dispatcher;

        // NOTE jvs 25-Jan-2004:  Use LinkedHashXXX, since order
        // matters for these.
        schedulingMap = new LinkedHashMap<String, JmiValidationAction>();
        validatedMap = new LinkedHashMap<RefObject, JmiValidationAction>();
        deleteQueue = new LinkedHashSet<RefObject>();

        // NOTE:  deletionRules are populated implicitly as action handlers
        // are set up below.
        deletionRules =
            new MultiMap<Class<? extends Object>, JmiDeletionRule>();

        // MDR pre-change instance creation events are useless, since they don't
        // refer to the new instance.  Instead, we rely on the fact that it's
        // pretty much guaranteed that the new object will have attributes or
        // associations set (though someone will probably come up with a
        // pathological case eventually).
        activeThread = Thread.currentThread();
        getMdrRepos().addListener(this,
            InstanceEvent.EVENT_INSTANCE_DELETE
            | AttributeEvent.EVENTMASK_ATTRIBUTE
            | AssociationEvent.EVENTMASK_ASSOCIATION);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Releases any resources associated with this change.
     *
     * <p>TODO jvs 16-Nov-2005: move FarragoAllocation framework up to
     * org.eigenbase level.
     */
    public void closeAllocation()
    {
        stopListening();
    }

    /**
     * Controls cascaded delete behavior.  By default, cascades recursively.
     *
     * @param singleLevelCascade if true, only cascade to one level; if
     * false, cascade recursively.
     */
    public void setSingleLevelCascade(boolean singleLevelCascade)
    {
        this.singleLevelCascade = singleLevelCascade;
    }

    /**
     * Determines whether an object is being deleted by this change.
     *
     * @param refObject object in question
     *
     * @return true if refObject is being deleted
     */
    public boolean isDeletedObject(RefObject refObject)
    {
        return testObjectStatus(refObject, JmiValidationAction.DELETION);
    }

    /**
     * Determines whether an object is being created by this change.
     *
     * @param refObject object in question
     *
     * @return true if refObject is being created
     */
    public boolean isCreatedObject(RefObject refObject)
    {
        return testObjectStatus(refObject, JmiValidationAction.CREATION);
    }

    private boolean testObjectStatus(
        RefObject refObject,
        JmiValidationAction status)
    {
        return
            (schedulingMap.get(refObject.refMofId()) == status)
            || (validatedMap.get(refObject) == status)
            || (
                (transitMap != null)
                && (transitMap.get(refObject.refMofId()) == status)
               );
    }

    private void stopListening()
    {
        if (activeThread != null) {
            getMdrRepos().removeListener(this);
            activeThread = null;
        }
    }

    public MDRepository getMdrRepos()
    {
        return dispatcher.getMdrRepos();
    }

    public void execute()
    {
        // catalog updates which occur during execution should not result in
        // further validation
        stopListening();

        // build deletion list
        List<RefObject> deletionList = new ArrayList<RefObject>();
        for (Map.Entry<RefObject, JmiValidationAction> mapEntry
            : validatedMap.entrySet()) {
            RefObject obj = (RefObject) mapEntry.getKey();
            Object action = mapEntry.getValue();

            if (action != JmiValidationAction.DELETION) {
                continue;
            }
            
            dispatcher.clearDependencySuppliers(obj);
            RefFeatured container = obj.refImmediateComposite();
            if (container != null) {
                JmiValidationAction containerAction =
                    validatedMap.get(container);
                if (containerAction == JmiValidationAction.DELETION) {
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

        // now we can finally consummate any requested deletions, since we're
        // all done referencing the objects
        Collections.reverse(deletionList);
        for (RefObject refObj : deletionList) {
            if (tracer.isLoggable(Level.FINE)) {
                tracer.fine("really deleting " + refObj);
            }
            refObj.refDelete();
        }

        // verify repository integrity post-delete
        for (Map.Entry<RefObject, JmiValidationAction> mapEntry
            : validatedMap.entrySet())
        {
            RefObject obj = (RefObject) mapEntry.getKey();
            Object action = mapEntry.getValue();
            if (action != JmiValidationAction.DELETION) {
                checkJmiConstraints(obj);
            }
        }
    }

    // TODO jvs 4-Sept-2006:  Move this interface to JmiChangeDispatcher
    protected void checkJmiConstraints(RefObject obj)
    {
        // e.g. subclass can call FarragoRepos.verifyIntegrity
    }

    // implement MDRPreChangeListener
    public void plannedChange(MDRChangeEvent event)
    {
        if (tracer.isLoggable(Level.FINE)) {
            tracer.fine(event.toString());
        }

        // ignore events from other threads
        if (activeThread != Thread.currentThread()) {
            // REVIEW:  This isn't going to be good enough if we have
            // reentrant change set activity.
            return;
        }

        // NOTE:  do not throw exceptions from this method, because MDR will
        // swallow them!  Instead, use enqueueValidationExcn(), and the
        // exception will be thrown when validate() is called.
        if (event.getType() == InstanceEvent.EVENT_INSTANCE_DELETE) {
            RefObject obj = (RefObject) (event.getSource());
            dispatcher.clearDependencySuppliers(obj);
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
                scheduleModification(associationEvent.getOldElement());
            }
            if (associationEvent.getNewElement() != null) {
                scheduleModification(associationEvent.getNewElement());
            }
            if (event.getType() == AssociationEvent.EVENT_ASSOCIATION_REMOVE) {
                RefAssociation refAssoc =
                    (RefAssociation) associationEvent.getSource();
                List<JmiDeletionRule> rules =
                    deletionRules.getMulti(refAssoc.getClass());
                for (JmiDeletionRule rule : rules) {
                    if ((rule != null)
                        && rule.getEndName().equals(
                            associationEvent.getEndName())) {
                        fireDeletionRule(
                            refAssoc,
                            rule,
                            associationEvent.getFixedElement(),
                            associationEvent.getOldElement());
                    }
                }
            }
        } else {
            // REVIEW jvs 19-Nov-2005:  Somehow these slip through even
            // though we mask them out.  Probably an MDR bug.
            assert (event.getType() == InstanceEvent.EVENT_INSTANCE_CREATE);
        }
    }

    // implement MDRPreChangeListener
    public void changeCancelled(MDRChangeEvent event)
    {
        // don't care
    }

    // implement MDRChangeListener
    public void change(MDRChangeEvent event)
    {
        // don't care
    }

    public void validate()
    {
        checkValidationExcnQueue();

        boolean deleting = !deleteQueue.isEmpty();

        // Process deletions until a fixpoint is reached, using MDR events
        // to implement RESTRICT/CASCADE.
        while (!deleteQueue.isEmpty()) {
            RefObject refObj = deleteQueue.iterator().next();
            deleteQueue.remove(refObj);
            JmiValidationAction action = schedulingMap.get(refObj.refMofId());
            if (action != JmiValidationAction.DELETION) {
                if (tracer.isLoggable(Level.FINE)) {
                    tracer.fine("probe deleting " + refObj);
                }
                refObj.refDelete();
            }
        }

        // In order to validate deletion, we need the objects to exist, but
        // they've already been deleted.  So rollback the deletion now, but
        // we still remember the objects encountered above.
        if (deleting) {
            rollbackDeletions();
        }

        // REVIEW:  This may need to get more complicated in the future.
        // Like, if deletions triggered modifications to some referencing
        // object which needs to have its update validation run AFTER the
        // deletion takes place, not before.

        while (!schedulingMap.isEmpty()) {
            checkValidationExcnQueue();

            // Swap in a new map so new scheduling calls aren't handled until
            // the next round.
            transitMap = schedulingMap;
            schedulingMap = new LinkedHashMap<String, JmiValidationAction>();

            setOrdinalsInTransitMap();

            boolean progress = false;
            boolean unvalidatedDependency = false;
            for (Map.Entry<String, JmiValidationAction> mapEntry
                : transitMap.entrySet()) {
                RefObject obj =
                    (RefObject) getMdrRepos().getByMofId(mapEntry.getKey());
                if (obj == null) {
                    continue;
                }
                JmiValidationAction action = mapEntry.getValue();

                // mark this object as already validated so it doesn't slip
                // back in by updating itself
                validatedMap.put(obj, action);

                try {
                    dispatcher.validateAction(obj, action);
                    progress = true;
                } catch (JmiUnvalidatedDependencyException ex) {
                    // Something hit an unvalidated dependency; we'll have
                    // to retry this object later.
                    unvalidatedDependency = true;
                    validatedMap.remove(obj);
                    schedulingMap.put(
                        obj.refMofId(),
                        action);
                }
            }
            transitMap = null;
            if (unvalidatedDependency && !progress) {
                // Every single object hit a
                // JmiUnvalidatedDependencyException.  This implies a
                // cycle.  TODO:  identify the cycle in the exception.
                throw new JmiUnvalidatedDependencyException();
            }
        }

        // one last time
        checkValidationExcnQueue();
    }

    public void defineDeletionRule(
        RefAssociation refAssoc,
        JmiDeletionRule dropRule)
    {
        // NOTE:  use class object because in some circumstances MDR makes up
        // multiple instances of the same association, but doesn't implement
        // equals/hashCode correctly.
        deletionRules.putMulti(
            refAssoc.getClass(),
            dropRule);
    }

    public void validateUniqueNames(
        RefObject container,
        Collection<RefObject> collection,
        boolean includeType)
    {
        Map<String, RefObject> nameMap = new LinkedHashMap<String, RefObject>();
        for (RefObject element : collection) {
            String nameKey = dispatcher.getNameKey(element, includeType);
            if (nameKey == null) {
                continue;
            }

            RefObject other = nameMap.get(nameKey);
            if (other != null) {
                if (dispatcher.isNewObject(other)
                    && dispatcher.isNewObject(element)) {
                    // clash between two new objects being defined
                    // simultaneously
                    dispatcher.notifyNameCollision(
                        container,
                        element,
                        other,
                        true);
                    continue;
                } else {
                    RefObject newElement;
                    RefObject oldElement;
                    if (dispatcher.isNewObject(other)) {
                        newElement = other;
                        oldElement = element;
                    } else {
                        newElement = element;
                        oldElement = other;
                    }

                    // new object clashes with existing object
                    dispatcher.notifyNameCollision(
                        container,
                        newElement,
                        oldElement,
                        false);
                    continue;
                }
            }
            nameMap.put(nameKey, element);
        }
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

    private void fireDeletionRule(
        RefAssociation refAssoc,
        JmiDeletionRule rule,
        RefObject droppedEnd,
        RefObject otherEnd)
    {
        if (rule.isReversed()) {
            // Swap ends
            RefObject tmp = droppedEnd;
            droppedEnd = otherEnd;
            otherEnd = tmp;
        }
        JmiValidationAction scheduledAction =
            schedulingMap.get(droppedEnd.refMofId());
        if (scheduledAction != JmiValidationAction.DELETION) {
            // Spurious notification from opposite end
            return;
        }
        if ((rule.getSuperInterface() != null)
            && !(rule.getSuperInterface().isInstance(droppedEnd))) {
            return;
        }
        JmiDeletionAction action = rule.getAction();

        if ((action == JmiDeletionAction.CASCADE)
            || (dispatcher.getDeletionAction() == JmiDeletionAction.CASCADE)) {
            if (singleLevelCascade) {
                scheduleDeletion(otherEnd);
            } else {
                deleteQueue.add(otherEnd);
            }
            dispatcher.notifyDeleteEffect(
                otherEnd,
                JmiDeletionAction.CASCADE);
            return;
        }

        if (dispatcher.getDeletionAction() == JmiDeletionAction.INVALIDATE) {
            // Don't actually delete anything; instead just
            // break the link and leave a dangling reference.
            // NOTE jvs 29-Dec-2005:  This won't work if we ever need to
            // INVALIDATE a composite association; in that case
            // we would need to explicitly queue an action to
            // break the link.
            dispatcher.notifyDeleteEffect(
                otherEnd,
                JmiDeletionAction.INVALIDATE);
            return;
        }

        // NOTE: We can't construct the exception now since the object is
        // deleted.  Instead, defer until after rollback.
        final String mofId = droppedEnd.refMofId();
        enqueueValidationExcn(new DeferredException() {
                RuntimeException getException()
                {
                    RefObject droppedElement =
                        (RefObject) getMdrRepos().getByMofId(mofId);
                    return new JmiRestrictException(droppedElement);
                }
            });
    }

    private void rollbackDeletions()
    {
        // A savepoint or chained transaction would be smoother, but MDR doesn't
        // support those.  Instead, have to restart the txn altogether.  Take
        // advantage of the fact that MDR allows us to retain references across
        // txns.
        getMdrRepos().endTrans(true);

        // TODO:  really need a lock to protect us against someone else's
        // change here, which could invalidate our schedulingMap.
        getMdrRepos().beginTrans(true);
    }

    /**
     * Explicitly schedules an object for deletion. Objects may also be
     * scheduled for deletion implicitly via cascades, or by listening for
     * refDelete events.
     *
     * @param obj object whose deletion is to be scheduled as part of this
     * change
     */
    public void scheduleDeletion(RefObject obj)
    {
        assert (!validatedMap.containsKey(obj));

        // delete overrides anything else
        schedulingMap.put(
            obj.refMofId(),
            JmiValidationAction.DELETION);
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
        JmiValidationAction action = JmiValidationAction.MODIFICATION;
        if (dispatcher.isNewObject(obj)) {
            action = JmiValidationAction.CREATION;
        }
        schedulingMap.put(
            obj.refMofId(),
            action);
    }

    /**
     * Explicitly schedules an object for creation or modification (which one
     * depends on the result of JmiChangeDispatcher.isNewObject). This can be
     * used to include objects for which events were not heard by the listener
     * mechanism.
     *
     * @param obj object whose creation or modification is to be scheduled as
     * part of this change
     */
    public void scheduleObject(RefObject obj)
    {
        scheduleModification(obj);
    }

    /**
     * Requests that events on an object be ignored.
     *
     * @param obj object to be ignored
     */
    public void scheduleIgnore(RefObject obj)
    {
        schedulingMap.remove(obj.refMofId());
        validatedMap.put(obj, JmiValidationAction.MODIFICATION);
    }

    /**
     * Calls setOrdinals for each object being created or modified in
     * transitMap.
     */
    private void setOrdinalsInTransitMap()
    {
        for (Map.Entry<String, JmiValidationAction> mapEntry
            : transitMap.entrySet()) {
            JmiValidationAction action = mapEntry.getValue();
            if (action == JmiValidationAction.DELETION) {
                continue;
            }
            RefObject refObj =
                (RefObject) getMdrRepos().getByMofId(mapEntry.getKey());
            // Check for null because object might have been deleted
            // by a trigger.
            if (refObj != null) {
                setOrdinals(refObj);
            }
        }
    }

    /**
     * Sets the "ordinal" attribute of all objects which are targets of ordered
     * composite associations with the given object as the source. The ordinal
     * attribute (if it exists) is set to match the 0-based association order.
     *
     * @param refObj source of associations to maintain
     */
    private void setOrdinals(RefObject refObj)
    {
        JmiModelView modelView = dispatcher.getModelView();
        JmiClassVertex classVertex =
            modelView.getModelGraph().getVertexForRefClass(
                refObj.refClass());
        assert (classVertex != null);
        Set edges = modelView.getAllOutgoingAssocEdges(classVertex);
        for (Object edgeObj : edges) {
            JmiAssocEdge edge = (JmiAssocEdge) edgeObj;
            if (edge.getSourceEnd().getAggregation()
                != AggregationKindEnum.COMPOSITE) {
                continue;
            }
            if (!edge.getTargetEnd().getMultiplicity().isOrdered()) {
                continue;
            }

            Collection targets =
                edge.getRefAssoc().refQuery(
                    edge.getSourceEnd(),
                    refObj);

            // It's an ordered end, so it should be a List.
            assert (targets instanceof List);

            int nextOrdinal = 0;
            for (Object targetObj : targets) {
                RefObject target = (RefObject) targetObj;
                Integer oldOrdinal = null;
                Integer newOrdinal = nextOrdinal;
                ++nextOrdinal;
                try {
                    oldOrdinal = (Integer) target.refGetValue("ordinal");
                } catch (InvalidNameException ex) {
                    // No ordinal attribute to be maintained.
                    continue;
                }

                // Avoid unnecessary updates in the case where ordinals
                // are already correct.
                if (oldOrdinal != newOrdinal) {
                    target.refSetValue("ordinal", newOrdinal);
                }
            }
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
        abstract RuntimeException getException();
    }
}

// End JmiChangeSet.java
