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

import javax.jmi.model.*;
import javax.jmi.reflect.*;


/**
 * JmiDependencyMappedTransform implements {@link JmiDependencyTransform} by
 * keeping a map from {@link JmiAssocEdge} to {@link JmiAssocMapping}.
 *
 * @author John Sichi
 * @version $Id$
 */
public class JmiDependencyMappedTransform
    implements JmiDependencyTransform
{

    //~ Instance fields --------------------------------------------------------

    private final Map<JmiAssocEdge, List<AssocRule>> map;

    private final JmiModelView modelView;

    private final boolean produceSelfLoops;

    private Comparator<RefBaseObject> tieBreaker;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new mapped transform. Initially, all associations are mapped to
     * JmiAssocMapping.REMOVAL. Callers should use setXXX methods to change
     * mappings.
     *
     * @param modelView the model for which dependencies are being defined
     * @param produceSelfLoops see {@link
     * JmiDependencyTransform#shouldProduceSelfLoops}
     */
    public JmiDependencyMappedTransform(
        JmiModelView modelView,
        boolean produceSelfLoops)
    {
        this.modelView = modelView;
        this.produceSelfLoops = produceSelfLoops;
        map = new HashMap<JmiAssocEdge, List<AssocRule>>();
        tieBreaker = JmiMofIdComparator.instance;
    }

    //~ Methods ----------------------------------------------------------------

    // implement JmiDependencyTransform
    public Comparator<RefBaseObject> getTieBreaker()
    {
        return tieBreaker;
    }
    
    /**
     * Sets a new tie-breaker.  (Default after construction is
     * {@link JmiMofIdComparator#instance}.)
     *
     * @param tieBreaker new tie-breaker, or null to disable tie-breaking
     */
    public void setTieBreaker(Comparator<RefBaseObject> tieBreaker)
    {
        this.tieBreaker = tieBreaker;
    }

    // implement JmiDependencyTransform
    public Collection<RefObject> getSourceNeighbors(
        RefObject target,
        Collection<RefObject> candidates,
        JmiAssocMapping mapping)
    {
        List<RefObject> collection = new ArrayList<RefObject>();
        RefClass refClass = target.refClass();
        JmiClassVertex targetClassVertex =
            modelView.getModelGraph().getVertexForRefClass(refClass);
        for (Object assocEdgeObj
            : modelView.getAllIncomingAssocEdges(targetClassVertex)) {
            JmiAssocEdge assocEdge = (JmiAssocEdge) assocEdgeObj;
            List<AssocRule> rules = map.get(assocEdge);
            if (rules == null) {
                continue;
            }

            // optimize common case:  no refinements
            if (rules.size() == 1) {
                if (rules.get(0).mapping != mapping) {
                    continue;
                }
            }
            if (!(
                    target.refIsInstanceOf(
                        assocEdge.getTargetEnd().getType(),
                        true)
                 )) {
                continue;
            }
            Collection sources =
                assocEdge.getRefAssoc().refQuery(
                    assocEdge.getTargetEnd(),
                    target);

            if (rules.size() == 1) {
                AssocRule rule = rules.get(0);
                if ((rule.sourceClass == null) && (rule.targetClass == null)) {
                    // optimize common case:  no refinements
                    collection.addAll(sources);
                    continue;
                }
            }

            // deal with refinements
            for (Object obj : sources) {
                RefObject source = (RefObject) obj;
                applyRefinedRules(
                    rules,
                    source,
                    target,
                    collection,
                    mapping);
            }
        }
        collection.retainAll(candidates);
        if (tieBreaker != null) {
            Collections.sort(collection, tieBreaker);
        }
        return collection;
    }

    private void applyRefinedRules(List<AssocRule> rules,
        RefObject source,
        RefObject target,
        Collection result,
        JmiAssocMapping mapping)
    {
        for (AssocRule rule : rules) {
            if (rule.sourceClass != null) {
                if (!(source.refIsInstanceOf(rule.sourceClass, true))) {
                    continue;
                }
            }
            if (rule.targetClass != null) {
                if (!(target.refIsInstanceOf(rule.targetClass, true))) {
                    continue;
                }
            }
            if (rule.mapping == mapping) {
                result.add(source);
            }
            break;
        }
    }

    // implement JmiDependencyTransform
    public boolean shouldProduceSelfLoops()
    {
        return produceSelfLoops;
    }

    /**
     * Sets mappings for all associations with a given aggregation kind,
     * discarding any existing mappings for those associations.
     *
     * @param requestedKind association filter
     * @param mapping mapping to use for matching associations
     */
    public void setAllByAggregation(
        AggregationKind requestedKind,
        JmiAssocMapping mapping)
    {
        for (Object assocEdgeObj
            : modelView.getModelGraph().getAssocGraph().edgeSet()) {
            JmiAssocEdge assocEdge = (JmiAssocEdge) assocEdgeObj;
            AggregationKind actualKind = AggregationKindEnum.NONE;
            for (int i = 0; i < 2; i++) {
                AssociationEnd end = assocEdge.getEnd(i);
                if (end.getAggregation() != AggregationKindEnum.NONE) {
                    actualKind = end.getAggregation();
                }
            }
            if (requestedKind == actualKind) {
                map.put(
                    assocEdge,
                    new ArrayList<AssocRule>(
                        Collections.singleton(new AssocRule(mapping))));
            }
        }
    }

    /**
     * Sets mapping for a specific association, discarding any existing mappings
     * for that association.
     *
     * @param assoc association to map
     * @param mapping mapping to use
     */
    public void setByRefAssoc(
        RefAssociation assoc,
        JmiAssocMapping mapping)
    {
        JmiAssocEdge assocEdge =
            modelView.getModelGraph().getEdgeForRefAssoc(assoc);
        assert (assocEdge != null);
        map.put(
            assocEdge,
            new ArrayList<AssocRule>(
                Collections.singleton(new AssocRule(mapping))));
    }

    /**
     * Sets mapping for a specific association, refining the rule to only apply
     * in the context of specific end classes. Does not discard any existing
     * mappings. Refined mappings are used in the reverse of the order in which
     * they are defined (later definition overrides earlier).
     *
     * @param assoc association to map
     * @param mapping mapping to use
     * @param sourceClass source class required for match, or null for wildcard
     * @param targetClass target class required for match, or null for wildcard
     */
    public void setByRefAssocRefined(
        RefAssociation assoc,
        JmiAssocMapping mapping,
        RefClass sourceClass,
        RefClass targetClass)
    {
        JmiAssocEdge assocEdge =
            modelView.getModelGraph().getEdgeForRefAssoc(assoc);
        assert (assocEdge != null);
        List<AssocRule> list = map.get(assocEdge);
        if (list == null) {
            list = new ArrayList<AssocRule>();
            map.put(assocEdge, list);
        }

        // add in reverse order
        list.add(
            0,
            new AssocRule(
                mapping,
                convertRefClassToMof(sourceClass),
                convertRefClassToMof(targetClass)));
    }

    private MofClass convertRefClassToMof(RefClass c)
    {
        if (c == null) {
            return null;
        }
        return modelView.getModelGraph().getVertexForRefClass(c).getMofClass();
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class AssocRule
    {
        /**
         * Mapping to use when this rule applies.
         */
        final JmiAssocMapping mapping;

        /**
         * Source class required for match, or null for wildcard.
         */
        final MofClass sourceClass;

        /**
         * Target class required for match, or null for wildcard.
         */
        final MofClass targetClass;

        AssocRule(JmiAssocMapping mapping)
        {
            this(mapping, null, null);
        }

        AssocRule(
            JmiAssocMapping mapping,
            MofClass sourceClass,
            MofClass targetClass)
        {
            this.mapping = mapping;
            this.sourceClass = sourceClass;
            this.targetClass = targetClass;
        }
    }
}

// End JmiDependencyMappedTransform.java
