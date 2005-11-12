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

import javax.jmi.reflect.*;
import javax.jmi.model.*;

/**
 * JmiDependencyMappedTransform implements {@link JmiDependencyTransform} by
 * keeping a map from {@link JmiAssocEdge} to {@link JmiAssocMapping}.
 *
 * @author John Sichi
 * @version $Id$
 */
public class JmiDependencyMappedTransform implements JmiDependencyTransform
{
    private final Map<JmiAssocEdge, JmiAssocMapping> map;

    private final JmiModelView modelView;

    private final boolean produceSelfLoops;

    /**
     * Creates a new mapped transform.  Initially, all associations are mapped
     * to JmiAssocMapping.REMOVAL.  Callers should use setXXX methods to change
     * mappings.
     *
     * @param modelView the model for which dependencies are being
     * defined
     *
     * @param produceSelfLoops see {@link
     * JmiDependencyTransform#shouldProduceSelfLoops}
     */
    public JmiDependencyMappedTransform(
        JmiModelView modelView,
        boolean produceSelfLoops)
    {
        this.modelView = modelView;
        this.produceSelfLoops = produceSelfLoops;
        map = new HashMap<JmiAssocEdge, JmiAssocMapping>();
    }
    
    // implement JmiDependencyTransform
    public Collection<RefObject> getSourceNeighbors(
        RefObject target,
        Collection<RefObject> candidates,
        JmiAssocMapping mapping)
    {
        Collection<RefObject> collection = new ArrayList<RefObject>();
        RefClass refClass = target.refClass();
        JmiClassVertex targetClassVertex =
            modelView.getModelGraph().getVertexForRefClass(refClass);
        for (Object assocEdgeObj
                 : modelView.getAllIncomingAssocEdges(targetClassVertex))
        {
            JmiAssocEdge assocEdge = (JmiAssocEdge) assocEdgeObj;
            JmiAssocMapping edgeMapping = map.get(assocEdge);
            if (edgeMapping != mapping) {
                continue;
            }
            if (!(target.refIsInstanceOf(
                      assocEdge.getTargetEnd().getType(), true)))
            {
                continue;
            }
            collection.addAll(
                assocEdge.getRefAssoc().refQuery(
                    assocEdge.getTargetEnd(), target));
        }
        collection.retainAll(candidates);
        return collection;
    }
    
    // implement JmiDependencyTransform
    public boolean shouldProduceSelfLoops()
    {
        return produceSelfLoops;
    }

    /**
     * Sets mappings for all associations with a given aggregation kind.
     *
     * @param requestedKind association filter
     *
     * @param mapping mapping to use for matching associations
     */
    public void setAllByAggregation(
        AggregationKind requestedKind, JmiAssocMapping mapping)
    {
        for (Object assocEdgeObj
                 : modelView.getModelGraph().getAssocGraph().edgeSet())
        {
            JmiAssocEdge assocEdge = (JmiAssocEdge) assocEdgeObj;
            AggregationKind actualKind = AggregationKindEnum.NONE;
            for (int i = 0; i < 2; i++) {
                AssociationEnd end = assocEdge.getEnd(i);
                if (end.getAggregation() != AggregationKindEnum.NONE) {
                    actualKind = end.getAggregation();
                }
            }
            if (requestedKind == actualKind) {
                map.put(assocEdge, mapping);
            }
        }
    }

    /**
     * Sets mapping for a specific association.
     *
     * @param assoc association to map
     *
     * @param mapping mapping to use
     */
    public void setByRefAssoc(
        RefAssociation assoc, JmiAssocMapping mapping)
    {
        JmiAssocEdge assocEdge =
            modelView.getModelGraph().getEdgeForRefAssoc(assoc);
        assert(assocEdge != null);
        map.put(assocEdge, mapping);
    }
}

// End JmiDependencyMappedTransform.java
