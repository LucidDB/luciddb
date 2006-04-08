/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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
package org.eigenbase.relopt.hep;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.util.*;

/**
 * HepRelVertex wraps a real {@link RelNode} as a vertex in a
 * DAG representing the entire query expression.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class HepRelVertex extends AbstractRelNode
{
    /**
     * Original expression from which this expression originated.
     * This is used for answering logical metadata queries, since
     * physical rels may have lost information during transformation.
     */
    private final RelNode originalRel;

    /**
     * Wrapped rel currently chosen for implementation of expression.
     */
    private RelNode currentRel;

    HepRelVertex(RelNode rel)
    {
        super(rel.getCluster(), rel.getTraits());
        originalRel = rel;
        currentRel = rel;
    }

    // implement RelNode
    public Object clone()
    {
        return this;
    }
    
    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        // HepRelMetadataProvider is supposed to intercept this
        // and redirect to the real rels.
        throw Util.newInternal("should never get here");
    }

    // implement RelNode
    public double getRows()
    {
        return RelMetadataQuery.getRowCount(currentRel);
    }

    // implement RelNode
    protected RelDataType deriveRowType()
    {
        return originalRel.getRowType();
    }

    // implement RelNode
    public boolean isDistinct()
    {
        return originalRel.isDistinct();
    }
    
    // implement RelNode
    protected String computeDigest()
    {
        return "HepRelVertex(" + currentRel + ")";
    }
    
    /**
     * Replaces the implementation for this expression with a new one.
     *
     * @param newRel new expression
     */
    void replaceRel(RelNode newRel)
    {
        currentRel = newRel;
    }

    /**
     * @return original rel which first caused this vertex to be created
     */
    public RelNode getOriginalRel()
    {
        return originalRel;
    }

    /**
     * @return current implementation chosen for this vertex
     */
    public RelNode getCurrentRel()
    {
        return currentRel;
    }
}

// End HepRelVertex.java
