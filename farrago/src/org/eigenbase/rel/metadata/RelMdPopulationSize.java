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
package org.eigenbase.rel.metadata;

import org.eigenbase.util14.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.*;

import java.util.*;

/**
 * RelMdPopulationSize supplies a default implementation of
 * {@link RelMetadataQuery#getPopulationSize} for the standard logical algebra.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class RelMdPopulationSize extends ReflectiveRelMetadataProvider
{
    public RelMdPopulationSize()
    {
        // Tell superclass reflection about parameter types expected
        // for various metadata queries.

        // This corresponds to getPopulationSize(rel, BitSet groupKey);
        // note that we don't specify the rel type because we always overload
        // on that.
        mapParameterTypes(
            "getPopulationSize",
            Collections.singletonList((Class) BitSet.class));
    }
    
    public Double getPopulationSize(FilterRelBase rel, BitSet groupKey)
    {
        return RelMetadataQuery.getPopulationSize(rel.getChild(), groupKey);
    }
    
    public Double getPopulationSize(SortRel rel, BitSet groupKey)
    {
        return RelMetadataQuery.getPopulationSize(rel.getChild(), groupKey);
    }
    
    public Double getPopulationSize(UnionRelBase rel, BitSet groupKey)
    {
        Double population = 0.0;
        for (RelNode input : rel.getInputs()) {
            Double subPop = RelMetadataQuery.getPopulationSize(input, groupKey);
            if (subPop == null) {
                return null;
            }
            population += subPop;
        }
        return population;
    }
    
    public Double getPopulationSize(JoinRelBase rel, BitSet groupKey)
    {
        BitSet leftMask = new BitSet();
        BitSet rightMask = new BitSet();
        
        // separate the mask into masks for the left and right
        RelMdUtil.setLeftRightBitmaps(
            groupKey, leftMask, rightMask,
            rel.getLeft().getRowType().getFieldCount());   
        
        return NumberUtil.multiply(
            RelMetadataQuery.getPopulationSize(rel.getLeft(), leftMask),
            RelMetadataQuery.getPopulationSize(rel.getRight(), rightMask));
    }
    
    public Double getPopulationSize(SemiJoinRel rel, BitSet groupKey)
    {
        return RelMetadataQuery.getPopulationSize(rel.getLeft(), groupKey);
    }
    
    public Double getPopulationSize(AggregateRelBase rel, BitSet groupKey)
    {
        BitSet childKey = new BitSet();
        RelMdUtil.setAggChildKeys(groupKey, rel, childKey);
        return RelMetadataQuery.getPopulationSize(rel.getChild(), childKey);
    }
    
    public Double getPopulationSize(ValuesRelBase rel, BitSet groupKey)
    {
        // assume half the rows are duplicates
        return rel.getRows() / 2;
    }
    
    // Catch-all rule when none of the others apply.  Have not implemented
    // rules for aggregation and projection.
    public Double getPopulationSize(RelNode rel, BitSet groupKey)
    {
        // if the keys are unique, return the row count; otherwise, we have
        // no further information on which to return any legitimate value
        
        // REVIEW zfong 4/11/06 - Broadbase code returns the product of each
        // unique key, which would result in the population being larger
        // than the total rows in the relnode
        Boolean uniq = RelMdUtil.areColumnsUnique(rel, groupKey);
        if (uniq != null && uniq == true) {
            return RelMetadataQuery.getRowCount(rel);
        }
        
        return null;
    }
    
}

// End RelMdPopulationSize.java
