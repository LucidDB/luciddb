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

import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.*;

import java.util.*;

/**
 * RelMdUniqueKeys supplies a default implementation of
 * {@link RelMetadataQuery#getUniqueKeys} for the standard logical algebra.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class RelMdUniqueKeys extends ReflectiveRelMetadataProvider
{
    public Set<BitSet> getUniqueKeys(FilterRelBase rel)
    {
        return RelMetadataQuery.getUniqueKeys(rel.getChild());
    }
    
    public Set<BitSet> getUniqueKeys(SortRel rel)
    {
        return RelMetadataQuery.getUniqueKeys(rel.getChild());
    }
    
    public Set<BitSet> getUniqueKeys(ProjectRelBase rel)
    {
        return RelMetadataQuery.getUniqueKeys(rel.getChild());
    }
    
    public Set<BitSet> getUniqueKeys(JoinRelBase rel)
    {
        // TODO - need to account for outer joins
        
        // locate the columns that participate in equijoins
        BitSet leftJoinCols = new BitSet();
        BitSet rightJoinCols = new BitSet();
        RelMdUtil.findEquiJoinCols(
            rel, rel.getCondition(), leftJoinCols, rightJoinCols);
        
        // determine if either or both the LHS and RHS are unique on the
        // equijoin columns
        RelNode left = rel.getLeft();
        RelNode right = rel.getRight();
        Boolean leftUnique = RelMdUtil.areColumnsUnique(left, leftJoinCols);
        Boolean rightUnique = RelMdUtil.areColumnsUnique(right, rightJoinCols);

        // add bits from left and/or right depending on which sides are
        // unique
        Set<BitSet> retSet = new HashSet<BitSet>();
        if (rightUnique != null && rightUnique) {
            Set<BitSet> leftSet = RelMetadataQuery.getUniqueKeys(left);
            if (leftSet == null) {
                return null;
            }
            retSet.addAll(leftSet);
        }
        // bits on the right need to be adjusted to reflect addition of left
        // input
        if (leftUnique != null && leftUnique) {
            int nFieldsOnLeft = left.getRowType().getFieldCount();
            Set<BitSet> rightSet = RelMetadataQuery.getUniqueKeys(right);
            if (rightSet == null) {
                return null;
            }
            Iterator it = rightSet.iterator();
            while (it.hasNext()) {
                BitSet colMask = (BitSet) it.next();
                BitSet tmpMask = new BitSet();
                for (int bit = colMask.nextSetBit(0); bit >= 0;
                    bit = colMask.nextSetBit(bit + 1))
                {
                    tmpMask.set(bit + nFieldsOnLeft);
                }
                retSet.add(tmpMask);
            }
        }
        
        if (leftUnique == null && rightUnique == null) {
            return null;
        }

        return retSet;
    }
    
    public Set<BitSet> getUniqueKeys(SemiJoinRel rel)
    {
        // only return the unique keys from the LHS since a semijoin only
        // returns the LHS
        return RelMetadataQuery.getUniqueKeys(rel.getLeft());
    }
    
    public Set<BitSet> getUniqueKeys(AggregateRelBase rel)
    {
        Set<BitSet> retSet = new HashSet<BitSet>();
        
        // group by keys form a unique key
        if (rel.getGroupCount() > 0) {
            BitSet groupKey = new BitSet();
            for (int i = 0; i < rel.getGroupCount(); i++) {
                groupKey.set(i);
            }
            retSet.add(groupKey);
        }
        return retSet;
    }
    
    // Catch-all rule when none of the others apply.
    public Set<BitSet> getUniqueKeys(RelNode rel)
    {
        // no information available
        return null;
    }
}

// End RelMdUniqueKeys.java
