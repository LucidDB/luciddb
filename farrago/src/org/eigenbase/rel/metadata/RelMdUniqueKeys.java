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

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.rex.*;


/**
 * RelMdUniqueKeys supplies a default implementation of {@link
 * RelMetadataQuery#getUniqueKeys} for the standard logical algebra.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class RelMdUniqueKeys
    extends ReflectiveRelMetadataProvider
{
    //~ Methods ----------------------------------------------------------------

    public Set<BitSet> getUniqueKeys(FilterRelBase rel)
    {
        return RelMetadataQuery.getUniqueKeys(rel.getChild());
    }

    public Set<BitSet> getUniqueKeys(SortRel rel)
    {
        return RelMetadataQuery.getUniqueKeys(rel.getChild());
    }

    public Set<BitSet> getUniqueKeys(CorrelatorRel rel)
    {
        return RelMetadataQuery.getUniqueKeys(rel.getLeft());
    }

    public Set<BitSet> getUniqueKeys(ProjectRelBase rel)
    {
        // ProjectRel maps a set of rows to a different set;
        // Without knowledge of the mapping function(whether it
        // preserves uniqueness), it is only safe to derive uniqueness
        // info from the child of a project when the mapping is f(a) => a.
        //
        // Further more, the unique bitset coming from the child needs
        // to be mapped to match the output of the project.
        Map<Integer, Integer> mapInToOutPos = new HashMap<Integer, Integer>();

        RexNode [] projExprs = rel.getProjectExps();

        Set<BitSet> projUniqueKeySet = new HashSet<BitSet>();

        // Build an input to ouput position map.
        for (int i = 0; i < projExprs.length; i++) {
            RexNode projExpr = projExprs[i];
            if (projExpr instanceof RexInputRef) {
                mapInToOutPos.put(((RexInputRef) projExpr).getIndex(), i);
            } else {
                continue;
            }
        }

        if (mapInToOutPos.isEmpty()) {
            // if there's no RexInputRef in the projected expressions
            // return empty set.
            return projUniqueKeySet;
        }

        Set<BitSet> childUniqueKeySet =
            RelMetadataQuery.getUniqueKeys(rel.getChild());

        if (childUniqueKeySet != null) {
            // Now add to the projUniqueKeySet the child keys that are fully
            // projected.
            Iterator itChild = childUniqueKeySet.iterator();

            while (itChild.hasNext()) {
                BitSet colMask = (BitSet) itChild.next();
                BitSet tmpMask = new BitSet();
                boolean completeKeyProjected = true;
                for (
                    int bit = colMask.nextSetBit(0);
                    bit >= 0;
                    bit = colMask.nextSetBit(bit + 1))
                {
                    if (mapInToOutPos.containsKey(bit)) {
                        tmpMask.set(mapInToOutPos.get(bit));
                    } else {
                        // Skip the child unique key if part of it is not
                        // projected.
                        completeKeyProjected = false;
                        break;
                    }
                }
                if (completeKeyProjected) {
                    projUniqueKeySet.add(tmpMask);
                }
            }
        }

        return projUniqueKeySet;
    }

    public Set<BitSet> getUniqueKeys(JoinRelBase rel)
    {
        RelNode left = rel.getLeft();
        RelNode right = rel.getRight();

        // first add the different combinations of concatenated unique keys
        // from the left and the right, adjusting the right hand side keys to
        // reflect the addition of the left hand side
        //
        // NOTE zfong 12/18/06 - If the number of tables in a join is large,
        // the number of combinations of unique key sets will explode.  If
        // that is undesirable, use RelMetadataQuery.areColumnsUnique() as
        // an alternative way of getting unique key information.

        Set<BitSet> retSet = new HashSet<BitSet>();
        Set<BitSet> leftSet = RelMetadataQuery.getUniqueKeys(left);
        Set<BitSet> rightSet = null;

        Set<BitSet> tmpRightSet = RelMetadataQuery.getUniqueKeys(right);
        int nFieldsOnLeft = left.getRowType().getFieldCount();

        if (tmpRightSet != null) {
            rightSet = new HashSet<BitSet>();
            Iterator itRight = tmpRightSet.iterator();
            while (itRight.hasNext()) {
                BitSet colMask = (BitSet) itRight.next();
                BitSet tmpMask = new BitSet();
                for (
                    int bit = colMask.nextSetBit(0);
                    bit >= 0;
                    bit = colMask.nextSetBit(bit + 1))
                {
                    tmpMask.set(bit + nFieldsOnLeft);
                }
                rightSet.add(tmpMask);
            }

            if (leftSet != null) {
                itRight = rightSet.iterator();
                while (itRight.hasNext()) {
                    BitSet colMaskRight = (BitSet) itRight.next();
                    Iterator itLeft = leftSet.iterator();
                    while (itLeft.hasNext()) {
                        BitSet colMaskLeft = (BitSet) itLeft.next();
                        BitSet colMaskConcat = new BitSet();
                        colMaskConcat.or(colMaskLeft);
                        colMaskConcat.or(colMaskRight);
                        retSet.add(colMaskConcat);
                    }
                }
            }
        }

        // locate the columns that participate in equijoins
        BitSet leftJoinCols = new BitSet();
        BitSet rightJoinCols = new BitSet();
        RelMdUtil.findEquiJoinCols(
            left,
            right,
            rel.getCondition(),
            leftJoinCols,
            rightJoinCols);

        // determine if either or both the LHS and RHS are unique on the
        // equijoin columns
        Boolean leftUnique =
            RelMetadataQuery.areColumnsUnique(left, leftJoinCols);
        Boolean rightUnique =
            RelMetadataQuery.areColumnsUnique(right, rightJoinCols);

        // if the right hand side is unique on its equijoin columns, then we can
        // add the unique keys from left if the left hand side is not null
        // generating
        if ((rightUnique != null)
            && rightUnique
            && (leftSet != null)
            && !(rel.getJoinType().generatesNullsOnLeft()))
        {
            retSet.addAll(leftSet);
        }

        // same as above except left and right are reversed
        if ((leftUnique != null)
            && leftUnique
            && (rightSet != null)
            && !(rel.getJoinType().generatesNullsOnRight()))
        {
            retSet.addAll(rightSet);
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
