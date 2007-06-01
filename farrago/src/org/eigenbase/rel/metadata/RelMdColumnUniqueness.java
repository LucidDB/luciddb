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
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;


/**
 * RelMdColumnUniqueness supplies a default implementation of {@link
 * RelMetadataQuery#areColumnsUnique} for the standard logical algebra.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class RelMdColumnUniqueness
    extends ReflectiveRelMetadataProvider
{
    //~ Constructors -----------------------------------------------------------

    public RelMdColumnUniqueness()
    {
        // Tell superclass reflection about parameter types expected
        // for various metadata queries.

        // This corresponds to areColumnsUnique(rel, BitSet columns);
        // note that we don't specify the rel type because we always overload
        // on that.
        mapParameterTypes(
            "areColumnsUnique",
            Collections.singletonList((Class) BitSet.class));
    }

    //~ Methods ----------------------------------------------------------------

    public Boolean areColumnsUnique(FilterRelBase rel, BitSet columns)
    {
        return RelMetadataQuery.areColumnsUnique(rel.getChild(), columns);
    }

    public Boolean areColumnsUnique(SortRel rel, BitSet columns)
    {
        return RelMetadataQuery.areColumnsUnique(rel.getChild(), columns);
    }

    public Boolean areColumnsUnique(CorrelatorRel rel, BitSet columns)
    {
        return RelMetadataQuery.areColumnsUnique(rel.getLeft(), columns);
    }

    public Boolean areColumnsUnique(ProjectRelBase rel, BitSet columns)
    {
        // ProjectRel maps a set of rows to a different set;
        // Without knowledge of the mapping function(whether it
        // preserves uniqueness), it is only safe to derive uniqueness
        // info from the child of a project when the mapping is f(a) => a.
        //
        // Also need to map the input column set to the corresponding child
        // references

        RexNode [] projExprs = rel.getProjectExps();
        BitSet childColumns = new BitSet();
        for (
            int bit = columns.nextSetBit(0);
            bit >= 0;
            bit = columns.nextSetBit(bit + 1))
        {
            RexNode projExpr = projExprs[bit];
            if (projExpr instanceof RexInputRef) {
                childColumns.set(((RexInputRef) projExpr).getIndex());
            } else {
                return null;
            }
        }

        return RelMetadataQuery.areColumnsUnique(rel.getChild(), childColumns);
    }

    public Boolean areColumnsUnique(JoinRelBase rel, BitSet columns)
    {
        if (columns.cardinality() == 0) {
            return false;
        }

        RelNode left = rel.getLeft();
        RelNode right = rel.getRight();

        // Divide up the input column mask into column masks for the left and
        // right sides of the join
        BitSet leftColumns = new BitSet();
        BitSet rightColumns = new BitSet();
        int nLeftColumns = left.getRowType().getFieldCount();
        for (
            int bit = columns.nextSetBit(0);
            bit >= 0;
            bit = columns.nextSetBit(bit + 1))
        {
            if (bit < nLeftColumns) {
                leftColumns.set(bit);
            } else {
                rightColumns.set(bit - nLeftColumns);
            }
        }

        // If the original column mask contains columns from both the left and
        // right hand side, then the columns are unique if and only if they're
        // unique for their respective join inputs
        Boolean leftUnique =
            RelMetadataQuery.areColumnsUnique(left, leftColumns);
        Boolean rightUnique =
            RelMetadataQuery.areColumnsUnique(right, rightColumns);
        if ((leftColumns.cardinality() > 0)
            && (rightColumns.cardinality() > 0))
        {
            if ((leftUnique == null) || (rightUnique == null)) {
                return null;
            } else {
                return (leftUnique && rightUnique);
            }
        }

        // If we're only trying to determine uniqueness for columns that
        // originate from one join input, then determine if the equijoin
        // columns from the other join input are unique.  If they are, then
        // the columns are unique for the entire join if they're unique for
        // the corresponding join input, provided that input is not null
        // generating.
        BitSet leftJoinCols = new BitSet();
        BitSet rightJoinCols = new BitSet();
        RelMdUtil.findEquiJoinCols(
            left,
            right,
            rel.getCondition(),
            leftJoinCols,
            rightJoinCols);

        if (leftColumns.cardinality() > 0) {
            if (rel.getJoinType().generatesNullsOnLeft()) {
                return false;
            }
            Boolean rightJoinColsUnique =
                RelMetadataQuery.areColumnsUnique(right, rightJoinCols);
            if ((rightJoinColsUnique == null) || (leftUnique == null)) {
                return null;
            }
            return (rightJoinColsUnique && leftUnique);
        } else if (rightColumns.cardinality() > 0) {
            if (rel.getJoinType().generatesNullsOnRight()) {
                return false;
            }
            Boolean leftJoinColsUnique =
                RelMetadataQuery.areColumnsUnique(left, leftJoinCols);
            if ((leftJoinColsUnique == null) || (rightUnique == null)) {
                return null;
            }
            return (leftJoinColsUnique && rightUnique);
        }

        assert (false);
        return null;
    }

    public Boolean areColumnsUnique(SemiJoinRel rel, BitSet columns)
    {
        // only return the unique keys from the LHS since a semijoin only
        // returns the LHS
        return RelMetadataQuery.areColumnsUnique(rel.getLeft(), columns);
    }

    public Boolean areColumnsUnique(AggregateRelBase rel, BitSet columns)
    {
        // group by keys form a unique key
        if (rel.getGroupCount() > 0) {
            BitSet groupKey = new BitSet();
            for (int i = 0; i < rel.getGroupCount(); i++) {
                groupKey.set(i);
            }
            return RelOptUtil.contains(columns, groupKey);
        } else {
            return false;
        }
    }

    // Catch-all rule when none of the others apply.
    public Boolean areColumnsUnique(RelNode rel, BitSet columns)
    {
        // no information available
        return null;
    }
}

// End RelMdColumnUniqueness.java
