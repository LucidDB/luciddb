/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2009 The Eigenbase Project
// Copyright (C) 2006-2009 SQLstream, Inc.
// Copyright (C) 2006-2009 LucidEra, Inc.
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
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.fun.*;


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

        // This corresponds to areColumnsUnique(rel, BitSet columns,
        // boolean ignoreNulls);
        // note that we don't specify the rel type because we always overload
        // on that.
        List<Class> args = new ArrayList<Class>();
        args.add((Class) BitSet.class);
        args.add((Class) Boolean.TYPE);
        mapParameterTypes(
            "areColumnsUnique",
            args);
    }

    //~ Methods ----------------------------------------------------------------

    public Boolean areColumnsUnique(
        FilterRelBase rel,
        BitSet columns,
        boolean ignoreNulls)
    {
        return RelMetadataQuery.areColumnsUnique(
            rel.getChild(),
            columns,
            ignoreNulls);
    }

    public Boolean areColumnsUnique(
        SortRel rel,
        BitSet columns,
        boolean ignoreNulls)
    {
        return RelMetadataQuery.areColumnsUnique(
            rel.getChild(),
            columns,
            ignoreNulls);
    }

    public Boolean areColumnsUnique(
        CorrelatorRel rel,
        BitSet columns,
        boolean ignoreNulls)
    {
        return RelMetadataQuery.areColumnsUnique(
            rel.getLeft(),
            columns,
            ignoreNulls);
    }

    public Boolean areColumnsUnique(
        ProjectRelBase rel,
        BitSet columns,
        boolean ignoreNulls)
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
            } else if (projExpr instanceof RexCall && ignoreNulls) {
                // If the expression is a cast such that the types are the same
                // except for the nullability, then if we're ignoring nulls,
                // it doesn't matter whether the underlying column reference
                // is nullable.  Check that the types are the same by making a
                // nullable copy of both types and then comparing them.
                RexCall call = (RexCall) projExpr;
                if (call.getOperator() != SqlStdOperatorTable.castFunc) {
                    continue;
                }
                RexNode castOperand = call.getOperands()[0];
                if (!(castOperand instanceof RexInputRef)) {
                    continue;
                }
                RelDataTypeFactory typeFactory =
                    rel.getCluster().getTypeFactory();
                RelDataType castType =
                    typeFactory.createTypeWithNullability(
                        projExpr.getType(), true);
                RelDataType origType = typeFactory.createTypeWithNullability(
                    castOperand.getType(),
                    true);
                if (castType.equals(origType)) {
                    childColumns.set(((RexInputRef) castOperand).getIndex());
                }
            } else {
                // If the expression will not influence uniqueness of the
                // projection, then skip it.
                continue;
            }
        }

        // If no columns can affect uniqueness, then return unknown
        if (childColumns.cardinality() == 0) {
            return null;
        }

        return RelMetadataQuery.areColumnsUnique(
            rel.getChild(),
            childColumns,
            ignoreNulls);
    }

    public Boolean areColumnsUnique(
        JoinRelBase rel,
        BitSet columns, boolean
        ignoreNulls)
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
            RelMetadataQuery.areColumnsUnique(left, leftColumns, ignoreNulls);
        Boolean rightUnique =
            RelMetadataQuery.areColumnsUnique(right, rightColumns, ignoreNulls);
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
                RelMetadataQuery.areColumnsUnique(
                    right,
                    rightJoinCols,
                    ignoreNulls);
            if ((rightJoinColsUnique == null) || (leftUnique == null)) {
                return null;
            }
            return (rightJoinColsUnique && leftUnique);
        } else if (rightColumns.cardinality() > 0) {
            if (rel.getJoinType().generatesNullsOnRight()) {
                return false;
            }
            Boolean leftJoinColsUnique =
                RelMetadataQuery.areColumnsUnique(
                    left,
                    leftJoinCols,
                    ignoreNulls);
            if ((leftJoinColsUnique == null) || (rightUnique == null)) {
                return null;
            }
            return (leftJoinColsUnique && rightUnique);
        }

        assert (false);
        return null;
    }

    public Boolean areColumnsUnique(
        SemiJoinRel rel,
        BitSet columns,
        boolean ignoreNulls)
    {
        // only return the unique keys from the LHS since a semijoin only
        // returns the LHS
        return RelMetadataQuery.areColumnsUnique(
            rel.getLeft(),
            columns,
            ignoreNulls);
    }

    public Boolean areColumnsUnique(
        AggregateRelBase rel,
        BitSet columns,
        boolean ignoreNulls)
    {
        // group by keys form a unique key
        if (rel.getGroupCount() > 0) {
            BitSet groupKey = new BitSet();
            for (int i = 0; i < rel.getGroupCount(); i++) {
                groupKey.set(i);
            }
            return RelOptUtil.contains(columns, groupKey);
        } else {
            // interpret an empty set as asking whether the aggregation is full
            // table (in which case it returns at most one row);
            // TODO jvs 1-Sept-2008:  apply this convention consistently
            // to other relational expressions, as well as to
            // RelMetadataQuery.getUniqueKeys
            return columns.isEmpty();
        }
    }

    // Catch-all rule when none of the others apply.
    public Boolean areColumnsUnique(
        RelNode rel,
        BitSet columns,
        boolean ignoreNulls)
    {
        // no information available
        return null;
    }
}

// End RelMdColumnUniqueness.java
