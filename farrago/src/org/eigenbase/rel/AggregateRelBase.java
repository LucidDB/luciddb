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
package org.eigenbase.rel;

import java.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;


/**
 * <code>AggregateRelBase</code> is an abstract base class for implementations
 * of {@link AggregateRel}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class AggregateRelBase
    extends SingleRel
{
    //~ Instance fields --------------------------------------------------------

    protected Call [] aggCalls;
    protected int groupCount;

    //~ Constructors -----------------------------------------------------------

    protected AggregateRelBase(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        int groupCount,
        Call [] aggCalls)
    {
        super(cluster, traits, child);
        Util.pre(aggCalls != null, "aggCalls != null");
        this.groupCount = groupCount;
        this.aggCalls = aggCalls;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelNode
    public boolean isDistinct()
    {
        return (aggCalls.length == 0)
            && (groupCount == getChild().getRowType().getFieldList().size());
    }

    public Call [] getAggCalls()
    {
        return aggCalls;
    }

    public int getGroupCount()
    {
        return groupCount;
    }

    public void explain(RelOptPlanWriter pw)
    {
        List<String> names = new ArrayList<String>();
        List<Object> values = new ArrayList<Object>();
        names.add("child");
        names.add("groupCount");
        values.add(groupCount);
        for (int i = 0; i < aggCalls.length; i++) {
            names.add("agg#" + i);
            values.add(aggCalls[i]);
        }
        pw.explain(
            this,
            names.toArray(new String[names.size()]),
            values.toArray(new Object[values.size()]));
    }

    // implement RelNode
    public double getRows()
    {
        // Assume that each sort column has 50% of the value count.
        // Therefore one sort column has .5 * rowCount,
        // 2 sort columns give .75 * rowCount.
        // Zero sort columns yields 1 row (or 0 if the input is empty).
        if (groupCount == 0) {
            return 1;
        } else {
            double rowCount = super.getRows();
            rowCount *= (1.0 - Math.pow(.5, groupCount));
            return rowCount;
        }
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        return planner.makeTinyCost();
    }

    protected RelDataType deriveRowType()
    {
        final RelDataType childType = getChild().getRowType();
        final RelDataType [] types =
            new RelDataType[groupCount + aggCalls.length];
        for (int i = 0; i < groupCount; i++) {
            types[i] = childType.getFields()[i].getType();
        }
        for (int i = 0; i < aggCalls.length; i++) {
            final Call aggCall = aggCalls[i];
            assert typeMatchesInferred(aggCall, true);
            types[groupCount + i] = aggCall.getType();
        }
        return getCluster().getTypeFactory().createStructType(
            new RelDataTypeFactory.FieldInfo() {
                public int getFieldCount()
                {
                    return groupCount + aggCalls.length;
                }

                public String getFieldName(int index)
                {
                    if (index < groupCount) {
                        return childType.getFields()[index].getName();
                    } else {
                        return "$f" + index;
                    }
                }

                public RelDataType getFieldType(int index)
                {
                    return types[index];
                }
            });
    }

    /**
     * Returns whether the inferred type of a {@link Call} matches the type it
     * was given when it was created.
     *
     * @param aggCall Aggregate call
     * @param fail Whether to fail if the types do not match
     *
     * @return Whether the inferred and declared types match
     */
    private boolean typeMatchesInferred(final Call aggCall,
        final boolean fail)
    {
        SqlAggFunction aggFunction = (SqlAggFunction) aggCall.aggregation;
        AggCallBinding callBinding = aggCall.createBinding(this);
        RelDataType type = aggFunction.inferReturnType(callBinding);
        RelDataType expectedType = aggCall.getType();
        return RelOptUtil.eq(
            "aggCall type",
            expectedType,
            "inferred type",
            type,
            fail);
    }

    /**
     * Returns whether any of the aggregates are DISTINCT.
     */
    public boolean containsDistinctCall()
    {
        for (Call call : aggCalls) {
            if (call.isDistinct()) {
                return true;
            }
        }
        return false;
    }

    //~ Inner Classes ----------------------------------------------------------

    public static class Call
    {
        private final Aggregation aggregation;

        // TODO jvs 24-Apr-2006:  make this array and its contents
        // immutable
        public final int [] args;
        private final boolean distinct;
        private final RelDataType type;

        public Call(
            Aggregation aggregation,
            boolean distinct,
            int [] args,
            RelDataType type)
        {
            this.type = type;
            assert aggregation != null;
            assert args != null;
            assert type != null;
            this.aggregation = aggregation;
            this.args = args;
            this.distinct = distinct;
        }

        public boolean isDistinct()
        {
            return distinct;
        }

        public Aggregation getAggregation()
        {
            return aggregation;
        }

        public int [] getArgs()
        {
            return args;
        }

        public RelDataType getType()
        {
            return type;
        }

        public String toString()
        {
            StringBuilder buf = new StringBuilder(aggregation.getName());
            buf.append("(");
            if (distinct) {
                buf.append((args.length == 0) ? "DISTINCT" : "DISTINCT ");
            }
            for (int i = 0; i < args.length; i++) {
                if (i > 0) {
                    buf.append(", ");
                }
                buf.append(args[i]);
            }
            buf.append(")");
            return buf.toString();
        }

        // override Object
        public boolean equals(Object o)
        {
            if (!(o instanceof Call)) {
                return false;
            }
            Call other = (Call) o;
            return aggregation.equals(other.aggregation)
                && (distinct == other.distinct)
                && Arrays.equals(args, other.args);
        }

        /**
         * Creates a binding of this call in the context of an {@link
         * AggregateRel}, which can then be used to infer the return type.
         */
        public AggCallBinding createBinding(AggregateRelBase aggregateRelBase)
        {
            return new AggCallBinding(
                aggregateRelBase.getCluster().getTypeFactory(),
                (SqlAggFunction) aggregation,
                aggregateRelBase,
                args);
        }
    }

    /**
     * Implementation of the {@link SqlOperatorBinding} interface for an {@link
     * Call aggregate call} applied to a set of operands in the context of a
     * {@link AggregateRel}.
     */
    public static class AggCallBinding
        extends SqlOperatorBinding
    {
        private final AggregateRelBase aggregateRel;
        private final int [] operands;

        AggCallBinding(
            RelDataTypeFactory typeFactory,
            SqlAggFunction aggFunction,
            AggregateRelBase aggregateRel,
            int [] operands)
        {
            super(typeFactory, aggFunction);
            this.aggregateRel = aggregateRel;
            this.operands = operands;
        }

        public int getOperandCount()
        {
            return operands.length;
        }

        public RelDataType getOperandType(int ordinal)
        {
            final RelDataType childType = aggregateRel.getChild().getRowType();
            int operand = operands[ordinal];
            return childType.getFields()[operand].getType();
        }
    }
}

// End AggregateRelBase.java
