/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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

import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.validate.*;
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

    protected final List<RelDataTypeField> systemFieldList;
    protected final List<AggregateCall> aggCalls;
    protected final BitSet groupSet;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an AggregateRelBase.
     *
     * @param cluster Cluster
     * @param traits Traits
     * @param child Child
     * @param systemFieldList List of system fields
     * @param groupSet Bitset of grouping fields
     * @param aggCalls Collection of calls to aggregate functions
     */
    protected AggregateRelBase(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        List<RelDataTypeField> systemFieldList,
        BitSet groupSet,
        List<AggregateCall> aggCalls)
    {
        super(cluster, traits, child);
        Util.pre(aggCalls != null, "aggCalls != null");
        this.systemFieldList = systemFieldList;
        this.aggCalls = aggCalls;
        this.groupSet = groupSet;
        assert groupSet != null;
        assert systemFieldList != null;
        assert groupSet.isEmpty() == (groupSet.cardinality() == 0)
            : "See http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6222207";
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelNode
    public boolean isDistinct()
    {
        // REVIEW jvs 5-Apr-2008:  Shouldn't this just return true always?
        // How can the result of aggregation have any duplicates?

        return (aggCalls.size() == 0)
            && (getGroupCount()
                == getChild().getRowType().getFieldCount()
                   - getSystemFieldList().size());
    }

    /**
     * Returns a list of calls to aggregate functions.
     *
     * @return list of calls to aggregate functions
     */
    public List<AggregateCall> getAggCallList()
    {
        return aggCalls;
    }

    /**
     * Returns the number of grouping fields. Unless there are system fields,
     * these grouping fields are the leading fields in both the input and output
     * records.
     *
     * <p>NOTE: The {@link #getGroupSet()} data structure allows for the
     * grouping fields to not be on the leading edge. New code should, if
     * possible, assume that grouping fields are in arbitrary positions in the
     * input relational expression.
     *
     * @return number of grouping fields
     */
    public int getGroupCount()
    {
        return groupSet.cardinality();
    }

    /**
     * Returns a bitmap of the grouping fields.
     *
     * @return bitset of ordinals of grouping fields
     */
    public BitSet getGroupSet()
    {
        return groupSet;
    }

    public void explain(RelOptPlanWriter pw)
    {
        List<String> names = new ArrayList<String>();
        List<Object> values = new ArrayList<Object>();
        populateExplainAttributes(names, values);
        pw.explain(this, names, values);
    }

    /**
     * Populates attributes for EXPLAIN.
     *
     * @param names Names of attributes
     * @param values Values of attributes
     */
    protected void populateExplainAttributes(
        List<String> names,
        List<Object> values)
    {
        names.add("child");
        if (!getSystemFieldList().isEmpty()) {
            names.add("sysFields");
            values.add(Pair.projectKeys(getSystemFieldList()));
        }
        names.add("group");
        values.add(groupSet);
        int i = -1;
        for (AggregateCall aggCall : aggCalls) {
            ++i;
            final String name;
            if (aggCall.getName() != null) {
                name = aggCall.getName();
            } else {
                name = "agg#" + i;
            }
            names.add(name);
            values.add(aggCall);
        }
    }

    // implement RelNode
    public double getRows()
    {
        // Assume that each sort column has 50% of the value count.
        // Therefore one sort column has .5 * rowCount,
        // 2 sort columns give .75 * rowCount.
        // Zero sort columns yields 1 row (or 0 if the input is empty).
        final int groupCount = groupSet.cardinality();
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
        // REVIEW jvs 24-Aug-2008:  This is bogus, but no more bogus
        // than what's currently in JoinRelBase.
        double rowCount = RelMetadataQuery.getRowCount(this);
        return planner.makeCost(rowCount, 0, 0);
    }

    protected RelDataType deriveRowType()
    {
        return getCluster().getTypeFactory().createStructType(
            CompositeList.of(
                // system fields (may be empty)
                systemFieldList,

                // fields derived from grouping columns
                new AbstractList<RelDataTypeField>()
                {
                    public int size()
                    {
                        return groupSet.cardinality();
                    }

                    public RelDataTypeField get(int index)
                    {
                        return getChild().getRowType().getFieldList().get(
                            Util.toList(groupSet).get(index));
                    }
                },

                // fields derived from aggregate calls
                new AbstractList<RelDataTypeField>()
                {
                    public int size()
                    {
                        return aggCalls.size();
                    }

                    public RelDataTypeField get(int index)
                    {
                        final AggregateCall aggCall = aggCalls.get(index);
                        String name;
                        if (aggCall.getName() != null) {
                            name = aggCall.getName();
                        } else {
                            name = "$f" + index;
                        }
                        assert typeMatchesInferred(aggCall, true);
                        return new RelDataTypeFieldImpl(
                            name, index, aggCall.getType());
                    }
                }
            ));
    }

    /**
     * Returns whether the inferred type of an {@link AggregateCall} matches the
     * type it was given when it was created.
     *
     * @param aggCall Aggregate call
     * @param fail Whether to fail if the types do not match
     *
     * @return Whether the inferred and declared types match
     */
    private boolean typeMatchesInferred(
        final AggregateCall aggCall,
        final boolean fail)
    {
        SqlAggFunction aggFunction = (SqlAggFunction) aggCall.getAggregation();
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
        for (AggregateCall call : aggCalls) {
            if (call.isDistinct()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<RelDataTypeField> getSystemFieldList()
    {
        return systemFieldList;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Implementation of the {@link SqlOperatorBinding} interface for an {@link
     * AggregateCall aggregate call} applied to a set of operands in the context
     * of a {@link AggregateRel}.
     */
    public static class AggCallBinding
        extends SqlOperatorBinding
    {
        private final AggregateRelBase aggregateRel;
        private final List<Integer> operands;

        /**
         * Creates an AggCallBinding
         *
         * @param typeFactory Type factory
         * @param aggFunction Aggregation function
         * @param aggregateRel Relational expression which is context
         * @param operands Operand ordinals
         */
        AggCallBinding(
            RelDataTypeFactory typeFactory,
            SqlAggFunction aggFunction,
            AggregateRelBase aggregateRel,
            List<Integer> operands)
        {
            super(typeFactory, aggFunction);
            this.aggregateRel = aggregateRel;
            this.operands = operands;
        }

        public int getOperandCount()
        {
            return operands.size();
        }

        public RelDataType getOperandType(int ordinal)
        {
            final RelDataType childType = aggregateRel.getChild().getRowType();
            int operand = operands.get(ordinal);
            return childType.getFields()[operand].getType();
        }

        public EigenbaseException newError(
            SqlValidatorException e)
        {
            return SqlUtil.newContextException(SqlParserPos.ZERO, e);
        }
    }
}

// End AggregateRelBase.java
