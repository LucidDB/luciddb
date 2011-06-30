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

import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.type.*;

/**
 * <code>FilterRelBase</code> is an abstract base class for implementations of
 * {@link FilterRel}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FilterRelBase
    extends SingleRel
{
    //~ Instance fields --------------------------------------------------------

    private final RexNode condition;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a filter.
     *
     * @param cluster {@link RelOptCluster}  this relational expression belongs
     * to
     * @param traits the traits of this rel
     * @param child input relational expression
     * @param condition boolean expression which determines whether a row is
     * allowed to pass
     */
    protected FilterRelBase(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        RexNode condition)
    {
        super(cluster, traits, child);
        this.condition = condition;
        assert condition != null;
        assert !RexOver.containsOver(condition);
        assert isValid(true);
    }

    //~ Methods ----------------------------------------------------------------

    public RexNode [] getChildExps()
    {
        return new RexNode[] { condition };
    }

    /**
     * Returns the condition.
     *
     * @return Condition
     */
    public RexNode getCondition()
    {
        return condition;
    }

    public boolean isValid(boolean fail)
    {
        if (!super.isValid(fail)) {
            return false;
        }
        // We cannot assert that getRowType() == getChild().getRowType()
        // because if the child is a RelSubset its row type may change during a
        // merge (to accommodate differences in field names) after the filter's
        // rowtype has been determined.
        if (!SqlTypeUtil.equalSansNames(getRowType(), getChild().getRowType()))
        {
            assert !fail
                : "row type mismatch:\nthis: " + getRowType()
                  + "\nchild: " + getChild().getRowType();
            return false;
        }
        if (condition.getType().getSqlTypeName() != SqlTypeName.BOOLEAN) {
            assert !fail
                : "condition must be boolean: " + condition.getType();
            return false;
        }
        if (RexOver.containsOver(condition)) {
            assert !fail
                : "condition must not contain windowed aggs: " + condition;
            return false;
        }
        RexChecker checker =
            new RexChecker(
                getRowType(),
                fail);
        condition.accept(checker);
        if (checker.getFailureCount() > 0) {
            assert !fail
                : checker.getFailureCount()
                  + " failures in condition " + condition;
            return false;
        }
        return true;
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        double dRows = RelMetadataQuery.getRowCount(this);
        double dCpu = RelMetadataQuery.getRowCount(getChild());
        double dIo = 0;
        return planner.makeCost(dRows, dCpu, dIo);
    }

    // override RelNode
    public double getRows()
    {
        return estimateFilteredRows(
            getChild(),
            condition);
    }

    /**
     * Estimates the number of rows returned by an expression after a filtering
     * program has been applied.
     *
     * @param child Relational expression
     * @param program Program including filter
     * @return Estimate of number of rows returned by filtered expression
     */
    public static double estimateFilteredRows(RelNode child, RexProgram program)
    {
        // convert the program's RexLocalRef condition to an expanded RexNode
        RexLocalRef programCondition = program.getCondition();
        RexNode condition;
        if (programCondition == null) {
            condition = null;
        } else {
            condition = program.expandLocalRef(programCondition);
        }
        return estimateFilteredRows(
            child,
            condition);
    }

    /**
     * Estimates the number of rows returned by an expression after a filter
     * condition has been applied.
     *
     * @param child Relational expression
     * @param condition Filter expression
     * @return Estimate of number of rows returned by filtered expression
     */
    public static double estimateFilteredRows(RelNode child, RexNode condition)
    {
        return RelMetadataQuery.getRowCount(child)
            * RelMetadataQuery.getSelectivity(child, condition);
    }

    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String[] { "child", "condition" });
    }
}

// End FilterRelBase.java
