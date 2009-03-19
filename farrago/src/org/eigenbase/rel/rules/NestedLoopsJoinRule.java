/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2008-2008 The Eigenbase Project
// Copyright (C) 2008-2008 SQLstream, Inc.
// Copyright (C) 2008-2008 LucidEra, Inc.
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
package org.eigenbase.rel.rules;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.util.*;


/**
 * Rule which converts a {@link JoinRel} into a {@link CorrelatorRel}, which can
 * then be implemented using nested loops.
 *
 * <p>For example,
 *
 * <blockquote><code>select * from emp join dept on emp.deptno =
 * dept.deptno</code></blockquote>
 *
 * becomes a CorrelatorRel which restarts TableAccessRel("DEPT") for each row
 * read from TableAccessRel("EMP").</p>
 *
 * <p>This rule is not applicable if for certain types of outer join. For
 * example,
 *
 * <blockquote><code>select * from emp right join dept on emp.deptno =
 * dept.deptno</code></blockquote>
 *
 * would require emitting a NULL emp row if a certain department contained no
 * employees, and CorrelatorRel cannot do that.</p>
 *
 * @author jhyde
 * @version $Id$
 */
public class NestedLoopsJoinRule
    extends RelOptRule
{
    //~ Static fields/initializers ---------------------------------------------

    public static final NestedLoopsJoinRule INSTANCE =
        new NestedLoopsJoinRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Private constructor; use singleton {@link #INSTANCE}.
     */
    private NestedLoopsJoinRule()
    {
        super(
            new RelOptRuleOperand(
                JoinRel.class,
                ANY));
    }

    //~ Methods ----------------------------------------------------------------

    public boolean matches(RelOptRuleCall call)
    {
        JoinRel join = (JoinRel) call.rels[0];
        switch (join.getJoinType()) {
        case INNER:
        case LEFT:
            return true;
        case FULL:
        case RIGHT:
            return false;
        default:
            throw Util.unexpected(join.getJoinType());
        }
    }

    public void onMatch(RelOptRuleCall call)
    {
        assert matches(call);
        final JoinRel join = (JoinRel) call.rels[0];
        final List<Integer> leftKeys = new ArrayList<Integer>();
        final List<Integer> rightKeys = new ArrayList<Integer>();
        RelNode right = join.getRight();
        final RelNode left = join.getLeft();
        RexNode remainingCondition =
            RelOptUtil.splitJoinCondition(
                left,
                right,
                join.getCondition(),
                leftKeys,
                rightKeys);
        assert leftKeys.size() == rightKeys.size();
        final List<CorrelatorRel.Correlation> correlationList =
            new ArrayList<CorrelatorRel.Correlation>();
        if (leftKeys.size() > 0) {
            final RelOptCluster cluster = join.getCluster();
            final RexBuilder rexBuilder = cluster.getRexBuilder();
            int k = 0;
            RexNode condition = null;
            for (Integer leftKey : leftKeys) {
                Integer rightKey = rightKeys.get(k++);
                final String dyn_inIdStr = cluster.getQuery().createCorrel();
                final int dyn_inId = RelOptQuery.getCorrelOrdinal(dyn_inIdStr);

                // Create correlation to say 'each row, set variable #id
                // to the value of column #leftKey'.
                correlationList.add(
                    new CorrelatorRel.Correlation(
                        dyn_inId,
                        leftKey));
                condition =
                    RelOptUtil.andJoinFilters(
                        rexBuilder,
                        condition,
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.equalsOperator,
                            rexBuilder.makeInputRef(
                                right.getRowType().getFieldList().get(
                                    rightKey).getType(),
                                rightKey),
                            rexBuilder.makeCorrel(
                                left.getRowType().getFieldList().get(
                                    leftKey).getType(),
                                dyn_inIdStr)));
            }
            right =
                CalcRel.createFilter(
                    right,
                    condition);
        }
        RelNode newRel =
            new CorrelatorRel(
                join.getCluster(),
                left,
                right,
                remainingCondition,
                correlationList,
                join.getJoinType());
        call.transformTo(newRel);
    }
}

// End NestedLoopsJoinRule.java
