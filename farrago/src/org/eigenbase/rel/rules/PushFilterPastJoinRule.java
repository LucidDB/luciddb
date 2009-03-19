/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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


/**
 * PushFilterPastJoinRule implements the rule for pushing filters above and
 * within a join node into the join node and/or its children nodes.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class PushFilterPastJoinRule
    extends RelOptRule
{
    //~ Constructors -----------------------------------------------------------

    public PushFilterPastJoinRule()
    {
        super(
            new RelOptRuleOperand(
                FilterRel.class,
                new RelOptRuleOperand(JoinRel.class, ANY)));
    }

    public PushFilterPastJoinRule(RelOptRuleOperand rule, String id)
    {
        // This rule is fired for either of the following two patterns:
        //
        // RelOptRuleOperand(
        //     FilterRel.class,
        //     new RelOptRuleOperand(JoinRel.class, ANY))
        //
        // RelOptRuleOperand(JoinRel.class, null)
        //
        super(rule);
        description = "PushFilterRule: " + id;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        FilterRel filterRel;
        JoinRel joinRel;

        if (call.rels.length == 1) {
            filterRel = null;
            joinRel = (JoinRel) call.rels[0];
        } else {
            filterRel = (FilterRel) call.rels[0];
            joinRel = (JoinRel) call.rels[1];
        }

        List<RexNode> joinFilters = new ArrayList<RexNode>();
        RelOptUtil.decomposeConjunction(
            joinRel.getCondition(),
            joinFilters);

        if (filterRel == null) {
            // There is only the joinRel
            // make sure it does not match a cartesian product joinRel
            // (with "true" condition) otherwise this rule will be applied
            // again on the new cartesian product joinRel.
            boolean onlyTrueFilter = true;
            for (RexNode joinFilter : joinFilters) {
                if (!joinFilter.isAlwaysTrue()) {
                    onlyTrueFilter = false;
                    break;
                }
            }

            if (onlyTrueFilter) {
                return;
            }
        }

        List<RexNode> aboveFilters = new ArrayList<RexNode>();

        if (filterRel != null) {
            RelOptUtil.decomposeConjunction(
                filterRel.getCondition(),
                aboveFilters);
        }

        List<RexNode> leftFilters = new ArrayList<RexNode>();
        List<RexNode> rightFilters = new ArrayList<RexNode>();

        // TODO - add logic to derive additional filters.  E.g., from
        // (t1.a = 1 AND t2.a = 2) OR (t1.b = 3 AND t2.b = 4), you can
        // derive table filters:
        // (t1.a = 1 OR t1.b = 3)
        // (t2.a = 2 OR t2.b = 4)

        /*
         * Try to push down above filters. These are typically where clause
         * filters. They can be pushed down if they are not on the NULL
         * generating side.
         */
        boolean filterPushed = false;
        if (RelOptUtil.classifyFilters(
                joinRel,
                aboveFilters,
                (joinRel.getJoinType() == JoinRelType.INNER),
                !joinRel.getJoinType().generatesNullsOnLeft(),
                !joinRel.getJoinType().generatesNullsOnRight(),
                joinFilters,
                leftFilters,
                rightFilters))
        {
            filterPushed = true;
        }

        /*
         * Try to push down filters in ON clause. A ON clause filter can only be
         * pushed down if it does not affect the non-matching set, i.e. it is
         * not on the side which is preserved.
         */
        if (RelOptUtil.classifyFilters(
                joinRel,
                joinFilters,
                false,
                !joinRel.getJoinType().generatesNullsOnRight(),
                !joinRel.getJoinType().generatesNullsOnLeft(),
                joinFilters,
                leftFilters,
                rightFilters))
        {
            filterPushed = true;
        }

        if (!filterPushed) {
            return;
        }

        // create FilterRels on top of the children if any filters were
        // pushed to them
        RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
        RelNode leftRel =
            createFilterOnRel(
                rexBuilder,
                joinRel.getLeft(),
                leftFilters);
        RelNode rightRel =
            createFilterOnRel(
                rexBuilder,
                joinRel.getRight(),
                rightFilters);

        // create the new join node referencing the new children and
        // containing its new join filters (if there are any)
        RexNode joinFilter;

        if (joinFilters.size() == 0) {
            // if nothing actually got pushed and there is nothing leftover,
            // then this rule is a no-op
            if ((leftFilters.size() == 0) && (rightFilters.size() == 0)) {
                return;
            }
            joinFilter = rexBuilder.makeLiteral(true);
        } else {
            joinFilter = RexUtil.andRexNodeList(rexBuilder, joinFilters);
        }
        RelNode newJoinRel =
            new JoinRel(
                joinRel.getCluster(),
                leftRel,
                rightRel,
                joinFilter,
                joinRel.getJoinType(),
                Collections.<String>emptySet(),
                joinRel.isSemiJoinDone());

        // create a FilterRel on top of the join if needed
        RelNode newRel =
            createFilterOnRel(rexBuilder, newJoinRel, aboveFilters);

        call.transformTo(newRel);
    }

    /**
     * If the filter list passed in is non-empty, creates a FilterRel on top of
     * the existing RelNode; otherwise, just returns the RelNode
     *
     * @param rexBuilder rex builder
     * @param rel the RelNode that the filter will be put on top of
     * @param filters list of filters
     *
     * @return new RelNode or existing one if no filters
     */
    private RelNode createFilterOnRel(
        RexBuilder rexBuilder,
        RelNode rel,
        List<RexNode> filters)
    {
        RelNode newRel;

        if (filters.size() == 0) {
            newRel = rel;
        } else {
            RexNode andFilters = RexUtil.andRexNodeList(rexBuilder, filters);
            newRel = CalcRel.createFilter(rel, andFilters);
        }
        return newRel;
    }
}

// End PushFilterPastJoinRule.java
