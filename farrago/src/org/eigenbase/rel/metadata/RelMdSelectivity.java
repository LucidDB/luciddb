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
import org.eigenbase.sql.fun.*;


/**
 * RelMdSelectivity supplies a default implementation of {@link
 * RelMetadataQuery#getSelectivity} for the standard logical algebra.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class RelMdSelectivity
    extends ReflectiveRelMetadataProvider
{
    //~ Constructors -----------------------------------------------------------

    public RelMdSelectivity()
    {
        // Tell superclass reflection about parameter types expected
        // for various metadata queries.

        // This corresponds to getSelectivity(rel, RexNode predicate);
        // note that we don't specify the rel type because we always overload
        // on that.
        mapParameterTypes(
            "getSelectivity",
            Collections.singletonList((Class) RexNode.class));
    }

    //~ Methods ----------------------------------------------------------------

    public Double getSelectivity(UnionRelBase rel, RexNode predicate)
    {
        if ((rel.getInputs().length == 0) || (predicate == null)) {
            return 1.0;
        }

        double sumRows = 0.0;
        double sumSelectedRows = 0.0;
        int [] adjustments = new int[rel.getRowType().getFieldCount()];
        RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
        for (RelNode input : rel.getInputs()) {
            Double nRows = RelMetadataQuery.getRowCount(input);
            if (nRows == null) {
                return null;
            }

            // convert the predicate to reference the types of the union child
            RexNode modifiedPred =
                predicate.accept(
                    new RelOptUtil.RexInputConverter(
                        rexBuilder,
                        null,
                        input.getRowType().getFields(),
                        adjustments));
            double sel = RelMetadataQuery.getSelectivity(input, modifiedPred);

            sumRows += nRows;
            sumSelectedRows += nRows * sel;
        }

        if (sumRows < 1.0) {
            sumRows = 1.0;
        }
        return sumSelectedRows / sumRows;
    }

    public Double getSelectivity(SortRel rel, RexNode predicate)
    {
        return RelMetadataQuery.getSelectivity(
            rel.getChild(),
            predicate);
    }

    public Double getSelectivity(FilterRelBase rel, RexNode predicate)
    {
        // Take the difference between the predicate passed in and the
        // predicate in the filter's condition, so we don't apply the
        // selectivity of the filter twice.  If no predicate is passed in,
        // use the filter's condition.
        if (predicate != null) {
            return RelMetadataQuery.getSelectivity(
                rel.getChild(),
                RelMdUtil.minusPreds(
                    rel.getCluster().getRexBuilder(),
                    predicate,
                    rel.getCondition()));
        } else {
            return RelMetadataQuery.getSelectivity(
                rel.getChild(),
                rel.getCondition());
        }
    }

    public Double getSelectivity(SemiJoinRel rel, RexNode predicate)
    {
        // create a RexNode representing the selectivity of the
        // semijoin filter and pass it to getSelectivity
        RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
        RexNode newPred = RelMdUtil.makeSemiJoinSelectivityRexNode(rel);
        if (predicate != null) {
            newPred =
                rexBuilder.makeCall(
                    SqlStdOperatorTable.andOperator,
                    newPred,
                    predicate);
        }

        return RelMetadataQuery.getSelectivity(
            rel.getLeft(),
            newPred);
    }

    public Double getSelectivity(AggregateRelBase rel, RexNode predicate)
    {
        List<RexNode> notPushable = new ArrayList<RexNode>();
        List<RexNode> pushable = new ArrayList<RexNode>();
        RelOptUtil.splitFilters(
            rel.getGroupCount(),
            predicate,
            pushable,
            notPushable);
        RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
        RexNode childPred = RexUtil.andRexNodeList(rexBuilder, pushable);

        Double selectivity =
            RelMetadataQuery.getSelectivity(
                rel.getChild(),
                childPred);
        if (selectivity == null) {
            return null;
        } else {
            RexNode pred = RexUtil.andRexNodeList(rexBuilder, notPushable);
            return selectivity * RelMdUtil.guessSelectivity(pred);
        }
    }

    public Double getSelectivity(ProjectRelBase rel, RexNode predicate)
    {
        List<RexNode> notPushable = new ArrayList<RexNode>();
        List<RexNode> pushable = new ArrayList<RexNode>();
        RelOptUtil.splitFilters(
            rel.getRowType().getFieldCount(),
            predicate,
            pushable,
            notPushable);
        RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
        RexNode childPred = RexUtil.andRexNodeList(rexBuilder, pushable);

        RexNode modifiedPred;
        if (childPred == null) {
            modifiedPred = null;
        } else {
            modifiedPred = RelOptUtil.pushFilterPastProject(childPred, rel);
        }
        Double selectivity =
            RelMetadataQuery.getSelectivity(
                rel.getChild(),
                modifiedPred);
        if (selectivity == null) {
            return null;
        } else {
            RexNode pred = RexUtil.andRexNodeList(rexBuilder, notPushable);
            return selectivity * RelMdUtil.guessSelectivity(pred);
        }
    }

    // Catch-all rule when none of the others apply.
    public Double getSelectivity(RelNode rel, RexNode predicate)
    {
        return RelMdUtil.guessSelectivity(predicate);
    }
}

// End RelMdSelectivity.java
