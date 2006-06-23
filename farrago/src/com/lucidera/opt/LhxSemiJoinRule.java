/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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
package com.lucidera.opt;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import net.sf.farrago.query.FennelRel;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.RexNode;

/**
 * LcsSemiJoinRule implements the rule for converting a join(which evaluates
 * a semi join) expression into the a hash semi join.
 *       
 * @author Rushan Chen
 * @version $Id$
 */
public class LhxSemiJoinRule extends RelOptRule
{   
    //      ~ Constructors ----------------------------------------------------------

    public LhxSemiJoinRule()
    {
        super(
            new RelOptRuleOperand(
                ProjectRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(
                        JoinRel.class,
                        new RelOptRuleOperand [] {
                        	new RelOptRuleOperand(RelNode.class, null),                             
                            new RelOptRuleOperand(
                            	AggregateRel.class, null)})}));
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        ProjectRel    projRel = (ProjectRel) call.rels[0];
        JoinRel       joinRel = (JoinRel) call.rels[1];
        RelNode       leftRel = call.rels[2];
        AggregateRel  aggRel  = (AggregateRel)call.rels[3];
        RelNode      rightRel = aggRel.getChild();
        
        RexNode joinCondition = joinRel.getCondition();
        RexNode residualCondition;
        
        // determine if we have a valid join condition
        List<Integer> leftKeys = new ArrayList<Integer>();
        List<Integer> rightKeys = new ArrayList<Integer>();
        
        residualCondition = RelOptUtil.splitJoinCondition(
            leftRel, aggRel, joinCondition, leftKeys, rightKeys);
        
        if (leftKeys.size() == 0 || residualCondition != null) {
            // join key only references input fields directly
            return;
        }
        
        // First check if projecting only the left fields
        RexNode[] projExprs = projRel.getProjectExps();
        int leftFieldCount = leftRel.getRowType().getFieldCount();
        int aggFieldCount = aggRel.getRowType().getFieldCount();
        
        BitSet projRefs = new BitSet(leftFieldCount + aggFieldCount);
        
        RelOptUtil.InputFinder inputFinder = new RelOptUtil.InputFinder(projRefs);
        
        inputFinder.apply(projExprs, null);
        
        for (int bit = projRefs.nextSetBit(0); bit >= 0;
             bit = projRefs.nextSetBit(bit + 1))
        {
        	if (bit >= leftFieldCount) {
        		return;
        	}
        }
        
        if (aggRel.getAggCalls().length != 0) {
            // not guaranteed to be distinct
            return;
        }

        // then check if aggregate(distinct) keys are the join keys
        int numGroupByKeys = aggRel.getGroupCount();
        
        // only join on the group by keys
        int numRightKeys = rightKeys.size();
        
        for (int i = 0; i < numRightKeys; i ++) {
            if (rightKeys.get(i) >= numGroupByKeys) {
                return;
            }
        }
       
        // now we can replace the original join(A, distinct(B)) with semiJoin(A, B)
        RelNode fennelLeft =
            mergeTraitsAndConvert(
                joinRel.getTraits(), FennelRel.FENNEL_EXEC_CONVENTION,
                leftRel);
        
        if (fennelLeft == null) {
            return;
        }

        RelNode fennelRight =
            mergeTraitsAndConvert(
                joinRel.getTraits(), FennelRel.FENNEL_EXEC_CONVENTION,
                rightRel);
        
        if (fennelRight == null) {
            return;
        }

        Double numBuildRows = RelMetadataQuery.getRowCount(fennelRight);
        if (numBuildRows == null) {
            numBuildRows = 10000.0;
        }

        // Derive cardinality of RHS join keys.
        Double cndBuildKey;
        BitSet joinKeyMap = new BitSet();
        
        for (int i = 0; i < rightKeys.size(); i ++) {
            joinKeyMap.set(rightKeys.get(i));
        }
        
        cndBuildKey = RelMetadataQuery.getPopulationSize(
            fennelRight, joinKeyMap);
        
        if ((cndBuildKey == null) || (cndBuildKey > numBuildRows)) {
            cndBuildKey = numBuildRows;
        }
        
        boolean isSetop = false;
        List<String> newJoinOutputNames =
        	RelOptUtil.getFieldNameList(leftRel.getRowType());
        
        RelNode rel =
            new LhxJoinRel(
                joinRel.getCluster(),
                fennelLeft,
                fennelRight,
                LhxJoinRelType.LEFTSEMI,
                isSetop,
                leftKeys,
                rightKeys,
                newJoinOutputNames,
                numBuildRows.intValue(),
                cndBuildKey.intValue());
        
        // keep the original project
        rel =
            CalcRel.createProject(rel, projExprs,
            		              RelOptUtil.getFieldNames(projRel.getRowType()));

        call.transformTo(rel);
    }
}

// End LhxSemiJoinRule.java
