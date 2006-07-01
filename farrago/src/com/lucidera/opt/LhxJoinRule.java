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

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;

import net.sf.farrago.query.*;

/**
 * LhxJoinRule implements the planner rule for converting a JoinRel
 * with join condition into a LhxJoinRel (hash join).
 *
 * @author Rushan Chen
 * @version $Id$
 */
public class LhxJoinRule extends RelOptRule
{
    //~ Constructors ----------------------------------------------------------

    public LhxJoinRule()
    {
        super(new RelOptRuleOperand(JoinRel.class, null));
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelRel.FENNEL_EXEC_CONVENTION;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        JoinRel joinRel = (JoinRel) call.rels[0];
        RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
        
        RelNode leftRel = joinRel.getLeft();
        RelNode rightRel = joinRel.getRight();
        RexNode nonEquiCondition = null;
        
        // determine if we have a valid join condition
        List<Integer> leftKeys = new ArrayList<Integer>();
        List<Integer> rightKeys = new ArrayList<Integer>();

        List<RexNode> leftJoinKeys = new ArrayList<RexNode>();
        List<RexNode> rightJoinKeys = new ArrayList<RexNode>();

        
        nonEquiCondition = RelOptUtil.splitJoinCondition(
            joinRel, leftJoinKeys, rightJoinKeys);
        
        if (nonEquiCondition != null
            && joinRel.getJoinType() != JoinRelType.INNER) {
            // this one can not be imlemented by hash join
            // nor can it be implemented by cartesian product
            return;
        }
    
        if (!joinRel.getVariablesStopped().isEmpty()) {
            return;
        }

        if (leftJoinKeys.size() == 0) {
            // should use cartesian product instead of hash join
            return;
        }
        
        List<Integer> outputProj = new ArrayList<Integer>();
        List<String> newJoinOutputNames = new ArrayList<String>();

        RelNode[] inputRels = new RelNode[] {leftRel, rightRel};
        
        projectInputs(inputRels, leftJoinKeys, rightJoinKeys,
            leftKeys, rightKeys, outputProj);
        
        leftRel = inputRels[0];
        rightRel = inputRels[1];
        
        newJoinOutputNames.addAll(
            RelOptUtil.getFieldNameList(leftRel.getRowType()));
        newJoinOutputNames.addAll(
            RelOptUtil.getFieldNameList(rightRel.getRowType()));
        
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
        
        // since rightJoinKeys can be more than simply inputrefs
        // assume the adinality of the key to be the cardinality of all
        // referenced fields.
        
        for (int i = 0; i < rightKeys.size(); i ++) {
            joinKeyMap.set(rightKeys.get(i));
        }
        
        cndBuildKey = RelMetadataQuery.getPopulationSize(
            fennelRight, joinKeyMap);
        
        if ((cndBuildKey == null) || (cndBuildKey > numBuildRows)) {
            cndBuildKey = numBuildRows;
        }
        
        boolean isSetop = false;
        
        RelNode rel =
            new LhxJoinRel(
                joinRel.getCluster(),
                fennelLeft,
                fennelRight,
                LhxJoinRelType.getLhxJoinType(joinRel.getJoinType()),
                isSetop,
                leftKeys,
                rightKeys,
                newJoinOutputNames,
                numBuildRows.intValue(),
                cndBuildKey.intValue());
        
        int newProjectOutputSize = outputProj.size();
        RelDataTypeField[] joinOutputFields = rel.getRowType().getFields();
        
        // Need to project the new output(left+key+right+key) to the original
        // join output(left+right).
        // The projection needs to happen before additional filtering since
        // filtering condition references the original output ordinals.
        if (newProjectOutputSize < joinOutputFields.length) {
            RexNode[] newProjectOutputFields =
                new RexNode[newProjectOutputSize];
            String[]  newProjectOutputNames =
                new String[newProjectOutputSize];
                        
            for (int i = 0; i < newProjectOutputSize; i ++) {
                int fieldIndex = outputProj.get(i);
            
                newProjectOutputFields[i] =
                    rexBuilder.makeInputRef(
                        joinOutputFields[fieldIndex].getType(),
                        fieldIndex);
                newProjectOutputNames[i] =
                    joinOutputFields[fieldIndex].getName();
            }

            // Now let's create a project rel on the output of the join.
            RelNode projectOutputRel =
                CalcRel.createProject(rel, newProjectOutputFields,
                    newProjectOutputNames);
            
            rel = projectOutputRel;
        }
                
        transformCall(call, rel, nonEquiCondition);
    }


    private void transformCall(
        RelOptRuleCall call,
        RelNode rel,
        RexNode extraFilter)
    {
        if (extraFilter != null) {
            rel =
                new FilterRel(rel.getCluster(), rel, extraFilter);
        }
        call.transformTo(rel);
    }
    
    private void projectInputs(
        RelNode[] inputRels,
        List<RexNode> leftJoinKeys,
        List<RexNode> rightJoinKeys,
        List<Integer> leftKeys,
        List<Integer> rightKeys,            
        List<Integer> outputProj)
    {
    	RelNode leftRel  = inputRels[0];
    	RelNode rightRel = inputRels[1];
    	RexBuilder rexBuilder = leftRel.getCluster().getRexBuilder();
    	
    	int origLeftInputSize = leftRel.getRowType().getFieldCount();
    	int origRightInputSize = rightRel.getRowType().getFieldCount();
    	
    	List<RexNode> newLeftFields = new ArrayList<RexNode>();
    	List<String>  newLeftFieldNames = new ArrayList<String>();

    	List<RexNode> newRightFields = new ArrayList<RexNode>();
    	List<String>  newRightFieldNames = new ArrayList<String>();
    	int leftKeyCount = leftJoinKeys.size();
    	int rightKeyCount = rightJoinKeys.size();
    	int i;
    	
    	for (i = 0; i < origLeftInputSize; i ++) {
    	    newLeftFields.add(rexBuilder.makeInputRef(
    	        leftRel.getRowType().getFields()[i].getType(), i));
    	    newLeftFieldNames.add(
    	        leftRel.getRowType().getFields()[i].getName());
    	    outputProj.add(i);
    	}
            
    	int newLeftKeyCount = 0;
    	for (i = 0; i < leftKeyCount; i ++) {
    	    RexNode leftKey = leftJoinKeys.get(i);
            	
    	    if (leftKey instanceof RexInputRef) {
    	        // already added to the projected left fields
    	        // only need to remember the index in the join key list
    	        leftKeys.add(((RexInputRef)leftKey).getIndex());
    	    } else {
    	        newLeftFields.add(leftKey);
    	        newLeftFieldNames.add(leftKey.toString());
    	        leftKeys.add(origLeftInputSize + newLeftKeyCount);
    	        newLeftKeyCount ++;
    	    }
    	}
            
    	int leftFieldCount = origLeftInputSize + newLeftKeyCount;
    	for (i = 0; i < origRightInputSize; i ++) {
    	    newRightFields.add(rexBuilder.makeInputRef(
    	        rightRel.getRowType().getFields()[i].getType(), i));
    	    newRightFieldNames.add(
    	        rightRel.getRowType().getFields()[i].getName());
    	    outputProj.add(i + leftFieldCount);
    	}
                
    	int newRightKeyCount = 0;
    	for (i = 0; i < rightKeyCount; i ++) {
    	    RexNode rightKey = rightJoinKeys.get(i);

            if (rightKey instanceof RexInputRef) {
    	        // already added to the projected left fields
    	        // only need to remember the index in the join key list
    	        rightKeys.add(((RexInputRef)rightKey).getIndex());
    	    } else {
    	        newRightFields.add(rightKey);
    	        newRightFieldNames.add(rightKey.toString());
    	        rightKeys.add(origRightInputSize + newRightKeyCount);
    	        newRightKeyCount ++;
    	    }
    	}

    	// added project if need to produce new keys than the origianl input fields
    	if (newLeftKeyCount > 0) {
    	    leftRel =
    	        CalcRel.createProject(leftRel, newLeftFields, newLeftFieldNames);
    	}
            
    	if (newRightKeyCount > 0) {
    	    rightRel =
    	        CalcRel.createProject(rightRel, newRightFields, newRightFieldNames);
    	}
            
    	inputRels[0] = leftRel;
    	inputRels[1] = rightRel;
    }
}
// End LhxJoinRule.java
