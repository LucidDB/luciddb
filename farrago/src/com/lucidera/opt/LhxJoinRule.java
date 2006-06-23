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
        super(new RelOptRuleOperand(
                  JoinRel.class,
                  null));
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
        RelDataTypeFactory typeFactory = joinRel.getCluster().getTypeFactory();
        
        RelNode leftRel = joinRel.getLeft();
        RelNode rightRel = joinRel.getRight();
        RexNode joinCondition = joinRel.getCondition();
        RexNode nonEquiCondition;
        
        // determine if we have a valid join condition
        List<Integer> leftKeys = new ArrayList<Integer>();
        List<Integer> rightKeys = new ArrayList<Integer>();
        List<Integer> leftKeysToCast = new ArrayList<Integer>();
        List<Integer> rightKeysToCast = new ArrayList<Integer>();
        List<RexNode> leftFunctionKeys = new ArrayList<RexNode>();
        List<RexNode> rightFunctionKeys = new ArrayList<RexNode>();
        
        nonEquiCondition = RelOptUtil.splitJoinCondition(
            leftRel, joinCondition, leftKeys, rightKeys, leftKeysToCast,
            rightKeysToCast, leftFunctionKeys, rightFunctionKeys);
        
        if (leftKeys.size() == 0 && leftKeysToCast.size() == 0  &&
            leftFunctionKeys.size() == 0 ||
            leftKeys.size() != rightKeys.size() ||
            leftKeysToCast.size() != rightKeysToCast.size() ||
            leftFunctionKeys.size() != rightFunctionKeys.size()) {
            return;
        }

        if (!joinRel.getVariablesStopped().isEmpty()) {
            return;
        }
        
        List<Integer> outputProj = new ArrayList<Integer>();
        List<String> newJoinOutputNames = new ArrayList<String>();
                
        boolean projectionRequired =
            leftKeysToCast.size() > 0 || leftFunctionKeys.size() > 0;
        
        if (projectionRequired) {
            // cast the inputs so that join keys have matching types
            // this is required as during runtime, tuple comparison needs the
            // inputs to have identical types.
            RelNode[] inputRels = new RelNode[] {leftRel, rightRel};
        
            projectInputs(rexBuilder, typeFactory, inputRels,
                leftKeys, rightKeys,
                leftKeysToCast, rightKeysToCast,
                leftFunctionKeys, rightFunctionKeys,
                outputProj);
        
            leftRel = inputRels[0];
            rightRel = inputRels[1];
        }
        
        newJoinOutputNames.addAll(RelOptUtil.getFieldNameList(leftRel.getRowType()));
        newJoinOutputNames.addAll(RelOptUtil.getFieldNameList(rightRel.getRowType()));
        
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
        
        // Need to project the new output(left+cast+right+cast) to the original
        // join output(left+right).
        // The projection needs to happen before additional filtering since
        // filtering condition references the original output ordinals.
        if (projectionRequired) {
            int newProjectOutputSize = outputProj.size();
            RexNode[] newProjectOutputFields = new RexNode[newProjectOutputSize];
            String[]  newProjectOutputNames = new String[newProjectOutputSize];
            
            RelDataTypeField[] joinOutputFields = rel.getRowType().getFields();
            
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
        RexBuilder rexBuilder,
        RelDataTypeFactory typeFactory,
        RelNode[] inputRels,
        List<Integer> leftKeys,
        List<Integer> rightKeys,
        List<Integer> leftKeysToCast,
        List<Integer> rightKeysToCast,
        List<RexNode> leftFunctionKeys,
        List<RexNode> rightFunctionKeys,
        List<Integer> outputProj)
    {
        RelNode leftRel = inputRels[0];
        RelNode rightRel = inputRels[1];
        
        int origLeftInputSize = leftRel.getRowType().getFieldCount();
        int origRightInputSize = rightRel.getRowType().getFieldCount();
        
        List<RexNode> newLeftFields = new ArrayList<RexNode>();
        List<String>  newLeftFieldNames = new ArrayList<String>();
        RexNode newLeftInputRef;

        List<RexNode> newRightFields = new ArrayList<RexNode>();
        List<String>  newRightFieldNames = new ArrayList<String>();
        RexNode newRightInputRef;
        
        for (int i = 0; i < origLeftInputSize; i ++) {
            newLeftFields.add(
                rexBuilder.makeInputRef(
                    leftRel.getRowType().getFields()[i].getType(), i));
            newLeftFieldNames.add(
                leftRel.getRowType().getFields()[i].getName());
            outputProj.add(i);
        }
        
        for (int i = 0; i < origRightInputSize; i ++) {
            newRightFields.add(
                rexBuilder.makeInputRef(
                    rightRel.getRowType().getFields()[i].getType(), i));
            newRightFieldNames.add(
                rightRel.getRowType().getFields()[i].getName());
            // right input starts after the additional casted fields
            // and is set up after the casted fields are added.
        }
        
        int newLeftKeyIndex = 0;
        int newRightKeyIndex = 0;

        // first add all the implicit key type castings
        int numLeftKeysToCast =  leftKeysToCast.size();
            
        for (int i = 0; i < numLeftKeysToCast; i ++) {

            int leftFieldIndex = leftKeysToCast.get(i);
            int rightFieldIndex = rightKeysToCast.get(i);
                
            RelDataTypeField leftField
                = leftRel.getRowType().getFields()[leftFieldIndex];
            RelDataTypeField rightField
                = rightRel.getRowType().getFields()[rightFieldIndex];
                
            RelDataType leftFieldType = leftField.getType();
            RelDataType rightFieldType = rightField.getType();
                
            String leftFieldName = leftField.getName();
            String rightFieldName = rightField.getName();
                
            // now decide how to cast
            RelDataType targetFieldType = 
                typeFactory.leastRestrictive(
                    new RelDataType[] {leftFieldType, rightFieldType});

            if (targetFieldType == leftFieldType) {
                // just add the original field input index
                leftKeys.add(leftFieldIndex);
            } else {
                // needs to cast first and then add to the key projection
                newLeftInputRef =
                    rexBuilder.makeInputRef(leftFieldType, leftFieldIndex);                                
                newLeftFields.add(
                    rexBuilder.makeCast(targetFieldType,  newLeftInputRef));
                newLeftFieldNames.add(leftFieldName);
                // change the corresponding join key pairs
                // by referencing the newly created casted key
                leftKeys.add(origLeftInputSize + newLeftKeyIndex);
                newLeftKeyIndex ++;
            }
                
            if (targetFieldType == rightFieldType) {
                // just add the original field input index
                rightKeys.add(rightFieldIndex);
            } else {
                // needs to cast first and then add to the key projection
                newRightInputRef =
                    rexBuilder.makeInputRef(rightFieldType, rightFieldIndex);                                
                newRightFields.add(
                    rexBuilder.makeCast(targetFieldType,  newRightInputRef));
                newRightFieldNames.add(rightFieldName);
                // change the corresponding join key pairs
                // by referencing the newly created casted key
                rightKeys.add(origRightInputSize + newRightKeyIndex);
                newRightKeyIndex ++;
            }                
        }
        
        // then add the join predicates on functions that references
        // a single input field.
        int numLeftFunctionKeys =  leftFunctionKeys.size();

        for (int i = 0; i < numLeftFunctionKeys; i ++) {
            RexNode leftFunctionKey = leftFunctionKeys.get(i);
            RexNode rightFunctionKey = rightFunctionKeys.get(i);
            
            if (leftFunctionKey instanceof RexInputRef) {
                leftKeys.add(((RexInputRef)leftFunctionKey).getIndex());
            } else {
                newLeftFields.add(leftFunctionKey);
                newLeftFieldNames.add(leftFunctionKey.toString());
                // change the corresponding join key pairs
                // by referencing the newly created casted key                
                leftKeys.add(origLeftInputSize + newLeftKeyIndex);
                newLeftKeyIndex ++;
            }
            
            if (rightFunctionKey instanceof RexInputRef) {
                rightKeys.add(((RexInputRef)rightFunctionKey).getIndex());
            } else {
                newRightFields.add(rightFunctionKey);
                newRightFieldNames.add(rightFunctionKey.toString());
                // change the corresponding join key pairs
                // by referencing the newly created casted key                
                rightKeys.add(origRightInputSize + newRightKeyIndex);
                newRightKeyIndex ++;
            }
        }
        
        int newLeftInputSize = newLeftFields.size();
            
        for (int i = 0; i < origRightInputSize; i ++) {
            outputProj.add(newLeftInputSize + i);
        }

        // Now let's create project rels on the inputs.
        if (newLeftKeyIndex > 0) {
            // added project on top of the left input
            leftRel =
                CalcRel.createProject(leftRel, newLeftFields, newLeftFieldNames);
        }
            
        if (newRightKeyIndex > 0) {
            // added project on top of the right input
            rightRel =
                CalcRel.createProject(rightRel, newRightFields, newRightFieldNames);
        }
        
        inputRels[0] = leftRel;
        inputRels[1] = rightRel;
    }
}
// End LhxJoinRule.java
