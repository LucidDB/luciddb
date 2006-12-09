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
package org.eigenbase.sql2rel;

import java.util.*;
import java.util.logging.Logger;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.trace.EigenbaseTrace;
import org.eigenbase.util.*;
import org.eigenbase.rel.metadata.*;


/**
 * RelDecorrelator replaces all correlated expressions(corExp) in a relational
 * expression (RelNode) tree with non-correlated expressions that are produced
 * from joining the RelNode that produces the corExp with the RelNode that
 * references it.
 *
 * @author Rushan Chen
 * @version $Id$
 */
public class RelDecorrelator
{
    // maps built during translation
    Map<RelNode, SortedSet<CorrelatorRel.Correlation> > mapRefRelToCorVar;
    SortedMap<CorrelatorRel.Correlation, CorrelatorRel> mapCorVarToCorRel;
    Map<RexFieldAccess, CorrelatorRel.Correlation> mapFieldAccessToCorVar;

    private final DecorrelateRelVisitor decorrelateVisitor;
    private final RemoveCorrelationVisitor removeCorrelationVisitor;
    
    private final RexBuilder rexBuilder;
    
    // The rel which is being visited
    private RelNode currentRel;
    private boolean projectPulledAboveLeftCorrelator;
    
    // maps built during decorrelation
    private Map<RelNode, RelNode> mapOldToNewRel;

    // map rel to all the newly created correlated variables in its output
    private Map<RelNode, SortedMap<CorrelatorRel.Correlation, Integer> > mapNewRelToMapCorVarToOutputPos;
    
    // another map to map old input positions to new input positions
    // this is from the view point of the parent rel of a new rel.
    private Map<RelNode, Map<Integer, Integer> > mapNewRelToMapOldToNewOutputPos;
    
    private static final Logger sqlToRelTracer =
        EigenbaseTrace.getSqlToRelTracer();
    
    public RelDecorrelator(
        RexBuilder rexBuilder,
        Map<RelNode, SortedSet<CorrelatorRel.Correlation> > mapRefRelToCorVar,
        SortedMap<CorrelatorRel.Correlation, CorrelatorRel> mapCorVarToCorRel,
        Map<RexFieldAccess, CorrelatorRel.Correlation> mapFieldAccessToCorVar)
    {
        this.rexBuilder = rexBuilder;
        this.mapRefRelToCorVar = mapRefRelToCorVar;
        this.mapCorVarToCorRel = mapCorVarToCorRel;
        this.mapFieldAccessToCorVar = mapFieldAccessToCorVar;

        decorrelateVisitor = new DecorrelateRelVisitor();
        removeCorrelationVisitor = new RemoveCorrelationVisitor();
    }
    
    public RelNode decorrelate(RelNode root) 
    {
        // Perform decorrelation.
        mapOldToNewRel =
            new HashMap<RelNode, RelNode>();
        
        mapNewRelToMapCorVarToOutputPos =
            new HashMap<RelNode, SortedMap<CorrelatorRel.Correlation, Integer> >();
        
        mapNewRelToMapOldToNewOutputPos =
            new HashMap<RelNode, Map<Integer, Integer> >();
        
        decorrelateVisitor.visit(root, 0, null);
        
        if (mapOldToNewRel.containsKey(root)) {
            // has been rewritten
            return mapOldToNewRel.get(root);
        } else {
            // not rewritten
            return root;
        }
    }

    public RelNode removeCorrelation(RelNode root) 
    {
        mapOldToNewRel =
            new HashMap<RelNode, RelNode>();

        removeCorrelationVisitor.visit(root, 0, null);
            
        if (mapOldToNewRel.containsKey(root)) {
            // has been rewritten
            return mapOldToNewRel.get(root);
        } else {
            // not rewritten
            return root;
        }
    }

    protected RexNode decorrelateExpr(RexNode exp)
    {
        DecorrelateRexShuttle shuttle = new DecorrelateRexShuttle();
        return exp.accept(shuttle);
    }

    protected RexNode removeCorrelationExpr(RexNode exp)
    {
        assert (currentRel != null);
        RelDataTypeFactory typeFactory = 
            currentRel.getCluster().getTypeFactory();
        RemoveCorrelationRexShuttle shuttle = 
            new RemoveCorrelationRexShuttle(typeFactory);
        return exp.accept(shuttle);
    }
    
    public void decorrelateRelGeneric(RelNode rel)
    {
        RelNode newRel = rel.clone();

        if (rel.getInputs().length > 0) {
            RelNode [] oldInputs = rel.getInputs();
            for (int i = 0; i < oldInputs.length; ++i) {
                RelNode newInputRel = mapOldToNewRel.get(oldInputs[i]);
                if (newInputRel == null || 
                    mapNewRelToMapCorVarToOutputPos.containsKey(newInputRel)) {
                    // if child is not rewritten, or if it produces correlated
                    // variables, terminate rewrite
                    return;
                } else {
                    newRel.replaceInput(i, newInputRel);
                }
            }            
        }
        
        // the output position should not change since there're no corVars
        // coming from below.
        Map<Integer, Integer> mapOldToNewOutputPos =
            new HashMap<Integer, Integer> ();
        for (int i = 0; i < rel.getRowType().getFieldCount(); i ++) {
            mapOldToNewOutputPos.put(i, i);
        }
        mapOldToNewRel.put(rel, newRel);
        mapNewRelToMapOldToNewOutputPos.put(newRel, mapOldToNewOutputPos);
    }
    
    public void removeCorrelationRelGeneric(RelNode rel)
    {
        RelNode newRel = rel.clone();

        if (rel.getInputs().length > 0) {
            RelNode [] oldInputs = rel.getInputs();
            for (int i = 0; i < oldInputs.length; ++i) {
                RelNode newInputRel = mapOldToNewRel.get(oldInputs[i]);
                if (newInputRel == null) {
                    return;
                } else {
                    newRel.replaceInput(i, newInputRel);                    
                }
            }
        }
        
        mapOldToNewRel.put(rel, newRel);
    }

    public void removeCorrelationRel(CorrelatorRel corRel)
    {
        RelNode leftInputRel = corRel.getLeft();
        RelDataTypeFactory typeFactory = corRel.getCluster().getTypeFactory();
        
        if (corRel.getJoinType() != JoinRelType.LEFT) {
            // TODO rchen 2006-11-15: inner correlations can be optimized too
            // select * from emps 
            // where deptno in (select deptno from depts where depts.deptno = emps.deptno +1);
            return;
        }
        JoinRelType joinType = corRel.getJoinType();
        
        // check for this pattern
        // The pattern matching could be simplified if rules can be applied
        // during decorrelation,
        //
        // CorrelateRel
        //   leftInputRel
        //   AggregateRel (rightInputRel)
        //     ProjectRel
        //       FilterRel
        //         filterInputRel
        if (!(corRel.getRight() instanceof AggregateRel)) {
            return;
        }
        AggregateRel aggRel = (AggregateRel) corRel.getRight();
        
        // check that the agg is simply doing single_value() on the entire 
        // input
        if (aggRel.getGroupCount() != 0 ||
            aggRel.getAggCalls().length != 1) {
            return;
        }
        
        AggregateRel.Call aggCall = aggRel.getAggCalls()[0];
        if (!(aggCall.getAggregation() instanceof SqlSingleValueAggFunction)) {
            return;
        }
        
        // check the aggRel has a project input
        if (!(aggRel.getChild() instanceof ProjectRel)) {
            return;
        }
        ProjectRel projRel = (ProjectRel) aggRel.getChild();
        
        // check this project only projects one expression(scalar)
        if (projRel.getProjectExps().length != 1) {
        	return;
        }
        RexNode projExpr = projRel.getProjectExps()[0];
        
        // check the projRel has a filter input
        if (!(projRel.getChild() instanceof FilterRel)) {
            return;
        }
        
        FilterRel filterRel = (FilterRel) projRel.getChild();
        if (!mapRefRelToCorVar.containsKey((RelNode)filterRel)) {
            // this filterRel does not contain any correlated reference
            return;
        }

        HashSet<CorrelatorRel.Correlation> corVarInFilter =
            new HashSet<CorrelatorRel.Correlation> ();        
        corVarInFilter.addAll(mapRefRelToCorVar.get(filterRel));
        
        RelNode filterInputRel = filterRel.getChild();
        
        // extract the correlation out of the filterRel
        
        // first break up the filter conditions into equality comparisons
        // AND'ed together
        List<RexInputRef>  joinKeys = new ArrayList<RexInputRef> ();
        List<RexFieldAccess>  correlatedJoinKeys = new ArrayList<RexFieldAccess> ();
        RexNode nonEquiCond = 
            RelOptUtil.splitCorrelatedFilterCondition(
                filterRel, 
                joinKeys,
                correlatedJoinKeys);
        
        // check that the columns referenced in these comparisons form an unique
        // key of the filterInputRel
        if (joinKeys.isEmpty() ||
            !RelMdUtil.areColumnsDefinitelyUnique(filterInputRel, joinKeys)) {
        	
            if (!RelMdUtil.areColumnsDefinitelyUnique(filterInputRel, joinKeys)) {
                sqlToRelTracer.fine(
                		joinKeys.toString() + 
                		"are not unique keys for " +
                		filterInputRel.toString());
            }
            return;
        }
        
        // remember the columns from filterInputRel
        // check that all filter's correlated refs are used in the join
        for (RexFieldAccess fieldAccess : correlatedJoinKeys) {
            corVarInFilter.remove(mapFieldAccessToCorVar.get(fieldAccess));
        }
        
        if (!corVarInFilter.isEmpty()) {
            return;
        }
        
        // Check that the correlated variables referenced in these comparisons
        // do come from the correlatorRel.
        corVarInFilter.addAll(mapRefRelToCorVar.get(filterRel));
        
        for (CorrelatorRel.Correlation corVar : corVarInFilter) {
            if (mapCorVarToCorRel.get(corVar) != corRel) {
                return;
            }
        }        
                
        // if projRel has any correlated reference, make sure they are also provided
        // by the current corRel. They will be projected out of the LHS of the corRel.
        if (mapRefRelToCorVar.containsKey(projRel)) {
            SortedSet<CorrelatorRel.Correlation> corVarInProj =
                mapRefRelToCorVar.get(projRel);
            for (CorrelatorRel.Correlation corVar : corVarInProj) {
                if (mapCorVarToCorRel.get(corVar) != corRel) {
                    return;
                }
            }
        }
        
        // change the plan to this structure
        // Note that the aggregateRel is removed
        //
        // ProjectRel (replace corvar to input ref coming from the JoinRel)
        //   JoinRel (replace corvar to input ref coming from the JoinRel)
        //     leftInputRel
        //     filterInputRel        
        
        // make the new join rel
        
        // first change the filter condition into a join condition
        RexNode joinCond = removeCorrelationExpr(filterRel.getCondition());
        
        JoinRel joinRel = 
            new JoinRel(
                corRel.getCluster(),
                leftInputRel,
                filterInputRel,
                joinCond,
                joinType,
                Collections.EMPTY_SET);
        
        List<Integer>  leftJoinKeys = new ArrayList<Integer> ();
        List<Integer>  rightJoinKeys = new ArrayList<Integer> ();

        nonEquiCond  =
            RelOptUtil.splitJoinCondition(
                joinRel.getLeft(),
                joinRel.getRight(),
                joinRel.getCondition(), 
                leftJoinKeys,
                rightJoinKeys);

        assert (rightJoinKeys.size() >= 1);
        
        int nullIndicatorPos = 
            joinRel.getLeft().getRowType().getFieldCount() +
            rightJoinKeys.get(0);
        
        RexInputRef nullIndicator =
            new RexInputRef(
                nullIndicatorPos,
                typeFactory.createTypeWithNullability(
                    (joinRel.getRowType().getFields()[nullIndicatorPos]).getType(),
                    true));            
        
        // now create the new project
        List<String> newFieldNames = new ArrayList<String>();
        List<RexNode> newProjExprs = new ArrayList<RexNode>();
        
        // project everything from the LHS and then those from the original projRel
        RelDataTypeField[] leftInputFields = 
            leftInputRel.getRowType().getFields();
        
        for (int i = 0; i < leftInputFields.length; i ++) {
            RelDataType newType = leftInputFields[i].getType();
            newProjExprs.add(new RexInputRef(i, newType));
            newFieldNames.add(leftInputFields[i].getName());            
        }

        // Marked where the projected expr is coming from so that the types will
        // become nullable for the original projections which are now coming out 
        // of the nullable side of the OJ.
        projectPulledAboveLeftCorrelator = joinType.generatesNullsOnRight();

        RexNode newProjExpr = removeCorrelationExpr(projExpr);
        
        // Unless the projected expression is a simple input ref (of the RHS)
        // need to enforce nullability by applying an additional cast operator
        // over the transformed expression.
        // Input refs are transformed inside removeCorrelationExpr to be nullable.
        if (!(projExpr instanceof RexInputRef) &&
            projectPulledAboveLeftCorrelator) {
            RexNode [] caseOperands = new RexNode[3];
            
            // This also covers the case where a left correlated subquery 
            // projects fields from outer relation.
            // Sicne LOJ cannot produce nulls on the LHS, the projection now need to
            // make a nullable LHS reference using a join indicator(whether any RHS ref 
            // is null) 
            
            // Construct a CASE expression to handle the null
            // indicator.

            // WHEN indicator IS NULL
            caseOperands[0] =
                rexBuilder.makeCall(
                    SqlStdOperatorTable.isNullOperator,
                    new RexInputRef(
                        nullIndicator.getIndex(),
                        typeFactory.createTypeWithNullability(
                            nullIndicator.getType(), true)));

            // THEN CAST(NULL AS newInputTypeNullable)
            caseOperands[1] =
                rexBuilder.makeCast(
                    typeFactory.createTypeWithNullability(
                        newProjExpr.getType(), true),
                    rexBuilder.constantNull());
            
            // ELSE case (newInput AS newInputTypeNullable) END
            caseOperands[2] = 
                rexBuilder.makeCast(
                    typeFactory.createTypeWithNullability(
                        newProjExpr.getType(), true),
                    newProjExpr);

            newProjExpr =
                rexBuilder.makeCall(
                    SqlStdOperatorTable.caseOperator,
                    caseOperands);
        }
        
        newProjExprs.add(newProjExpr);
        newFieldNames.add((projRel.getRowType().getFields()[0]).getName());
        
        RelNode newProjRel =
            CalcRel.createProject(joinRel, newProjExprs, newFieldNames);

        mapOldToNewRel.put(corRel, newProjRel);
    }
    
    /**
     * Rewrite SortRel.
     * 
     * 1. change the collations field to reference the new input.
     * 
     * @param rel SortRel to be rewritten
     */
    public void decorrelateRel(SortRel rel)
    {
        // SortRel itself should not reference cor vars.
        assert (!mapRefRelToCorVar.containsKey(rel));

        // SortRel only references field positions in collations field.
        // The collations field in the newRel now need to refer to the
        // new output positions in its input.
        // Its output does not change the input ordering, so there's no
        // need to call propagateExpr. 
        
        RelNode oldChildRel = rel.getChild();

        RelNode newChildRel = mapOldToNewRel.get(oldChildRel);
        if (newChildRel == null) {
            // If child has not been rewritten, do not rewrite this rel.
            return;
        }
        
        Map<Integer, Integer> childMapOldToNewOutputPos =
            mapNewRelToMapOldToNewOutputPos.get(newChildRel);
        assert (childMapOldToNewOutputPos != null);
        
        RelFieldCollation [] oldCollations = rel.getCollations();
        RelFieldCollation [] newCollations =
            new RelFieldCollation[oldCollations.length];
        
        int oldInputPos;
        for (int i = 0; i < oldCollations.length; ++i) {
            oldInputPos = oldCollations[i].getFieldIndex();
                
            newCollations[i] =
                new RelFieldCollation(childMapOldToNewOutputPos.get(oldInputPos));
        }

        SortRel newRel =
            new SortRel(
                rel.getCluster(),
                newChildRel,
                newCollations);

        mapOldToNewRel.put(rel, newRel);
        
        // SortRel does not change input ordering
        mapNewRelToMapOldToNewOutputPos.put(newRel, childMapOldToNewOutputPos);
    }

    /**
     * Rewrite AggregateRel.
     * 
     * 1. Permute the group by keys to the front.
     * 2. If the child of an AggregateRel is producing correlated variables,
     *    add them to the group list.
     * 3. Change aggCalls to reference the new ProjectRel. 
     *  
     * @param rel the project rel to rewrite
     */
    public void decorrelateRel(AggregateRel rel)
    {   
        // AggregaterRel itself should not reference cor vars.
        assert (!mapRefRelToCorVar.containsKey(rel));

        RelNode oldChildRel = rel.getChild();

        RelNode newChildRel = mapOldToNewRel.get(oldChildRel);
        if (newChildRel == null) {
            // If child has not been rewritten, do not rewrite this rel.
            return;
        }
        
        Map<Integer, Integer> childMapOldToNewOutputPos =
            mapNewRelToMapOldToNewOutputPos.get(newChildRel);
        assert (childMapOldToNewOutputPos != null);
        
        // map from newChildRel
        Map<Integer, Integer> mapNewChildToProjOutputPos =
            new HashMap<Integer, Integer> ();
        int oldGroupKeyCount = rel.getGroupCount();

        // ProjectRel projects the original expressions,
        // plus any correlated variables the child wants to pass along.
        List<RexNode> exprs = new ArrayList<RexNode>();
        List<String> exprNames = new ArrayList<String>();
        
        RelDataTypeField[] newChildOutput =
            newChildRel.getRowType().getFields();

        RelDataType fieldType;
        RexInputRef newInput;
        
        int newChildPos;
        int newPos;
        int newGroupKeyCount;
        
        // oldChildRel has the original group by keys in the front.
        for (newPos = 0; newPos < oldGroupKeyCount; newPos ++) { 
            newChildPos = childMapOldToNewOutputPos.get(newPos);
            
            fieldType = newChildOutput[newChildPos].getType();
            newInput = new RexInputRef(newChildPos, fieldType);
            exprs.add(newInput);
            exprNames.add(newChildOutput[newChildPos].getName());                    

            mapNewChildToProjOutputPos.put(newChildPos, newPos);
        }

        SortedMap<CorrelatorRel.Correlation, Integer> mapCorVarToOutputPos =
            new TreeMap<CorrelatorRel.Correlation, Integer> ();
        
        boolean produceCorVar =
            mapNewRelToMapCorVarToOutputPos.containsKey(newChildRel);
        if (produceCorVar) {
            // If child produces correlated variables, move them to the front,
            // right after any existing groupby fields.

            SortedMap<CorrelatorRel.Correlation, Integer> childMapCorVarToOutputPos = 
                mapNewRelToMapCorVarToOutputPos.get(newChildRel);
        
            // Now add the corVars from the child, starting from
            // position oldGroupKeyCount.
            for (CorrelatorRel.Correlation corVar : childMapCorVarToOutputPos.keySet()) {
                newChildPos = childMapCorVarToOutputPos.get(corVar);
                
                fieldType = newChildOutput[newChildPos].getType();
                newInput = new RexInputRef(newChildPos, fieldType);
                exprs.add(newInput);
                exprNames.add(newChildOutput[newChildPos].getName());

                mapCorVarToOutputPos.put(corVar, newPos);
                mapNewChildToProjOutputPos.put(newChildPos, newPos);                        
                newPos ++;
            }
        }
        
        // add the remaining fields
        newGroupKeyCount = newPos;
        for (int i = 0; i < newChildOutput.length; i ++) {
            if (!mapNewChildToProjOutputPos.containsKey(i)) {
                fieldType = newChildOutput[i].getType();
                newInput = new RexInputRef(i, fieldType);
                exprs.add(newInput);
                exprNames.add(newChildOutput[i].getName());
                
                mapNewChildToProjOutputPos.put(i, newPos);
                newPos ++;
            }
        }
            
        assert (newPos == newChildOutput.length);
        
        // This ProjectRel will be what the old child maps to,
        // replacing any previous mapping from old child).            
        RelNode newProjectRel =
            CalcRel.createProject(newChildRel, exprs, exprNames);
                   
        // update mappings:
        // oldChildRel ----> newChildRel
        //
        //                   newProjectRel
        //                        |
        // oldChildRel ---->  newChildRel
        //
        // is transformed to
        //
        // oldChildRel ----> newProjectRel
        //                        |
        //                   newChildRel
        Map<Integer, Integer> combinedMap = new HashMap<Integer, Integer> ();
                                        
        for (Integer oldChildPos : childMapOldToNewOutputPos.keySet()) {
            combinedMap.put(oldChildPos,
                mapNewChildToProjOutputPos.get(childMapOldToNewOutputPos.get(oldChildPos)));
        }
                    
        mapOldToNewRel.put(oldChildRel, newProjectRel);
        mapNewRelToMapOldToNewOutputPos.put(newProjectRel, combinedMap);
        
        if (produceCorVar) {
            mapNewRelToMapCorVarToOutputPos.put(newProjectRel, mapCorVarToOutputPos);                    
        }
        
        // now it's time to rewrite AggregateRel
        List<AggregateRel.Call> newAggCalls =
            new ArrayList<AggregateRel.Call> ();
        AggregateRel.Call [] oldAggCalls = rel.getAggCalls();
                
        AggregateRel.Call oldAggCall;
        int oldChildOutputFieldCount = oldChildRel.getRowType().getFieldCount();
        int newChildOutputFieldCount = newProjectRel.getRowType().getFieldCount();
            
        for (int i = 0; i < oldAggCalls.length; i ++) {
            oldAggCall = oldAggCalls[i];
            int [] oldAggArgs = oldAggCall.getArgs();

            int[] aggArgs = new int[oldAggArgs.length];
            
            for (int j = 0; j < oldAggArgs.length; j ++) {
                aggArgs[j] = oldAggArgs[j];
            }
            
            // Adjust them aggregater argument positions.
            // Note aggregater does not change input ordering, so the child
            // output position mapping can be used to derive the new positions
            // for the argument.
            for (int k = 0; k < aggArgs.length; k ++) {
                int oldPos = aggArgs[k];
                aggArgs[k] = combinedMap.get(oldPos);
            }
                    
            newAggCalls.add(
                new AggregateRel.Call(
                    oldAggCall.getAggregation(),
                    oldAggCall.isDistinct(),
                    aggArgs,
                    oldAggCall.getType()));
            // The old to new output position mapping will be the same as that of
            // newProjectRel, plus any aggregates that the oldAgg produces.
            combinedMap.put(oldChildOutputFieldCount + i, newChildOutputFieldCount + i);
        }
                
        AggregateRel newAggregateRel= 
            new AggregateRel(
                rel.getCluster(),
                newProjectRel,
                newGroupKeyCount,
                newAggCalls.toArray(new AggregateRel.Call[0]));
                
        mapOldToNewRel.put(rel, newAggregateRel);
        
        mapNewRelToMapOldToNewOutputPos.put(newAggregateRel, combinedMap);
                
        if (produceCorVar) {
            // AggregaterRel does not change input ordering so corVars will be
            // located at the same position as the input newProjectRel.
            mapNewRelToMapCorVarToOutputPos.put(newAggregateRel, mapCorVarToOutputPos);
        }
    } 
    
    /**
     * Rewrite ProjectRel.
     * 
     * 1. Pass along any correlated variables coming from the child.
     * 
     * @param rel the project rel to rewrite
     */
    public void decorrelateRel(ProjectRel rel)
    {        
        RelNode oldChildRel = rel.getChild();

        RelNode newChildRel = mapOldToNewRel.get(oldChildRel);
        if (newChildRel == null) {
            // If child has not been rewritten, do not rewrite this rel.
            return;
        }
        RexNode[] oldProj = rel.getProjectExps();
        RelDataTypeField[] relOutput = rel.getRowType().getFields();
        
        Map<Integer, Integer> childMapOldToNewOutputPos =
            mapNewRelToMapOldToNewOutputPos.get(newChildRel);
        assert (childMapOldToNewOutputPos != null);

        Map<Integer, Integer> mapOldToNewOutputPos =
            new HashMap<Integer, Integer> ();

        boolean produceCorVar = 
            mapNewRelToMapCorVarToOutputPos.containsKey(newChildRel);

        // ProjectRel projects the original expressions,
        // plus any correlated variables the child wants to pass along.
        List<RexNode> exprs = new ArrayList<RexNode>();
        List<String> exprNames = new ArrayList<String>();
        
        // If this ProjectRel has correlated reference, create value generator and
        // produce the correlated variables in the new output.
        if (mapRefRelToCorVar.containsKey(rel)) {
            decorrelateInputWithValueGenerator(rel);
            // The old child should be mapped to the JoinRel created by
            // rewriteInputWithValueGenerator().
            newChildRel = mapOldToNewRel.get(oldChildRel);            
            produceCorVar = true;            
        }        

        // ProjectRel projects the original expressions
        int newPos;
        for (newPos = 0; newPos < oldProj.length; newPos ++) {
            exprs.add(newPos, decorrelateExpr(oldProj[newPos]));
            exprNames.add(newPos, relOutput[newPos].getName());
            mapOldToNewOutputPos.put(newPos, newPos);
        }

        SortedMap<CorrelatorRel.Correlation, Integer> mapCorVarToOutputPos =
            new TreeMap<CorrelatorRel.Correlation, Integer> ();
        
        // Project any correlated variables the child wants to pass along.
        if (produceCorVar) {
            SortedMap<CorrelatorRel.Correlation, Integer> childMapCorVarToOutputPos = 
                mapNewRelToMapCorVarToOutputPos.get(newChildRel);
            
            // propagate cor vars from the new child
            int corVarPos;
            RelDataType fieldType;
            RexInputRef newInput;
            RelDataTypeField[] newChildOutput = newChildRel.getRowType().getFields();
            for (CorrelatorRel.Correlation corVar : childMapCorVarToOutputPos.keySet()) {
                corVarPos = childMapCorVarToOutputPos.get(corVar);
                fieldType = newChildOutput[corVarPos].getType();
                newInput = new RexInputRef(corVarPos, fieldType);
                exprs.add(newInput);
                exprNames.add(newChildOutput[corVarPos].getName());
                mapCorVarToOutputPos.put(corVar, newPos);
                newPos ++;
            }
        }
        
        RelNode newProjectRel =
            CalcRel.createProject(newChildRel, exprs, exprNames);
            
        mapOldToNewRel.put(rel, newProjectRel);
        mapNewRelToMapOldToNewOutputPos.put(newProjectRel, mapOldToNewOutputPos);
            
        if (produceCorVar) {
            mapNewRelToMapCorVarToOutputPos.put(newProjectRel, mapCorVarToOutputPos);
        }
    }

    /**
     * Create RelNode tree that produces a list of correlated variables.
     * 
     * @param correlations correlated variables to generate
     * @param valueGenFieldOffset offset in the output that generated columns will start
     * @param mapCorVarToOutputPos output positions for the correlated variables generated
     * @return RelNode the root of the resultant RelNode tree
     */
    private RelNode createValueGenerator(
        SortedSet<CorrelatorRel.Correlation> correlations,
        int valueGenFieldOffset,
        SortedMap<CorrelatorRel.Correlation, Integer> mapCorVarToOutputPos)
    {
        RelNode resultRel = null;
        
        Map<RelNode, List<Integer> > mapNewInputRelToOutputPos
            = new HashMap<RelNode, List<Integer> >();
                
        Map<RelNode, Integer> mapNewInputRelToNewOffset
            = new HashMap<RelNode, Integer> ();
        
        RelNode oldInputRel;
        RelNode newInputRel;
        List<Integer> newLocalOutputPosList;

        // inputRel provides the definition of a correlated variable.
        // Add to map all the referenced positions(relative to each input rel)
        for (CorrelatorRel.Correlation corVar : correlations) {
            int oldCorVarOffset = corVar.getOffset();
            
            oldInputRel = mapCorVarToCorRel.get(corVar).getInput(0);
            assert (oldInputRel != null);
            newInputRel = mapOldToNewRel.get(oldInputRel);
            assert (newInputRel != null);
            
            if (!mapNewInputRelToOutputPos.containsKey(newInputRel)) {
                newLocalOutputPosList = new ArrayList<Integer>();
            } else {
                newLocalOutputPosList = mapNewInputRelToOutputPos.get(newInputRel);
            }
            
            Map<Integer, Integer> mapOldToNewOutputPos =
                mapNewRelToMapOldToNewOutputPos.get(newInputRel);
            assert(mapOldToNewOutputPos != null);
            
            int newCorVarOffset = mapOldToNewOutputPos.get(oldCorVarOffset);
            
            // Add all unique positions referenced.
            if (!newLocalOutputPosList.contains(newCorVarOffset)) {
                newLocalOutputPosList.add(newCorVarOffset);
            }
            mapNewInputRelToOutputPos.put(newInputRel, newLocalOutputPosList);                
        }
        
        int offset = 0;
        
        // Project only the correlated fields out of each inputRel
        // and join the projectRel together.
        // To make sure the plan does not change in terms of join order,
        // join these rels based on their occurance in cor var list which
        // is sorted.
        Set<RelNode> joinedInputRelSet = new HashSet<RelNode> ();
        
        for (CorrelatorRel.Correlation corVar : correlations) {
            oldInputRel = mapCorVarToCorRel.get(corVar).getInput(0);
            assert (oldInputRel != null);
            newInputRel = mapOldToNewRel.get(oldInputRel);
            assert (newInputRel != null);

            if (!joinedInputRelSet.contains(newInputRel)) {
                RelNode projectRel =
                    CalcRel.createProject(newInputRel,
                        mapNewInputRelToOutputPos.get(newInputRel));
                RelNode distinctRel = RelOptUtil.createDistinctRel(projectRel);
                RelOptCluster cluster = distinctRel.getCluster();
            
                joinedInputRelSet.add(newInputRel);
                mapNewInputRelToNewOffset.put(newInputRel, offset);
                offset += distinctRel.getRowType().getFieldCount();
            
                if (resultRel == null) {
                    resultRel = distinctRel;
                } else {
                    resultRel =
                        new JoinRel(
                            cluster,
                            resultRel,
                            distinctRel,
                            cluster.getRexBuilder().makeLiteral(true),
                            JoinRelType.INNER,
                            Collections.EMPTY_SET);                    
                }
            }
        }
        
        // Translate the positions of correlated variables to be relative to
        // the join output, leaving room for valueGenFieldOffset because
        // valueGenerators are joined with the original left input of the rel
        // referencing correlated variables.
        int newOutputPos, newLocalOutputPos;
        for (CorrelatorRel.Correlation corVar : correlations) {
            // The first child of a correlatorRel is always the rel defining
            // the correlated variables.
            newInputRel = mapOldToNewRel.get(mapCorVarToCorRel.get(corVar).getInput(0));
            newLocalOutputPosList = mapNewInputRelToOutputPos.get(newInputRel);
            
            Map<Integer, Integer> mapOldToNewOutputPos =
                mapNewRelToMapOldToNewOutputPos.get(newInputRel);
            assert(mapOldToNewOutputPos != null);

            newLocalOutputPos = mapOldToNewOutputPos.get(corVar.getOffset());
            
            // newOutputPos is the index of the cor var in the referenced
            // position list plus the offset of referenced postition list of
            // each newInputRel.
            newOutputPos = newLocalOutputPosList.indexOf(newLocalOutputPos) +
                           mapNewInputRelToNewOffset.get(newInputRel) +
                           valueGenFieldOffset;
            
            if (mapCorVarToOutputPos.containsKey(corVar)) {
                assert (mapCorVarToOutputPos.get(corVar) == newOutputPos);
            }
            mapCorVarToOutputPos.put(corVar, newOutputPos);
        }
        
        return resultRel;
    }

    private void decorrelateInputWithValueGenerator(
        RelNode rel)
    {
        // currently only handles one child input
        assert (rel.getInputs().length == 1);
        RelNode oldChildRel = rel.getInput(0);
        RelNode newChildRel = mapOldToNewRel.get(oldChildRel);

        Map<Integer, Integer> childMapOldToNewOutputPos =
            mapNewRelToMapOldToNewOutputPos.get(newChildRel);
        assert (childMapOldToNewOutputPos != null);

        SortedMap<CorrelatorRel.Correlation, Integer> mapCorVarToOutputPos =
            new TreeMap<CorrelatorRel.Correlation, Integer> ();
        
        if (mapNewRelToMapCorVarToOutputPos.containsKey(newChildRel)) {
            mapCorVarToOutputPos.putAll(
                mapNewRelToMapCorVarToOutputPos.get(newChildRel));
        }
        
        SortedSet<CorrelatorRel.Correlation> corVarList =
            mapRefRelToCorVar.get(rel);
        
        RelNode newLeftChildRel = newChildRel;

        int leftChildOutputCount =
            newLeftChildRel.getRowType().getFieldCount();

        // can directly add positions into mapCorVarToOutputPos since join
        // does not change the output ordering from the children.
        RelNode valueGenRel =
            createValueGenerator(
                corVarList,
                leftChildOutputCount,
                mapCorVarToOutputPos);

        RelNode joinRel =
            new JoinRel(
                rel.getCluster(),
                newLeftChildRel,
                valueGenRel,
                rexBuilder.makeLiteral(true),
                JoinRelType.INNER,
                Collections.EMPTY_SET);
        
        mapOldToNewRel.put(oldChildRel, joinRel);
        mapNewRelToMapCorVarToOutputPos.put(joinRel, mapCorVarToOutputPos);
        // JoinRel or FilterRel does not change the old input ordering. All
        // input fields from newLeftInput(i.e. the original input to the old
        // FilterRel) are in the output and in the same position.
        mapNewRelToMapOldToNewOutputPos.put(joinRel, childMapOldToNewOutputPos);
    }
    
    /**
     * Rewrite FilterRel.
     * 
     * 1. If a FilterRel references a correlated field in its filter condition,
     * rewrite the FilterRel to be
     * 
     *  FilterRel
     *    JoinRel (cross product)
     *      OriginalFilterInput
     *      ValueGenerator (produces distinct sets of correlated variables)
     *      
     *  and rewrite the correlated fieldAccess in the filter condition to
     *  reference the JoinRel output. 
     * 2. If FilterRel does not reference correlated variables, simply rewrite
     * the filter condition using new input.
     * 
     * @param rel the filter rel to rewrite
     */
    public void decorrelateRel(FilterRel rel)
    { 
        RelNode oldChildRel = rel.getChild();

        RelNode newChildRel = mapOldToNewRel.get(oldChildRel);
        if (newChildRel == null) {
            // If child has not been rewritten, do not rewrite this rel.
            return;
        }

        Map<Integer, Integer> childMapOldToNewOutputPos =
            mapNewRelToMapOldToNewOutputPos.get(newChildRel);
        assert (childMapOldToNewOutputPos != null);

        boolean produceCorVar = 
            mapNewRelToMapCorVarToOutputPos.containsKey(newChildRel);
                
        // If this FilterRel has correlated reference, create value generator and
        // produce the correlated variables in the new output.
        if (mapRefRelToCorVar.containsKey(rel)) {
            decorrelateInputWithValueGenerator(rel);
            // The old child should be mapped to the newly created JoinRel by
            // rewriteInputWithValueGenerator().
            newChildRel = mapOldToNewRel.get(oldChildRel);            
            produceCorVar = true;            
        }

        // Replace the filter expression to reference output of the join
        // Map filter to the new filter over join
        RelNode newFilterRel =
            CalcRel.createFilter(newChildRel, 
                decorrelateExpr(rel.getCondition()));
            
        mapOldToNewRel.put(rel, newFilterRel);
        // Filter does not change the input ordering.
        mapNewRelToMapOldToNewOutputPos.put(newFilterRel, childMapOldToNewOutputPos);

        if (produceCorVar) {
            // filter rel does not permute the input
            // all corvars produced by filter will have the same output positions
            // in the child rel.
            mapNewRelToMapCorVarToOutputPos.put(
                newFilterRel, 
                mapNewRelToMapCorVarToOutputPos.get(newChildRel));
        }
    }

    /**
     * Rewrite CorrelatorRel into a left outer join. The original left input
     * will be joined with the new right input that has generated correlated
     * variables propagated up.
     * 
     * For any generated cor vars that are not used in the join key, pass them
     * along to be joined later with the CorrelatorRels that produce them.
     * 
     * @param rel CorrelatorRel
     */
    public void decorrelateRel(CorrelatorRel rel)
    {
        // the right input to CorrelatorRel should produce correlated variables
        RelNode oldLeftRel = rel.getInputs()[0];
        RelNode oldRightRel = rel.getInputs()[1];

        RelNode newLeftRel = mapOldToNewRel.get(oldLeftRel);
        RelNode newRightRel = mapOldToNewRel.get(oldRightRel);

        if (newLeftRel == null || newRightRel == null) {
            // If any child has not been rewritten, do not rewrite this rel.
            return;
        }
                        
        SortedMap<CorrelatorRel.Correlation, Integer> rightChildMapCorVarToOutputPos =
            mapNewRelToMapCorVarToOutputPos.get(newRightRel);
        
        if (rightChildMapCorVarToOutputPos == null) {
            return;
        }
        
        Map<Integer, Integer> leftChildMapOldToNewOutputPos =
            mapNewRelToMapOldToNewOutputPos.get(newLeftRel);
        assert (leftChildMapOldToNewOutputPos != null);

        Map<Integer, Integer> rightChildMapOldToNewOutputPos =
            mapNewRelToMapOldToNewOutputPos.get(newRightRel);
        
        assert (rightChildMapOldToNewOutputPos != null);

        SortedMap<CorrelatorRel.Correlation, Integer> mapCorVarToOutputPos =
            rightChildMapCorVarToOutputPos;
        
        assert (rel.getCorrelations().size() <= 
                rightChildMapCorVarToOutputPos.keySet().size());
        
        // Change correlator rel into a join.
        // Join all the correlated variables produced by this correlator rel
        // with the values generated and propagated from the right input
        RexNode condition = rel.getCondition();
        final RelDataTypeField [] newLeftOutput =
            newLeftRel.getRowType().getFields();
        int newLeftFieldCount = newLeftOutput.length;
        
        final RelDataTypeField [] newRightOutput =
            newRightRel.getRowType().getFields();
        
        int newLeftPos, newRightPos;
        for (CorrelatorRel.Correlation corVar : rel.getCorrelations()) {
            newLeftPos = leftChildMapOldToNewOutputPos.get(corVar.getOffset());
            newRightPos = rightChildMapCorVarToOutputPos.get(corVar);
            RexNode equi = 
                rexBuilder.makeCall(
                    SqlStdOperatorTable.equalsOperator,
                    new RexInputRef(
                        newLeftPos,
                        newLeftOutput[newLeftPos].getType()),
                    new RexInputRef(
                        newLeftFieldCount + newRightPos,
                        newRightOutput[newRightPos].getType()));
            if (condition == rexBuilder.makeLiteral(true)) {
                condition = equi;
            } else {
                condition =
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.andOperator,
                        condition,
                        equi);
            }

            // remove this cor var from output position mapping
            mapCorVarToOutputPos.remove(corVar);
        }
        
        // Update the output position for the cor vars: only pass on the cor
        // vars that are not used in the join key.
        for (CorrelatorRel.Correlation corVar : mapCorVarToOutputPos.keySet()) {
            int newPos = mapCorVarToOutputPos.get(corVar) + newLeftFieldCount;
            mapCorVarToOutputPos.put(corVar, newPos);
        }

        // then add any cor var from the left input. Do not need to change
        // output positions.
        if (mapNewRelToMapCorVarToOutputPos.containsKey(newLeftRel)) {
            mapCorVarToOutputPos.putAll(
                mapNewRelToMapCorVarToOutputPos.get(newLeftRel));
        }
        
        // Create the mapping between the output of the old correlation rel
        // and the new join rel
        Map<Integer, Integer> mapOldToNewOutputPos =
            new HashMap<Integer, Integer> ();

        int oldLeftFieldCount = oldLeftRel.getRowType().getFieldCount();
        
        int oldRightFieldCount = oldRightRel.getRowType().getFieldCount();
        assert (rel.getRowType().getFieldCount() ==
                oldLeftFieldCount + oldRightFieldCount);
        
        // Left input positions are not changed.
        mapOldToNewOutputPos.putAll(leftChildMapOldToNewOutputPos);
        
        // Right input positions are shifted by newLeftFieldCount.
        for (int i = 0; i < oldRightFieldCount; i ++) {
            mapOldToNewOutputPos.put(i+oldLeftFieldCount,
                rightChildMapOldToNewOutputPos.get(i)+newLeftFieldCount);
        }

        RelNode newRel = new JoinRel(
            rel.getCluster(),
            newLeftRel,
            newRightRel,
            condition,
            rel.getJoinType(),
            Collections.EMPTY_SET);
                
        mapOldToNewRel.put(rel, newRel);
        mapNewRelToMapOldToNewOutputPos.put(newRel, mapOldToNewOutputPos); 
        
        if (!mapCorVarToOutputPos.isEmpty()) {
            mapNewRelToMapCorVarToOutputPos.put(newRel, mapCorVarToOutputPos);
        }
    }
    
    /**
     * Rewrite JoinRel.
     * 
     * 1. rewrite join condition.
     * 2. map output positions and produce cor vars if any.
     * 
     * @param rel JoinRel
     */
    public void decorrelateRel(JoinRel rel)
    {
        RelNode oldLeftRel = rel.getInputs()[0];
        RelNode oldRightRel = rel.getInputs()[1];

        RelNode newLeftRel = mapOldToNewRel.get(oldLeftRel);
        RelNode newRightRel = mapOldToNewRel.get(oldRightRel);

        if (newLeftRel == null || newRightRel == null) {
            // If any child has not been rewritten, do not rewrite this rel.
            return;
        }
                        
        Map<Integer, Integer> leftChildMapOldToNewOutputPos =
            mapNewRelToMapOldToNewOutputPos.get(newLeftRel);
        assert (leftChildMapOldToNewOutputPos != null);

        Map<Integer, Integer> rightChildMapOldToNewOutputPos =
            mapNewRelToMapOldToNewOutputPos.get(newRightRel);
        assert (rightChildMapOldToNewOutputPos != null);

        SortedMap<CorrelatorRel.Correlation, Integer> mapCorVarToOutputPos =
            new TreeMap<CorrelatorRel.Correlation, Integer> ();
        
        RelNode newRel = new JoinRel(
            rel.getCluster(),
            newLeftRel,
            newRightRel,
            decorrelateExpr(rel.getCondition()),
            rel.getJoinType(),
            Collections.EMPTY_SET);
        
        // Create the mapping between the output of the old correlation rel
        // and the new join rel
        Map<Integer, Integer> mapOldToNewOutputPos =
            new HashMap<Integer, Integer> ();

        int oldLeftFieldCount = oldLeftRel.getRowType().getFieldCount();
        int newLeftFieldCount = newLeftRel.getRowType().getFieldCount();

        int oldRightFieldCount = oldRightRel.getRowType().getFieldCount();
        assert (rel.getRowType().getFieldCount() ==
                oldLeftFieldCount + oldRightFieldCount);
        
        // Left input positions are not changed.
        mapOldToNewOutputPos.putAll(leftChildMapOldToNewOutputPos);
        
        // Right input positions are shifted by newLeftFieldCount.
        for (int i = 0; i < oldRightFieldCount; i ++) {
            mapOldToNewOutputPos.put(i+oldLeftFieldCount,
                rightChildMapOldToNewOutputPos.get(i)+newLeftFieldCount);
        }
                             
        if (mapNewRelToMapCorVarToOutputPos.containsKey(newLeftRel)) {
            mapCorVarToOutputPos.putAll(
                mapNewRelToMapCorVarToOutputPos.get(newLeftRel));
        }
        
        // Right input positions are shifted by newLeftFieldCount.
        int oldRightPos;
        if (mapNewRelToMapCorVarToOutputPos.containsKey(newRightRel)) {
            SortedMap<CorrelatorRel.Correlation, Integer> rightChildMapCorVarToOutputPos =
                mapNewRelToMapCorVarToOutputPos.get(newRightRel);
            for (CorrelatorRel.Correlation corVar : rightChildMapCorVarToOutputPos.keySet()) {
                oldRightPos = rightChildMapCorVarToOutputPos.get(corVar);
                mapCorVarToOutputPos.put(corVar, oldRightPos + newLeftFieldCount);
            }
        }
        mapOldToNewRel.put(rel, newRel);
        mapNewRelToMapOldToNewOutputPos.put(newRel, mapOldToNewOutputPos); 
        
        if (!mapCorVarToOutputPos.isEmpty()) {
            mapNewRelToMapCorVarToOutputPos.put(newRel, mapCorVarToOutputPos);            
        }
    }
    
    //~ Inner Classes ----------------------------------------------------------

    private class DecorrelateRelVisitor
        extends RelVisitor
    {
        // implement RelVisitor
        public void visit(RelNode p, int ordinal, RelNode parent)
        {
            // rewrite children first  (from left to right)
            super.visit(p, ordinal, parent);

            currentRel = p;
            
            final String visitMethodName = "decorrelateRel";
            boolean found =
                ReflectUtil.invokeVisitor(
                    RelDecorrelator.this,
                    currentRel,
                    RelNode.class,
                    visitMethodName);
            currentRel = null;
            
            if (!found) {
                decorrelateRelGeneric(p);
            }
            //else no rewrite will occur. This will terminate the bottom-up
            // rewrite. If root node of a RelNode tree is not rewritten, the
            // original tree will be returned. See decorrelate() method.
        }
    }
    
    private class RemoveCorrelationVisitor
        extends RelVisitor
    {
        // implement RelVisitor
        public void visit(RelNode p, int ordinal, RelNode parent)
        {
            // rewrite children first  (from left to right)
            super.visit(p, ordinal, parent);

            currentRel = p;
            projectPulledAboveLeftCorrelator = false;
            
            final String visitMethodName = "removeCorrelationRel";
            boolean found =
                ReflectUtil.invokeVisitor(
                    RelDecorrelator.this,
                    currentRel,
                    RelNode.class,
                    visitMethodName);
            currentRel = null;
        
            if (!found) {
                removeCorrelationRelGeneric(p);
            }
            //else no rewrite will occur. This will terminate the bottom-up
            // rewrite. If root node of a RelNode tree is not rewritten, the
            // original tree will be returned. See decorrelate() method.
        }
    }

    private RexInputRef getNewForOldInputRef(RexInputRef oldInputRef)
    {
        assert (currentRel != null);

        int oldOrdinal = oldInputRef.getIndex();
        int newOrdinal = 0;

        // determine which input rel oldOrdinal references, and adjust
        // oldOrdinal to be relative to that input rel
        RelNode [] oldInputRels = currentRel.getInputs();
        RelNode oldInputRel = null;

        for (int i = 0; i < oldInputRels.length; ++i) {
            RelDataType oldInputType = oldInputRels[i].getRowType();
            int n = oldInputType.getFieldCount();
            if (oldOrdinal < n) {
                oldInputRel = oldInputRels[i];
                break;
            }
            RelNode newInput = mapOldToNewRel.get(oldInputRels[i]);
            newOrdinal += newInput.getRowType().getFieldCount();
            oldOrdinal -= n;
        }
        
        assert (oldInputRel != null);

        RelNode newInputRel = mapOldToNewRel.get(oldInputRel);
        assert (newInputRel != null);

        // now oldOrdinal is relative to oldInputRel
        int oldLocalOrdinal = oldOrdinal;
        
        // figure out the newLocalOrdinal, relative to the newInputRel.
        int newLocalOrdinal = oldLocalOrdinal;
        
        Map<Integer, Integer> mapOldToNewOutputPos =
            mapNewRelToMapOldToNewOutputPos.get(newInputRel);
            
        if (mapOldToNewOutputPos != null) {
            newLocalOrdinal = mapOldToNewOutputPos.get(oldLocalOrdinal);
        }
        
        newOrdinal += newLocalOrdinal;
        
        return new RexInputRef(newOrdinal,
            newInputRel.getRowType().getFields()[newLocalOrdinal].getType());
    }

    private class DecorrelateRexShuttle
    extends RexShuttle
    {
        // override RexShuttle
        public RexNode visitFieldAccess(RexFieldAccess fieldAccess)
        {
            int newInputRelOutputOffset = 0;
            RelNode oldInputRel;
            RelNode newInputRel;
            Integer newInputPos;
            
            for (int i = 0; i < currentRel.getInputs().length; i ++) {
                oldInputRel = currentRel.getInputs()[i];
                newInputRel = mapOldToNewRel.get(oldInputRel);
            
                if ((newInputRel != null) &&
                    mapNewRelToMapCorVarToOutputPos.containsKey(newInputRel)) {
                    SortedMap<CorrelatorRel.Correlation, Integer> childMapCorVarToOutputPos =
                        mapNewRelToMapCorVarToOutputPos.get(newInputRel);
            
                    if (childMapCorVarToOutputPos != null) {
                        //try to find in this input rel the position of cor var
                        CorrelatorRel.Correlation corVar =
                            mapFieldAccessToCorVar.get(fieldAccess);
         
                        if (corVar != null) {
                            newInputPos = 
                                childMapCorVarToOutputPos.get(corVar);
                            if (newInputPos != null) {
                                // this input rel does produce the cor var referenced
                                newInputPos += newInputRelOutputOffset;
                                // fieldAccess is assumed to have the correct type info.
                                RexInputRef newInput =
                                    new RexInputRef(newInputPos, fieldAccess.getType());
                                return newInput;
                            }
                        }
                    }
                    // this input rel does not produce the cor var needed 
                    newInputRelOutputOffset += newInputRel.getRowType().getFieldCount();
                } else {
                    // this input rel is not rewritten
                    newInputRelOutputOffset += oldInputRel.getRowType().getFieldCount();
                }
            }
            return fieldAccess;
        }

        // override RexShuttle
        public RexNode visitInputRef(RexInputRef inputRef)
        {
            RexInputRef newInputRef = getNewForOldInputRef(inputRef);
            return newInputRef;
        }
    }
    
    private class RemoveCorrelationRexShuttle
    extends RexShuttle
    {
        RelDataTypeFactory typeFactory;
        
        public RemoveCorrelationRexShuttle(RelDataTypeFactory typeFactory)
        {
            this.typeFactory = typeFactory;
        }
        
        // override RexShuttle
        public RexNode visitFieldAccess(RexFieldAccess fieldAccess)
        {
            if (mapFieldAccessToCorVar.containsKey(fieldAccess)) {
                // if it is a corVar, change it to be input ref.
                CorrelatorRel.Correlation corVar = 
                    mapFieldAccessToCorVar.get(fieldAccess);
                // corVar offset shuold point to the leftInput of currentRel,
                // which is the CorrelatorRel.
                RexInputRef newInput =
                    new RexInputRef(corVar.getOffset(), fieldAccess.getType());
                
                return newInput;
            }
            return fieldAccess;
        }

        // override RexShuttle
        public RexNode visitInputRef(RexInputRef inputRef)
        {
            if (currentRel != null && currentRel instanceof CorrelatorRel) {
                // if this rel references corVar
                // and now it needs to be rewritten
                // it must have been pulled above the CorrelatorRel
                // replace the input ref to account for the LHS of the
                // CorrelatorRel
                int leftInputFieldCount = 
                    ((CorrelatorRel)currentRel).getLeft().getRowType().getFieldCount();
                RelDataType newType = inputRef.getType();
                
                if (projectPulledAboveLeftCorrelator) {
                    newType = 
                        typeFactory.createTypeWithNullability(newType, true);
                }
                
                RexInputRef newInputRef =
                    new RexInputRef(
                        leftInputFieldCount + inputRef.getIndex(), 
                        newType);
                return newInputRef;
            }
            return inputRef;
        }        
    }    
}

// End RelDecorrelator.java
