/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 LucidEra, Inc.
// Copyright (C) 2006-2007 The Eigenbase Project
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

import com.lucidera.lcs.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * LoptModifyRemovableSelfJoinRule implements a rule that modifies the
 * inputs into a removable self-join so that they are in a standard format
 * that will be recognized by {@link LoptRemoveSelfJoinRule}.
 *
 * <p>Each input into the join is already a simple {@link LcsRowScanRel}.
 * There needs to be a {@link FilterRel} and {@link ProjectRel} on top of
 * each of the row scans.  So, this rule adds the missing {@link ProjectRel}
 * and/or {@link FilterRel}.  The {@link FilterRel} contains a dummy IS TRUE
 * expression while the {@link ProjectRel} simply projects each column.
 * Both will be removed later by {@link LoptRemoveSelfJoinRule}.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class LoptModifyRemovableSelfJoinRule
    extends RelOptRule
{
    //~ Static fields/initializers ---------------------------------------------

    public static final LoptModifyRemovableSelfJoinRule
        instanceRowScanOnLeft =
            new LoptModifyRemovableSelfJoinRule(
                new RelOptRuleOperand(
                    JoinRel.class,
                    new RelOptRuleOperand(LcsRowScanRel.class, ANY),
                    new RelOptRuleOperand(RelNode.class, ANY)),
                "row scan on left");
    
    public static final LoptModifyRemovableSelfJoinRule
        instanceRowScanOnRight =
            new LoptModifyRemovableSelfJoinRule(
                new RelOptRuleOperand(
                    JoinRel.class,
                    new RelOptRuleOperand(RelNode.class, ANY),
                    new RelOptRuleOperand(LcsRowScanRel.class, ANY)),
                 "row scan on right");
    
    public static final LoptModifyRemovableSelfJoinRule
        instanceFilterOnLeft =
            new LoptModifyRemovableSelfJoinRule(
                new RelOptRuleOperand(
                    JoinRel.class,
                    new RelOptRuleOperand(
                        FilterRel.class,
                        new RelOptRuleOperand(LcsRowScanRel.class, ANY)),
                    new RelOptRuleOperand(RelNode.class, ANY)),
                "filter on left");
    
    public static final LoptModifyRemovableSelfJoinRule
        instanceProjectOnLeft =
            new LoptModifyRemovableSelfJoinRule(
                new RelOptRuleOperand(
                    JoinRel.class,
                    new RelOptRuleOperand(
                        ProjectRel.class,
                        new RelOptRuleOperand(LcsRowScanRel.class, ANY)),
                    new RelOptRuleOperand(RelNode.class, ANY)),
                "project on left");
    
    public static final LoptModifyRemovableSelfJoinRule
        instanceFilterOnRight =
            new LoptModifyRemovableSelfJoinRule(
                new RelOptRuleOperand(
                    JoinRel.class,
                    new RelOptRuleOperand(RelNode.class, ANY),
                    new RelOptRuleOperand(
                        FilterRel.class,
                        new RelOptRuleOperand(LcsRowScanRel.class, ANY))),
                "filter on right");
    
    public static final LoptModifyRemovableSelfJoinRule
        instanceProjectOnRight =
            new LoptModifyRemovableSelfJoinRule(
                new RelOptRuleOperand(
                    JoinRel.class,
                    new RelOptRuleOperand(RelNode.class, ANY),
                    new RelOptRuleOperand(
                        ProjectRel.class,
                        new RelOptRuleOperand(LcsRowScanRel.class, ANY))),
                "project on right");
    
    //~ Constructors -----------------------------------------------------------

    public LoptModifyRemovableSelfJoinRule(RelOptRuleOperand rule, String id)
    {
        super(rule);
        description = "LoptModifyRemovableSelfJoinRule: " + id;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        JoinRel joinRel = (JoinRel) call.rels[0];
        if (!LoptOptimizeJoinRule.isRemovableSelfJoin(joinRel)) {
            return;
        }
        
        RelNode leftInput = null;
        RelNode rightInput = null;
               
        if (call.rels.length == 3) {
            // Handle the cases where either or both of the join inputs are
            // row scans, in which case, we have to add a project and filter
            // on top of the row scan.
            if (call.rels[1] instanceof LcsRowScanRel) {
                leftInput = addProjectAndFilter(call.rels[1]);
            }
            if (call.rels[2] instanceof LcsRowScanRel) {
                rightInput = addProjectAndFilter(call.rels[2]);
            }
            if (leftInput == null) {
                leftInput = call.rels[1];
            }
            if (rightInput == null) {
                rightInput = call.rels[2];
            }
            
        // For the remaining cases, only modify the input that matches the
        // more specific pattern.  The other input will be modified, as needed,
        // by another instantiation of the rule.
        } else if (call.rels[1] instanceof FilterRel &&
            call.rels[2] instanceof LcsRowScanRel)
        {
            rightInput = call.rels[3];
            leftInput = addProject(call.rels[1]);
        } else if (call.rels[1] instanceof ProjectRel &&
            call.rels[2] instanceof LcsRowScanRel)
        {
            rightInput = call.rels[3];
            leftInput = addFilter((ProjectRel) call.rels[1]);
        } else if (call.rels[2] instanceof FilterRel &&
            call.rels[3] instanceof LcsRowScanRel)
        {
            leftInput = call.rels[1];
            rightInput = addProject(call.rels[2]);
        } else if (call.rels[2] instanceof ProjectRel &&
            call.rels[3] instanceof LcsRowScanRel)
        {
            leftInput = call.rels[1];
            rightInput = addFilter((ProjectRel) call.rels[2]);
        } else {
            assert(false);
        }
        
        JoinRel newSelfJoin =
            new JoinRel(
                joinRel.getCluster(),
                leftInput,
                rightInput,
                joinRel.getCondition(),
                joinRel.getJoinType(),
                joinRel.getVariablesStopped(),
                joinRel.isSemiJoinDone());
        call.transformTo(newSelfJoin);        
    }
    
    /**
     * Inserts a {@link ProjectRel} and {@link FilterRel} on top of a row scan.
     * 
     * @param rowScan the row scan
     * 
     * @return the constructed RelNode tree
     */
    private ProjectRel addProjectAndFilter(RelNode rowScan)
    {
        RexBuilder rexBuilder = rowScan.getCluster().getRexBuilder();
        RelNode filterRel =
            CalcRel.createFilter(
                rowScan,
                rexBuilder.makeLiteral(true));
        return addProject(filterRel);
    }
    
    /**
     * Inserts a {@link ProjectRel} on top of a filter node.
     * 
     * @param filterRel the filter node
     * 
     * @return the constructed {@link ProjectRel} tree
     */
    private ProjectRel addProject(RelNode filterRel)
    {
        RexBuilder rexBuilder = filterRel.getCluster().getRexBuilder();
        RelDataTypeField[] fields = filterRel.getRowType().getFields();
        int nFields = fields.length;
        RexNode[] projExprs = new RexNode[nFields];
        String[] fieldNames = new String[nFields];
        for (int i = 0; i < nFields; i++) {
            projExprs[i] = rexBuilder.makeInputRef(fields[i].getType(), i);
            fieldNames[i] = fields[i].getName();
        }
        ProjectRel projRel =
            CalcRel.createProject(filterRel, projExprs, fieldNames);
        
        return projRel;
    }
    
    /**
     * Inserts a {@link FilterRel} with a TRUE condition in between a
     * {@link ProjectRel} and its input.
     * 
     * @param projRel the ProjectRel
     * 
     * @return the constructed RelNode tree
     */
    private ProjectRel addFilter(ProjectRel projRel)
    {
        RexBuilder rexBuilder = projRel.getCluster().getRexBuilder();
        
        RelNode filterRel =
            CalcRel.createFilter(
                projRel.getChild(),
                rexBuilder.makeLiteral(true));
        
        RelDataTypeField[] fields = projRel.getRowType().getFields();
        int nFields = fields.length;
        String[] fieldNames = new String[nFields];
        for (int i = 0; i < nFields; i++) {
            fieldNames[i] = fields[i].getName();
        }
        ProjectRel newProjRel =
            CalcRel.createProject(
                filterRel,
                projRel.getChildExps(),
                fieldNames);
        
        return newProjRel;       
    }
}

// End LoptModifyRemovableSelfJoinRule.java
