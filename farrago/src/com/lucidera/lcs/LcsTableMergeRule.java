/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
package com.lucidera.lcs;

import com.lucidera.query.*;

import java.util.*;

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.type.*;


/**
 * LcsTableMergeRule is a rule for converting an abstract {@link
 * TableModificationRel} into a corresponding {@link LcsTableMergeRel}.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class LcsTableMergeRule
    extends RelOptRule
{

    //~ Constructors -----------------------------------------------------------

    public LcsTableMergeRule()
    {
        super(
            new RelOptRuleOperand(
                TableModificationRel.class,
                new RelOptRuleOperand[] {
                    new RelOptRuleOperand(ProjectRel.class, null)
                }));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelRel.FENNEL_EXEC_CONVENTION;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        TableModificationRel tableModification =
            (TableModificationRel) call.rels[0];

        if (!(tableModification.getTable() instanceof LcsTable)) {
            return;
        }

        if (!tableModification.isFlattened()) {
            return;
        }

        if (!tableModification.isMerge()) {
            return;
        }

        ProjectRel origProj = (ProjectRel) call.rels[1];
        RexNode [] origProjExprs = origProj.getProjectExps();
        RelDataTypeField [] targetFields =
            tableModification.getTable().getRowType().getFields();
        int nTargetFields = targetFields.length;
        List<String> updateList = tableModification.getUpdateColumnList();
        boolean updateOnly =
            (updateList.size() > 0)
            && (origProjExprs.length == (nTargetFields + updateList.size()));
        boolean insertOnly = (origProjExprs.length == nTargetFields);
        assert (!(updateOnly && insertOnly));

        // create a rid expression on the target table
        RexBuilder rexBuilder = origProj.getCluster().getRexBuilder();      
        int nSourceFields =
            origProj.getChild().getRowType().getFieldCount() - nTargetFields;
        RexNode ridExpr =
            LucidDbSpecialOperators.makeRidExpr(
                rexBuilder,
                origProj.getChild(),
                nSourceFields);     
        
        // The merge source currently contains a projection with the
        // insert expressions followed by the target columns and the update
        // expressions (if the statement contains an UPDATE component).
        // Replace this with whatever is appropriate for the specific type of
        // MERGE statement.
        RelNode mergeSource;
        if (insertOnly) {
            mergeSource =
                createInsertSource(
                    origProj,
                    targetFields,
                    ridExpr,
                    rexBuilder);
        } else {
            mergeSource =
                createUpdateSource(
                    origProj,
                    targetFields,
                    updateList,
                    updateOnly,
                    ridExpr,
                    rexBuilder);
        }
      
        RelNode fennelInput =
            mergeTraitsAndConvert(
                call.rels[0].getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION,
                mergeSource);
        if (fennelInput == null) {
            return;
        }

        LcsTableMergeRel mergeRel =
            new LcsTableMergeRel(
                tableModification.getCluster(),
                (LcsTable) tableModification.getTable(),
                tableModification.getConnection(),
                fennelInput,
                tableModification.getOperation(),
                tableModification.getUpdateColumnList(),
                updateOnly);

        call.transformTo(mergeRel);
    }
    
    /**
     * Creates a RelNode that serves as the source for an insert-only MERGE.
     * A FilterRel is inserted underneath the current ProjectRel.
     * The filter removes rids that are non-null.
     * 
     * @param origProj original projection
     * @param targetFields fields from the target table
     * @param ridExpr expression representing the target rid column
     * @param rexBuilder rex builder
     * 
     * @return RelNode corresponding to the source for the insert-only MERGE
     */
    private RelNode createInsertSource(
        ProjectRel origProj,
        RelDataTypeField[] targetFields,
        RexNode ridExpr,
        RexBuilder rexBuilder)
    {
        // create a filter to select only rows where the rid is null
        RexNode isNullExpr =
            rexBuilder.makeCall(
                SqlStdOperatorTable.isNullOperator,
                ridExpr);
        RelNode filterRel =
            CalcRel.createFilter(origProj.getChild(), isNullExpr);
        
        // recreate the original projection, but make its child the newly
        // created FilterRel
        int nTargetFields = targetFields.length;
        RexNode [] projExprs = new RexNode[nTargetFields];
        String [] fieldNames = new String[nTargetFields];
        RexNode[] origProjExprs = origProj.getProjectExps();
        RelDataTypeField[] origProjFields = origProj.getRowType().getFields();
        for (int i = 0; i < nTargetFields; i++) {
            projExprs[i] = origProjExprs[i];
            fieldNames[i] = origProjFields[i].getName();
        }

        return
            CalcRel.createProject(filterRel, projExprs, fieldNames);
    }
    
    /**
     * Creates the source RelNode for a MERGE that contains an UPDATE
     * component.  The current ProjectRel is replaced by a FilterRel underneath
     * a new ProjectRel.  The filter removes rows where the columns are not
     * actually updated.  The new projection projects the target rid (and 2
     * nulls) followed by a set of expressions representing new insert rows.
     * 
     * @param origProj the original projection being replaced
     * @param targetFields fields from the target table
     * @param updateList list of names corresponding to the update columns
     * @param updateOnly if true, MERGE statement contains no INSERT
     * @param ridExpr expression representing the target rid column
     * @param rexBuilder rex builder
     * 
     * @return source RelNode for a MERGE that contains an UPDATE
     */
    private RelNode createUpdateSource(
        ProjectRel origProj,
        RelDataTypeField[] targetFields,
        List<String> updateList,
        boolean updateOnly,
        RexNode ridExpr,
        RexBuilder rexBuilder)
    {
        RexNode[] origProjExprs = origProj.getProjectExps();
        int nTargetFields = targetFields.length;
        
        // create a filter selecting only rows where any of the update
        // columns are different from their original column values; if there's
        // an insert component in the MERGE, then we also need to allow
        // rows where the target rid is null; note also that since the
        // expression comparing the original and new values doesn't handle
        // nulls, we need to also explicitly add checks for nulls
        List<RexNode> filterList = new ArrayList<RexNode>();
        if (!updateOnly) {
            createNullFilter(rexBuilder, ridExpr, filterList);
        }   
        
        int nInsertFields = (updateOnly) ? 0 : nTargetFields;
        for (int i = 0; i < updateList.size(); i++) {
            // find the original target column corresponding to the update
            // column
            int targetColno;
            for (targetColno = 0; targetColno < nTargetFields; targetColno++) {
                if (targetFields[targetColno].getName().equals(
                    updateList.get(i)))
                {
                    break;
                }
            }
            assert(targetColno < nTargetFields);
            
            // create null filters on the original and new values, accounting
            // for the case where both are null
            RexNode origValue = origProjExprs[nInsertFields + targetColno];
            RexNode newValue = origProjExprs[nInsertFields + nTargetFields + i];
            createNullFilterAndIsNotNullFilter(
                rexBuilder,
                origValue,
                newValue,
                filterList);           
            createNullFilterAndIsNotNullFilter(
                rexBuilder,
                newValue,
                origValue,
                filterList);
            
            // create the != expression, comparing the original value
            // against the new
            RexNode filter =
                rexBuilder.makeCall(
                    SqlStdOperatorTable.notEqualsOperator, origValue, newValue);
            filterList.add(filter);
        }
        RexNode nonUpdateFilter =
            RexUtil.orRexNodeList(rexBuilder, filterList);
        RelNode filterRel =
            CalcRel.createFilter(origProj.getChild(), nonUpdateFilter);
   
        // Project out the rid and 2 null columns as well as the
        // expressions that make up a new insert target row.  The content of
        // insert target row depends on whether the rid is null or non-null.
        // In the case of the former, it corresponds to the target of the
        // INSERT substatement while in the latter, it corresponds to the
        // UPDATE.  These will be implemented using a CASE expression.  If
        // only an UPDATE substatement is present, no CASE expression is
        // required.
        RexNode [] projExprs = new RexNode[nTargetFields + 3];
        String [] fieldNames = new String[nTargetFields + 3];
        projExprs[0] = ridExpr;
        RexNode nullLiteral =
            rexBuilder.makeNullLiteral(
                SqlTypeName.Varbinary,
                LcsIndexGuide.LbmBitmapSegMaxSize);
        projExprs[1] = nullLiteral;
        projExprs[2] = nullLiteral;
        fieldNames[0] = "rid";
        fieldNames[1] = "descriptor";
        fieldNames[2] = "segment";
        
        // when expression used in the case expression
        RexNode whenExpr = null;
        if (!updateOnly) {
            whenExpr =
                rexBuilder.makeCall(
                    SqlStdOperatorTable.isNullOperator,
                    ridExpr);
        }
        
        for (int i = 0; i < nTargetFields; i++) {
            RexNode updateExpr = null;
            // determine whether a target expression was specified for the
            // field in the UPDATE call
            int matchedSetExpr =
                updateList.indexOf(targetFields[i].getName());

            if (matchedSetExpr != -1) {
                updateExpr =
                    origProjExprs[nInsertFields + nTargetFields
                        + matchedSetExpr];
            } else {
                updateExpr = origProjExprs[nInsertFields + i];
            }

            if (updateOnly) {
                projExprs[i + 3] = updateExpr;
            } else {
                projExprs[i + 3] =
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.caseOperator,
                        new RexNode[] {
                            whenExpr,
                            origProjExprs[i],
                            updateExpr
                        });
            }
            fieldNames[i + 3] = targetFields[i].getName();
        }
        
        return CalcRel.createProject(filterRel, projExprs, fieldNames);
    }
    
    /**
     * Creates an is null expression on an expression and adds it to a list
     * of filters
     * 
     * @param rexBuilder rex builder
     * @param expr expression to create the is null expression on
     * @param filterList list of filters
     */
    private void createNullFilter(
        RexBuilder rexBuilder,
        RexNode expr,
        List<RexNode> filterList)
    {
        RexNode filter = rexBuilder.makeCall(
            SqlStdOperatorTable.isNullOperator,
            expr);
        filterList.add(filter);
    }
    
    /**
     * Creates an expression AND'ing together an IS NULL expression on one
     * expression and a IS NOT NULL expression on a second, and adds it to a
     * list of filter
     * 
     * @param rexBuilder rex builder
     * @param nullExpr expression that the IS NULL will be created on
     * @param notNullExpr expression that the IS NOT NULL expression will be
     * created on
     * @param filterList list of filters
     */
    private void createNullFilterAndIsNotNullFilter(
        RexBuilder rexBuilder,
        RexNode nullExpr,
        RexNode notNullExpr,
        List<RexNode> filterList)
    {
    	RexNode nullFilter = rexBuilder.makeCall(
            SqlStdOperatorTable.isNullOperator,
            nullExpr);
    	RexNode notNullFilter = rexBuilder.makeCall(
            SqlStdOperatorTable.isNotNullOperator,
            notNullExpr);
        RexNode filter = rexBuilder.makeCall(
            SqlStdOperatorTable.andOperator,
            nullFilter,
            notNullFilter);
    	
        filterList.add(filter);
    }
}

// End LcsTableMergeRule
