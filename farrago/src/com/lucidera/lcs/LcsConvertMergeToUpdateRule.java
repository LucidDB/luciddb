/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Copyright (C) 2005-2007 The Eigenbase Project
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

import java.util.*;

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.fun.*;


/**
 * LcsConvertMergeToUpdateRule is a rule for converting a {@link
 * TableModificationRel} corresponding to a MERGE statement, where the
 * source and target are the same table.  The resulting conversion avoids
 * the self-join on the target/source.  I.e., it treats the MERGE as if it
 * were an UPDATE on the target table.  In order to do the conversion, the
 * following conditions must be met:
 * <ol>
 * <li> The MERGE statement must not contain a WHEN NOT MATCHED clause.
 * <li> The equality join conditions in the ON clause of the MERGE statement
 * must be the same set of keys and must be unique.
 * </ol>
 * 
 * <p>Note that this rule is sensitive to the contents of the logical
 * RelNode tree corresponding to a MERGE statement.  Therefore, if any changes
 * are made to that tree, this rule will likely have to be modified
 * accordingly.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class LcsConvertMergeToUpdateRule
    extends RelOptRule
{
    // ~ Static fields/initializers --------------------------------------------
    
    public final static LcsConvertMergeToUpdateRule instanceRowScan =
        new LcsConvertMergeToUpdateRule(
            new RelOptRuleOperand(
                TableModificationRel.class,
                new RelOptRuleOperand(
                    ProjectRel.class,
                    new RelOptRuleOperand(
                        JoinRel.class,
                        new RelOptRuleOperand(LcsRowScanRel.class, ANY),
                        new RelOptRuleOperand(LcsRowScanRel.class, ANY)))),
             "row scan only");
    
    // Note that this pattern may not match given that there normally is
    // always a projection if the source has a filter.  But it's included
    // for completeness, in case it should occur in the future.
    public final static LcsConvertMergeToUpdateRule instanceFilterScan =
        new LcsConvertMergeToUpdateRule(
            new RelOptRuleOperand(
                TableModificationRel.class,
                new RelOptRuleOperand(
                    ProjectRel.class,
                    new RelOptRuleOperand(
                        JoinRel.class,
                        new RelOptRuleOperand(
                            FilterRel.class,
                            new RelOptRuleOperand(LcsRowScanRel.class, ANY)),
                        new RelOptRuleOperand(LcsRowScanRel.class, ANY)))),
             "filter and scan");
    
    public final static LcsConvertMergeToUpdateRule instanceProjectScan =
        new LcsConvertMergeToUpdateRule(
            new RelOptRuleOperand(
                TableModificationRel.class,
                new RelOptRuleOperand(
                    ProjectRel.class,
                    new RelOptRuleOperand(
                        JoinRel.class,
                        new RelOptRuleOperand(
                            ProjectRel.class,
                            new RelOptRuleOperand(LcsRowScanRel.class, ANY)),
                        new RelOptRuleOperand(LcsRowScanRel.class, ANY)))),
             "project and scan");
    
    public final static LcsConvertMergeToUpdateRule instanceProjectFilterScan =
        new LcsConvertMergeToUpdateRule(
            new RelOptRuleOperand(
                TableModificationRel.class,
                new RelOptRuleOperand(
                    ProjectRel.class,
                    new RelOptRuleOperand(
                        JoinRel.class,
                        new RelOptRuleOperand(
                            ProjectRel.class,
                            new RelOptRuleOperand(
                                FilterRel.class,
                                new RelOptRuleOperand(
                                    LcsRowScanRel.class,
                                    ANY))),
                        new RelOptRuleOperand(LcsRowScanRel.class, ANY)))),
             "project, filter, and scan");
    
    //~ Constructors -----------------------------------------------------------

    public LcsConvertMergeToUpdateRule(RelOptRuleOperand operand, String id)
    {
        super(operand);
        description = "LcsConvertMergeToUpdateRule: " + id;
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
        TableModificationRel origTableModRel =
            (TableModificationRel) call.rels[0];
        JoinRel joinRel = (JoinRel) call.rels[2];
        
        // Extract the source and target based on the pattern that matched
        // this rule.
        RelNode source = call.rels[3];
        ProjectRel sourceProject = null;
        FilterRel sourceFilter = null;
        LcsRowScanRel sourceTable;
        LcsRowScanRel targetTable;
        if (source instanceof LcsRowScanRel) {
            sourceTable = (LcsRowScanRel) source;
            targetTable = (LcsRowScanRel) call.rels[4];
        } else if (source instanceof FilterRel) {
            sourceFilter = (FilterRel) source;
            sourceTable = (LcsRowScanRel) call.rels[4];
            targetTable = (LcsRowScanRel) call.rels[5];
        }  else {
            sourceProject = (ProjectRel) source;
            if (call.rels[4] instanceof LcsRowScanRel) {
                sourceTable = (LcsRowScanRel) call.rels[4];
                targetTable = (LcsRowScanRel) call.rels[5];
            } else {
                sourceFilter = (FilterRel) call.rels[4];
                sourceTable = (LcsRowScanRel) call.rels[5];
                targetTable = (LcsRowScanRel) call.rels[6];
            }
        }
        
        // Determine if pre-conditions for conversion are met
        RexNode extraFilters =
            checkPreConditions(
                origTableModRel,
                joinRel,
                sourceTable,
                targetTable);
        if (extraFilters == null) {
            return;
        }

        // Pre-conditions have been met.  Replace the JoinRel with a
        // ProjectRel.
        int nTargetCols = targetTable.getRowType().getFieldCount();
        int nSourceCols =
            (sourceProject == null) 
                ? nTargetCols
                : sourceProject.getProjectExps().length;
        RelNode joinReplacement = 
            createJoinReplacement(
                sourceTable,
                targetTable,
                sourceProject,
                nSourceCols,
                nTargetCols);
        
        // Place the residual filters from the join condition on top of
        // the project just created along with any filters from the source.       
        if (!extraFilters.isAlwaysTrue() || sourceFilter != null) {
            joinReplacement = 
                createReplacementFilter(
                    sourceTable,
                    targetTable,
                    sourceProject,
                    sourceFilter,
                    extraFilters,
                    joinReplacement,
                    nSourceCols,
                    nTargetCols);
        }
        
        // Put the original project on top of that.
        ProjectRel origProjRel = (ProjectRel) call.rels[1];
        RelNode newProjRel = 
            CalcRel.createProject(
                joinReplacement,
                origProjRel.getProjectExps(),
                null,
                true);
        
        // Finally, put the original TableModificationRel on top of that.
        RelNode newTableModRel =
            new TableModificationRel(
                origTableModRel.getCluster(),
                origTableModRel.getTable(),
                origTableModRel.getConnection(),
                newProjRel,
                TableModificationRel.Operation.MERGE,
                origTableModRel.getUpdateColumnList(),
                origTableModRel.isFlattened());
        
        call.transformTo(newTableModRel);
    }
    
    /**
     * Checks if all pre-conditions required for the conversion are met.
     * If they are, extracts from the ON condition in the join source those
     * filters that will need to be applied on the remaining row scan after
     * the join is replaced.
     * 
     * @param origTableModRel the original TableModificationRel
     * @param joinRel the join that provides the source for the merge
     * @param sourceTable the source table
     * @param targetTable the target table
     * 
     * @return null if the pre-conditions are met; otherwise, returns a
     * filter corresponding to excessive filters that will need to be applied
     * on the remaining row scan
     */
    private RexNode checkPreConditions(
        TableModificationRel origTableModRel,
        JoinRel joinRel,
        LcsRowScanRel sourceTable,
        LcsRowScanRel targetTable)
    {
        // Make sure this is a MERGE statement
        if (origTableModRel.getOperation() !=
            TableModificationRel.Operation.MERGE)
        {
            return null;
        }
        
        // If there is no WHEN NOT MATCHED clause, the join must be an inner
        // join.
        if (joinRel.getJoinType() != JoinRelType.INNER) {
            return null;
        }
        
        // Make sure both row scans are on the same table
        if (!Arrays.equals(
            sourceTable.lcsTable.getQualifiedName(),
            targetTable.lcsTable.getQualifiedName()))
        {
            return null;
        }
        
        // Locate the equality join conditions in the ON clause.  Determine
        // if the keys from the source map back to the same keys from the
        // target.
        List<Integer> leftKeys = new ArrayList<Integer>();
        List<Integer> rightKeys = new ArrayList<Integer>();
        RelNode source = joinRel.getInput(0);
        RexNode extraFilters =
            RelOptUtil.splitJoinCondition(
                source,
                targetTable,
                joinRel.getCondition(),
                leftKeys,
                rightKeys);
        int keyOffset = 0;
        for (Integer key : leftKeys) {
            Set<RelColumnOrigin> colOrigin =
                RelMetadataQuery.getColumnOrigins(
                    source,
                    key);
            if ((colOrigin == null) || (colOrigin.size() != 1)) {
                return null;
            }
            RelColumnOrigin [] coList =
                (RelColumnOrigin []) colOrigin.toArray(
                    new RelColumnOrigin[1]);
            if (coList[0].isDerived()) {
                return null;
            }
            if (coList[0].getOriginColumnOrdinal() != rightKeys.get(keyOffset))
            {
                return null;
            }
            keyOffset++;
        }
            
        // Now that we've verified that the keys are the same, see if the
        // keys from the target are unique.
        if (!RelMdUtil.areColumnsDefinitelyUnique(
            targetTable,
            RelMdUtil.setBitKeys(rightKeys)))
        {
            return null;
        }
        
        // splitJoinCondition returns a true literal even if there are no
        // real excess filters
        assert(extraFilters != null);
        
        // Even if the keys are part of a unique key, if any are nullable,
        // then those are excess keys that really aren't unique.  So, add
        // them back in as IS NOT NULL filters.
        RelDataTypeField[] fields = targetTable.getRowType().getFields();
        RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
        List<RexNode> extraFilterList = new ArrayList<RexNode>();       
        for (Integer key : leftKeys) {
            if (fields[key].getType().isNullable()) {
                RexNode[] expr =
                    new RexNode[]
                        { RelOptUtil.createInputRef(targetTable, key) };
                extraFilterList.add(
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.isNotNullOperator, expr));
            }
        }
        if (extraFilterList.isEmpty()) {
            return extraFilters;
        } else {
            if (!extraFilters.isAlwaysTrue()) {
                extraFilterList.add(extraFilters);
            }
            return RexUtil.andRexNodeList(rexBuilder, extraFilterList);
        }
    }
    
    /**
     * Creates a ProjectRel to replace the JoinRel in the MERGE tree.
     * The ProjectRel projects each column from the source, followed
     * by each column from the target, so it looks like it's the result of a
     * join between the source and target.
     * 
     * @param sourceTable the source table
     * @param targetTable the target table
     * @param sourceProject the project on top of the source table, if the
     * source is projected
     * @param nSourceCols the number of columns in the source
     * @param nTargetCols the number of columns in the target table
     * 
     * @return the constructed ProjectRel
     */
    private RelNode createJoinReplacement(
        LcsRowScanRel sourceTable,
        LcsRowScanRel targetTable,
        ProjectRel sourceProject,
        int nSourceCols,
        int nTargetCols)
    {
        RexNode[] joinReplacementExprs =
            new RexNode[nSourceCols + nTargetCols];
        if (sourceProject == null) {
            for (int i = 0; i < nTargetCols; i++) {
                joinReplacementExprs[i] =
                    RelOptUtil.createInputRef(targetTable, i);
            }
        } else {
            for (int i = 0; i < nSourceCols; i++) {
                joinReplacementExprs[i] = sourceProject.getProjectExps()[i];
            }
        }
        for (int i = 0; i < nTargetCols; i++) {
            joinReplacementExprs[nSourceCols + i] =
                RelOptUtil.createInputRef(targetTable, i);
        }
        
        return
            CalcRel.createProject(
                targetTable,
                joinReplacementExprs,
                null,
                true);
    }
    
    /**
     * Creates a FilterRel that contains additional filters that need to
     * be applied now that the JoinRel has been removed.  This includes
     * the non-equality/non-unique key filters from the join condition as
     * well as filters from the source.
     * 
     * @param sourceTable the source table
     * @param targetTable the target table
     * @param sourceProject the project on top of the source table, if it is
     * projected
     * @param sourceFilter the filter on top of the source, if it is being
     * filtered
     * @param extraJoinFilters excess filters from the join
     * @param filterSource the RelNode that will be the source for the new
     * FilterRel
     * @param nSourceCols the number of columns in the source
     * @param nTargetCols the number of columns in the target table
     * 
     * @return the constructed FilterRel
     */
    private RelNode createReplacementFilter(
        LcsRowScanRel sourceTable,
        LcsRowScanRel targetTable,
        ProjectRel sourceProject,
        FilterRel sourceFilter,
        RexNode extraJoinFilters,
        RelNode filterSource,
        int nSourceCols,
        int nTargetCols)
    {
        RexNode sourceFilterCond = null;
        RexBuilder rexBuilder = sourceTable.getCluster().getRexBuilder();
        if (sourceFilter != null) {
            sourceFilterCond = sourceFilter.getCondition();
            // The column offsets in the filters from the source need to be
            // mapped to the target table if the source is projected
            // after it's filtered.
            if (sourceProject != null) {
                int [] adjustments = new int[nTargetCols];
                for (int i = 0; i < nTargetCols; i++) {
                    adjustments[i] = nSourceCols;
                }
                sourceFilterCond =
                    sourceFilterCond.accept(
                        new RelOptUtil.RexInputConverter(
                            rexBuilder,
                            sourceTable.getRowType().getFields(),
                            adjustments));
            }
            if (extraJoinFilters.isAlwaysTrue()) {
                extraJoinFilters = sourceFilterCond;
            } else {
                extraJoinFilters = 
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.andOperator,
                        extraJoinFilters,
                        sourceFilterCond);
            }
        }
        return
            CalcRel.createFilter(filterSource, extraJoinFilters);
    }
}

// End LcsConvertMergeToUpdateRule.java
