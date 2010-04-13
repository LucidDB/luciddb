/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2010 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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
package org.luciddb.lcs;

import org.luciddb.session.*;

import java.util.*;
import java.util.logging.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.keysindexes.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.query.*;
import net.sf.farrago.trace.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.resource.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.fun.*;


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
    //~ Static fields/initializers ---------------------------------------------

    private static final Logger tracer = FarragoTrace.getOptimizerRuleTracer();

    /**
     * The upper bound on the percentage of columns that must be updated for the
     * replace column optimization to be used
     */
    private static final double COLUMN_UPDATE_THRESHOLD = .6;

    /**
     * The lower bound on the percentage of rows that must be updated for the
     * replace column optimization to be used. Note that this percentage will be
     * multiplied by the percentage of columns updated by the statement, so the
     * threshold is lower when fewer columns are updated.
     */
    private static final double ROW_UPDATE_THRESHOLD = .4;

    public static final LcsTableMergeRule instance =
        new LcsTableMergeRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a LcsTableMergeRule.
     */
    private LcsTableMergeRule()
    {
        super(
            new RelOptRuleOperand(
                TableModificationRel.class,
                new RelOptRuleOperand(
                    ProjectRel.class,
                    new RelOptRuleOperand(JoinRel.class, ANY))));
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

        List<FemLocalIndex> updateClusters =
            shouldReplaceColumns(
                (LcsTable) tableModification.getTable(),
                (JoinRel) call.rels[2],
                updateList,
                updateOnly);

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
                    (updateClusters != null),
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
                updateOnly,
                updateClusters);

        call.transformTo(mergeRel);
    }

    /**
     * Determines whether the merge should be executed by replacing the columns
     * being updated, as opposed to updating individual rows. This is only
     * feasible if:
     *
     * <ol>
     * <li>The underlying personality supports snapshots.</li>
     * <li>The merge statement only contains an update substatement.</li>
     * <li>The columns being updated all correspond to single column
     * clusters.</li>
     * <li>The keys in the ON condition are unique.</li>
     * <li>The number of columns being updated is less than some threshold.</li>
     * <li>The percentage of rows being updated is greater than a threshold that
     * also depends on the percentage of columns updated.</li>
     * </ol>
     *
     * @param target the target table
     * @param source the source for the merge
     * @param updateCols list of columns being updated
     * @param updateOnly true if the statement only contains an update
     * substatement
     *
     * @return list of clusters corresponding to the columns being updated if
     * the criteria are met; otherwise, null
     */
    List<FemLocalIndex> shouldReplaceColumns(
        LcsTable target,
        JoinRel source,
        List<String> updateCols,
        boolean updateOnly)
    {
        if (!target.getPreparingStmt().getSession().getPersonality()
            .supportsFeature(
                   EigenbaseResource.instance().PersonalitySupportsSnapshots))
        {
            return null;
        }

        if (!updateOnly) {
            return null;
        }

        double percentColsUpdated =
            ((double) updateCols.size()) / target.getRowType().getFieldCount();
        if (percentColsUpdated > COLUMN_UPDATE_THRESHOLD) {
            return null;
        }

        // Validate that the join keys are unique
        if (!checkUniqueJoinKeys(source)) {
            return null;
        }

        FarragoRepos repos = target.getPreparingStmt().getRepos();
        Double nSourceRows = RelMetadataQuery.getRowCount(source);
        Double nTargetRows =
            FarragoRelMetadataProvider.getRowCountStat(
                target,
                repos);
        if ((nSourceRows == null) || (nTargetRows == null)) {
            return null;
        }
        double percentRowsUpdated = nSourceRows / nTargetRows;

        // Make sure the percentage of rows is at least 1 so we avoid the
        // optimization when a small percentage of rows are updated.
        if (percentRowsUpdated < .01) {
            return null;
        }

        // By multiplying the row threshold by the percentage of columns
        // updated, this means that when fewer columns are updated, not as many
        // rows need to be updated to trigger the optimization.
        if (percentRowsUpdated < (ROW_UPDATE_THRESHOLD * percentColsUpdated)) {
            return null;
        }

        List<FemLocalIndex> updateClusters =
            checkSingleColClusters(updateCols, target, repos);

        if (updateClusters != null) {
            tracer.fine(
                "Replace columns optimization used for MERGE on target table "
                + target.getName());
        }
        return updateClusters;
    }

    /**
     * Determines if the join keys corresponding to the source for the MERGE
     * are unique.  Rid expressions are considered to be unique keys.
     *
     * @param joinRel the join
     *
     * @return true if the join keys are unique
     */
    private boolean checkUniqueJoinKeys(JoinRel joinRel)
    {
        List<RexNode> leftKeyExprs = new ArrayList<RexNode>();
        List<RexNode> rightKeyExprs = new ArrayList<RexNode>();
        RelOptUtil.splitJoinCondition(
            Collections.<RelDataTypeField>emptyList(),
            joinRel.getInput(0),
            joinRel.getInput(1),
            joinRel.getCondition(),
            leftKeyExprs,
            rightKeyExprs,
            null,
            null);
        if (leftKeyExprs.size() == 0) {
            return false;
        }

        // The source is always on the LHS of the join, so we only need to
        // check those keys.  Do so by creating a projection of the join
        // keys and checking the projection for uniqueness.
        RelNode project = CalcRel.createProject(
            joinRel.getInput(0),
            leftKeyExprs,
            null);
        BitSet leftKeys = new BitSet();
        RelOptUtil.setRexInputBitmap(leftKeys, 0, leftKeyExprs.size());
        // If the keys are unique, that ensures that at most one source row
        // joins with each target row.  Since nulls will be filtered out by the
        // join condition, it's ok if there are nulls in the source join keys.
        if (!RelMdUtil.areColumnsDefinitelyUniqueWhenNullsFiltered(
                project,
                leftKeys))
        {
            return false;
        }

        return true;
    }

    /**
     * Determines if each of the columns from a list of columns being updated
     * all belong to clusters containing only a single column.
     *
     * @param updateCols the list of columns being updated
     * @param table the target table
     * @param repos repository
     *
     * @return the clusters corresponding to the columns being updated in an
     * order matching the update columns, provided the columns are all part of
     * single-column clusters; otherwise, null is returned
     */
    private List<FemLocalIndex> checkSingleColClusters(
        List<String> updateCols,
        LcsTable table,
        FarragoRepos repos)
    {
        List<FemLocalIndex> updateClusters = new ArrayList<FemLocalIndex>();

        // Build a map, mapping each column ordinal to its corresponding
        // cluster.
        List<FemLocalIndex> clusteredIndexes =
            FarragoCatalogUtil.getClusteredIndexes(
                repos,
                table.getCwmColumnSet());
        Map<Integer, FemLocalIndex> colOrdToClusterMap =
            new HashMap<Integer, FemLocalIndex>();
        for (FemLocalIndex cluster : clusteredIndexes) {
            for (CwmIndexedFeature indexedFeature
                : cluster.getIndexedFeature())
            {
                FemAbstractColumn column =
                    (FemAbstractColumn) indexedFeature.getFeature();
                colOrdToClusterMap.put(column.getOrdinal(), cluster);
            }
        }

        // Determine if each column being updated is part of a cluster
        // containing only a single column
        for (String colName : updateCols) {
            int colOrdinal = table.getRowType().getFieldOrdinal(colName);
            FemLocalIndex cluster = colOrdToClusterMap.get(colOrdinal);
            if (cluster.getIndexedFeature().size() == 1) {
                updateClusters.add(cluster);
            } else {
                return null;
            }
        }

        return updateClusters;
    }

    /**
     * Creates a RelNode that serves as the source for an insert-only MERGE. A
     * FilterRel is inserted underneath the current ProjectRel. The filter
     * removes rids that are non-null.
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
        RelDataTypeField [] targetFields,
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
        RexNode [] origProjExprs = origProj.getProjectExps();
        RelDataTypeField [] origProjFields = origProj.getRowType().getFields();
        for (int i = 0; i < nTargetFields; i++) {
            projExprs[i] = origProjExprs[i];
            fieldNames[i] = origProjFields[i].getName();
        }

        return CalcRel.createProject(filterRel, projExprs, fieldNames);
    }

    /**
     * Creates the source RelNode for a MERGE that contains an UPDATE component.
     * The current ProjectRel is replaced by a FilterRel underneath a new
     * ProjectRel. The filter removes rows where the columns are not actually
     * updated.
     *
     * <p>The new projection projects the target rid followed by a set of
     * expressions representing new insert rows, or in the case where columns
     * are being replaced, the replaced column values.
     *
     * @param origProj the original projection being replaced
     * @param targetFields fields from the target table
     * @param updateList list of names corresponding to the update columns
     * @param updateOnly if true, MERGE statement contains no INSERT
     * @param replaceColumns true if the MERGE will be executed by replacing
     * entire columns
     * @param ridExpr expression representing the target rid column
     * @param rexBuilder rex builder
     *
     * @return source RelNode for a MERGE that contains an UPDATE
     */
    private RelNode createUpdateSource(
        ProjectRel origProj,
        RelDataTypeField [] targetFields,
        List<String> updateList,
        boolean updateOnly,
        boolean replaceColumns,
        RexNode ridExpr,
        RexBuilder rexBuilder)
    {
        RexNode [] origProjExprs = origProj.getProjectExps();
        int nTargetFields = targetFields.length;
        int nInsertFields = (updateOnly) ? 0 : nTargetFields;

        RelNode child;

        // create a filter selecting only rows where any of the update
        // columns are different from their original column values; if
        // there's an insert component in the MERGE, then we also need
        // to allow rows where the target rid is null; note also that
        // since the expression comparing the original and new values
        // doesn't handle nulls, we need to also explicitly add checks
        // for nulls
        child =
            createChangeFilterRel(
                origProj,
                targetFields,
                updateList,
                updateOnly,
                ridExpr,
                rexBuilder,
                nInsertFields);

        // Project out the rid column as well as the expressions that make up
        // a new target row or target columns.
        //
        // In the case where entire rows are being inserted, the content of
        // insert target row depends on whether the rid is null or non-null.
        // In the case of the former, it corresponds to the target of the
        // INSERT substatement while in the latter, it corresponds to the
        // UPDATE.  These will be implemented using a CASE expression.  If
        // only an UPDATE substatement is present, no CASE expression is
        // required.
        //
        // In the case where entire columns are being replaced, the target
        // columns simply contain the update expressions.
        int nProjExprs =
            ((replaceColumns) ? updateList.size() : nTargetFields) + 1;
        RexNode [] projExprs = new RexNode[nProjExprs];
        String [] fieldNames = new String[nProjExprs];
        projExprs[0] = ridExpr;
        fieldNames[0] = "rid";

        // when expression used in the case expression
        RexNode whenExpr = null;
        if (!updateOnly) {
            whenExpr =
                rexBuilder.makeCall(
                    SqlStdOperatorTable.isNullOperator,
                    ridExpr);
        }

        for (int i = 0; i < (nProjExprs - 1); i++) {
            RexNode updateExpr = null;

            if (replaceColumns) {
                updateExpr = origProjExprs[nTargetFields + i];
                fieldNames[i + 1] = updateList.get(i);
            } else {
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
                fieldNames[i + 1] = targetFields[i].getName();
            }

            if (updateOnly) {
                projExprs[i + 1] = updateExpr;
            } else {
                projExprs[i + 1] =
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.caseOperator,
                        whenExpr,
                        origProjExprs[i],
                        updateExpr);
            }
        }

        return CalcRel.createProject(child, projExprs, fieldNames);
    }

    /**
     * Creates the filter expression used to detect changed rows. The expression
     * looks like (ROW(O1, O2, ...) IS DISTINCT FROM ROW(N1, N2, ...)) where the
     * O references are the old values and the N references are the new values.
     * We rely on a custom implementation of the row-wise IS DISTINCT FROM
     * operator in the Java calc to evaluate this efficiently and without
     * exceeding the Java calc limit on method bytecode size. (As opposed to
     * {@link RelOptUtil#isDistinctFrom}, which generates a deeply nested tree.)
     *
     * @param origProj the original projection being replaced
     * @param targetFields fields from the target table
     * @param updateList list of names corresponding to the update columns
     * @param updateOnly if true, MERGE statement contains no INSERT
     * @param ridExpr expression representing the target rid column
     * @param rexBuilder rex builder
     * @param nInsertFields number of fields in INSERT portion of UPSERT
     *
     * @return filter rel
     */
    private RelNode createChangeFilterRel(
        ProjectRel origProj,
        RelDataTypeField [] targetFields,
        List<String> updateList,
        boolean updateOnly,
        RexNode ridExpr,
        RexBuilder rexBuilder,
        int nInsertFields)
    {
        RexNode [] origProjExprs = origProj.getProjectExps();
        List<RexNode> filterList = new ArrayList<RexNode>();
        if (!updateOnly) {
            createNullFilter(rexBuilder, ridExpr, filterList);
        }

        int nTargetFields = targetFields.length;

        List<RexNode> oldVals = new ArrayList<RexNode>();
        List<RexNode> newVals = new ArrayList<RexNode>();

        Map<String, Integer> targetColnoMap = new HashMap<String, Integer>();
        for (int i = 0; i < nTargetFields; i++) {
            targetColnoMap.put(targetFields[i].getName(), i);
        }
        for (int i = 0; i < updateList.size(); i++) {
            // find the original target column corresponding to the update
            // column
            Integer targetColno = targetColnoMap.get(updateList.get(i));
            assert (targetColno != null);

            // build up row lists
            RexNode origValue = origProjExprs[nInsertFields + targetColno];
            RexNode newValue = origProjExprs[nInsertFields + nTargetFields + i];

            if (newValue.getType() != origValue.getType()) {
                // comparison has to be done on result of cast from new value
                // to type of original value (which allows nulls, due to outer
                // join, even if target field does not)
                newValue =
                    rexBuilder.makeCast(
                        origValue.getType(),
                        newValue);
            }

            oldVals.add(origValue);
            newVals.add(newValue);
        }

        RexNode oldRow =
            rexBuilder.makeCall(
                SqlStdOperatorTable.rowConstructor,
                oldVals);
        RexNode newRow =
            rexBuilder.makeCall(
                SqlStdOperatorTable.rowConstructor,
                newVals);
        RexNode distinctTest =
            rexBuilder.makeCall(
                SqlStdOperatorTable.isDifferentFromOperator,
                oldRow,
                newRow);
        filterList.add(distinctTest);
        RexNode nonUpdateFilter = RexUtil.orRexNodeList(rexBuilder, filterList);

        RelNode filterRel =
            CalcRel.createFilter(origProj.getChild(), nonUpdateFilter);
        return filterRel;
    }

    /**
     * Creates an is null expression on an expression and adds it to a list of
     * filters
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
        RexNode filter =
            rexBuilder.makeCall(
                SqlStdOperatorTable.isNullOperator,
                expr);
        filterList.add(filter);
    }
}

// End LcsTableMergeRule.java
