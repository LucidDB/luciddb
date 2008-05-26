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

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sarg.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.stat.*;
import org.eigenbase.util14.*;


// REVIEW jvs 19-Apr-2006:  watch out for division-by-zero everywhere;
// may need some utilities to help with this.  I think Broadbase
// always bumped zero up to one instead of worrying about it.

// REVIEW jvs 19-Apr-2006:  for LucidDB, we should probably take
// into account the cost of CalcExecStream (Broadbase didn't
// because it was part of each XO).

/**
 * LoptMetadataProvider supplies relational expression metadata specific to
 * LucidDB.
 *
 * <p>NOTE jvs 10-Apr-2006: For now, I just created one class; if it gets too
 * big we can split it up like the ones in {@link org.eigenbase.rel.metadata},
 * e.g. add LoptRelMdCost.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LoptMetadataProvider
    extends ReflectiveRelMetadataProvider
{
    //~ Instance fields --------------------------------------------------------

    private FarragoRepos repos;

    private final LcsColumnMetadata columnMd;

    private final SimpleColumnOrigins columnOrigins;

    //~ Constructors -----------------------------------------------------------

    public LoptMetadataProvider(FarragoRepos repos)
    {
        this.repos = repos;
        columnMd = new LcsColumnMetadata();
        columnOrigins = new SimpleColumnOrigins();

        mapParameterTypes(
            "getCostWithFilters",
            Collections.singletonList((Class) RexNode.class));

        List<Class> args = new ArrayList<Class>();
        args.add((Class) BitSet.class);
        args.add((Class) RexNode.class);
        mapParameterTypes("getDistinctRowCount", args);

        mapParameterTypes(
            "getSelectivity",
            Collections.singletonList((Class) RexNode.class));

        mapParameterTypes(
            "getPopulationSize",
            Collections.singletonList((Class) BitSet.class));

        mapParameterTypes(
            "getSimpleColumnOrigins",
            Collections.singletonList((Class) Integer.TYPE));

        mapParameterTypes(
            "areColumnsUnique",
            Collections.singletonList((Class) BitSet.class));
        
        mapParameterTypes(
            "isVisibleInExplain",
            Collections.singletonList((Class) SqlExplainLevel.class));
    }

    //~ Methods ----------------------------------------------------------------

    // override the default; LucidDB computes cumulative cost in terms
    // of CostWithFilters
    public RelOptCost getCumulativeCost(RelNode rel)
    {
        Double result = LoptMetadataQuery.getCostWithFilters(rel, null);
        if (result != null) {
            return rel.getCluster().getPlanner().makeCost(result, 0, 0);
        } else {
            return null;
        }
    }

    // catch-all:  default cost is row count plus sum of cumulative
    // costs of inputs
    public Double getCostWithFilters(RelNode rel, RexNode filter)
    {
        return computeDefaultCostWithFilters(rel, filter, true);
    }

    private Double computeDefaultCostWithFilters(
        RelNode rel,
        RexNode filter,
        boolean includeRel)
    {
        Double result = 0.0;
        if (includeRel) {
            Double selfCost = RelMetadataQuery.getRowCount(rel);
            if (selfCost == null) {
                return null;
            }
            result += selfCost;
        }
        for (RelNode child : rel.getInputs()) {
            RelOptCost childCost = RelMetadataQuery.getCumulativeCost(child);
            if (childCost == null) {
                return null;
            }
            result += childCost.getRows();
        }
        return result;
    }

    public Double getCostWithFilters(LcsRowScanRel rel, RexNode filter)
    {
        double sargableSelectivity =
            estimateRowScanSelectivity(rel, filter, true);
        double inputSelectivity = rel.getInputSelectivity();
        sargableSelectivity *= inputSelectivity;

        Double nRowsInTable = RelMetadataQuery.getRowCount(rel);
        if (nRowsInTable == null) {
            return null;
        }
        
        // The selectivity of the rowscan's inputs are reflected in
        // "inputSelectivity," and the rowCount on the table reflects that
        // selectivity.  However, for the purpose of this method, nRowsInTable
        // is supposed to be the original, unfiltered rowCount.  Therefore,
        // we need to divide out inputSelectivity from that rowCount.
        nRowsInTable /= inputSelectivity;

        double sargableRowCount = sargableSelectivity * nRowsInTable;

        // NOTE jvs 19-Apr-2006: This holy formula is preserved straight from
        // Broadbase.  It is the geometric average of the number of rows in the
        // table and the number of rows which will be produced by the row scan
        // ExecStream.  Consider a million-row table.  If we're going to
        // produce all the rows, then the cost is one million, on the
        // assumption that the optimizer won't bother with the extra cost of
        // index lookup.  If we're only going to produce one row, then the cost
        // is one thousand (not one), the idea being that we'll have to do some
        // work (e.g.  bitmap intersection) to identify that row.  This would
        // be a bad assumption for OLTP index lookup, but it's reasonable for
        // analytic queries.  If we're going to produce 10% of the rows, the
        // cost works out to about a third of a million.  Sargable filters are
        // of interest regardless of whether they are evaluated via an
        // unclustered index because we can handle the residuals during
        // clustered index scan.  For our purposes here, semijoin selectivity
        // is rolled into sargableSelectivity since it lets us skip rows.
        // Non-sargable filters are NOT taken into account here because they
        // have to be handled via a calculator on top, so they don't reduce the
        // cost of the row scan itself in any way.
        return Math.sqrt(nRowsInTable * sargableRowCount);
    }

    public Double getCostWithFilters(UnionRel rel, RexNode filter)
    {
        // NOTE jvs 17-Apr-2006: UNION should already have been translated to
        // UNION ALL, so ignore the flag.  We pass includeRel=false because
        // LucidDB executes UNION ALL via block-level pass-by-reference rather
        // than row-by-row copying.
        return computeDefaultCostWithFilters(rel, filter, false);
    }

    public Double getCostWithFilters(FilterRel rel, RexNode filter)
    {
        // TODO jvs 19-Apr-2006: come up with more consistent nomenclature for
        // andJoinFilters, unionPreds, and decompCF.  And maybe use unionPreds
        // here?
        RexNode combinedFilter =
            RelOptUtil.andJoinFilters(
                rel.getCluster().getRexBuilder(),
                filter,
                rel.getCondition());
        return LoptMetadataQuery.getCostWithFilters(
            rel.getChild(),
            combinedFilter);
    }

    public Double getCostWithFilters(SemiJoinRel rel, RexNode filter)
    {
        RexNode selectivityNode = RelMdUtil.makeSemiJoinSelectivityRexNode(rel);
        RexNode combinedFilter =
            RelOptUtil.andJoinFilters(
                rel.getCluster().getRexBuilder(),
                filter,
                selectivityNode);
        Double leftCost =
            LoptMetadataQuery.getCostWithFilters(
                rel.getLeft(),
                combinedFilter);
        if (leftCost == null) {
            return null;
        }

        // TODO jvs 19-Apr-2006: once we can buffer the RHS and reuse it via a
        // tee into both the row scan and the join, don't add on its cost here,
        // since the join will already account for it (but still need to add the
        // sorting overhead)
        RelOptCost rightCost =
            LoptMetadataQuery.getCumulativeCost(
                rel.getRight());
        if (rightCost == null) {
            return null;
        }

        // Overhead for sort+agg in semijoin physical implementation
        Double rightRowCount = LoptMetadataQuery.getRowCount(rel.getRight());
        if (rightRowCount == null) {
            return null;
        }

        return leftCost + rightCost.getRows() + rightRowCount;
    }

    public Double getCostWithFilters(ProjectRel rel, RexNode filter)
    {
        if (filter == null) {
            RelOptCost cost =
                LoptMetadataQuery.getCumulativeCost(
                    rel.getChild());
            if (cost == null) {
                return null;
            }
            return cost.getRows();
        }

        // NOTE jvs 19-Apr-2006:  I borrowed most of this sequence
        // for combining a filter on top of a project from
        // MergeFilterOntoCalcRule.

        RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
        RexProgram bottomProgram =
            RexProgram.create(
                rel.getChild().getRowType(),
                rel.getProjectExps(),
                null,
                rel.getRowType(),
                rexBuilder);

        RexProgramBuilder topProgramBuilder =
            new RexProgramBuilder(
                rel.getRowType(),
                rexBuilder);
        topProgramBuilder.addIdentity();
        topProgramBuilder.addCondition(filter);
        RexProgram topProgram = topProgramBuilder.getProgram();

        RexProgram mergedProgram =
            RexProgramBuilder.mergePrograms(
                topProgram,
                bottomProgram,
                rexBuilder);

        RexNode expandedFilter =
            mergedProgram.expandLocalRef(
                mergedProgram.getCondition());

        return LoptMetadataQuery.getCostWithFilters(
            rel.getChild(),
            expandedFilter);
    }

    public Double getCostWithFilters(JoinRel rel, RexNode filter)
    {
        Double result = computeDefaultCostWithFilters(rel, filter, false);
        if (result == null) {
            return null;
        }

        // Beyond the cost of inputs, we add on the join's output row count
        // times a factor which models the cost of the join; this factor is
        // computed by a formula that favors asymmetry between input sizes,
        // since hash join does better in that case (only the build side needs
        // to fit in memory).  This is bogus in cases where we can't even use
        // hash join so we special case cartesian product joins and joins
        // that can't be fully processed using hash joins.  Anyway, the
        // factor works out to 10 in those non-hash-joinable cases or if
        // the two inputs are of equal size.  It decreases exponentially down to
        // to an asymptote of 1 representing one side being infinitely larger
        // than the other.  If one input is twice as big as the other, the
        // factor is 10^(1/2) = ~3.16.

        Double leftRowCount = RelMetadataQuery.getRowCount(rel.getLeft());
        if (leftRowCount == null) {
            return null;
        }
        Double rightRowCount = RelMetadataQuery.getRowCount(rel.getRight());
        if (rightRowCount == null) {
            return null;
        }
        double minInputRowCount = Math.min(leftRowCount, rightRowCount);
        double maxInputRowCount = Math.max(leftRowCount, rightRowCount);
        if (maxInputRowCount > 0) {
            double joinRowCount = RelMetadataQuery.getRowCount(rel);
            double factor =
                (rel.getCondition().isAlwaysTrue() ||
                    !hashJoinable(rel)) ?
                1.0 : (minInputRowCount / maxInputRowCount);
            joinRowCount *= Math.pow(10.0, factor);
            result += joinRowCount;
        }

        return result;
    }
    
    /**
     * Determines whether the join condition in a join can be fully processed
     * using a hash join.
     * 
     * @param joinRel the join
     * 
     * @return true if the join can be fully processed using a hash join
     */
    private boolean hashJoinable(JoinRel joinRel)
    {
        List<RexNode> leftJoinKeys = new ArrayList<RexNode>();
        List<RexNode> rightJoinKeys = new ArrayList<RexNode>();
        List<Integer> filterNulls = new ArrayList<Integer>();

        // Determine whether there are any join conditions that can't be
        // handled by hash join
        RexNode nonEquiCondition =
            RelOptUtil.splitJoinCondition(
                joinRel.getLeft(),
                joinRel.getRight(),
                joinRel.getCondition(),
                leftJoinKeys,
                rightJoinKeys,
                filterNulls,
                null);
        return (nonEquiCondition == null);
    }

    public Double getCostWithFilters(AggregateRel rel, RexNode filter)
    {
        Double relRowCount = RelMetadataQuery.getRowCount(rel);
        if (relRowCount == null) {
            return null;
        }
        Double childRowCount = RelMetadataQuery.getRowCount(rel.getChild());
        if (childRowCount == null) {
            return null;
        }

        RelOptCost childCost =
            RelMetadataQuery.getCumulativeCost(rel.getChild());
        if (childCost == null) {
            return null;
        }
        return (3 * relRowCount) + childRowCount + childCost.getRows();
    }

    public Double getDistinctRowCount(
        LhxJoinRel rel,
        BitSet groupKey,
        RexNode predicate)
    {
        // TODO zfong 8/29/06 - if we support mapping of physical relnodes to
        // logical relnodes, then this method may not be needed
        LhxJoinRelType joinType = rel.getJoinType();
        int nFieldsLeft = rel.getLeft().getRowType().getFieldCount();
        if (joinType == LhxJoinRelType.LEFTSEMI) {
            assert (groupKey.nextSetBit(nFieldsLeft) < 0);
            return RelMetadataQuery.getDistinctRowCount(
                rel.getLeft(),
                groupKey,
                predicate);
        } else if (joinType == LhxJoinRelType.RIGHTANTI) {
            // the key references all columns on the left, so we can reuse
            // it to represent the columns on the right
            assert (groupKey.nextSetBit(nFieldsLeft) < 0);
            return RelMetadataQuery.getDistinctRowCount(
                rel.getRight(),
                groupKey,
                predicate);
        } else {
            return RelMdUtil.getJoinDistinctRowCount(
                rel,
                joinType.getLogicalJoinType(),
                groupKey,
                predicate);
        }
    }

    public Double getDistinctRowCount(
        LcsRowScanRel rel,
        BitSet groupKey,
        RexNode predicate)
    {
        return columnMd.getDistinctRowCount(rel, groupKey, predicate);
    }

    public Double getSelectivity(JoinRel rel, RexNode predicate)
    {
        RexNode unionPreds =
            RelMdUtil.unionPreds(
                rel.getCluster().getRexBuilder(),
                predicate,
                rel.getCondition());

        Double rowCount = RelMetadataQuery.getRowCount(rel);
        if (rowCount == null) {
            return null;
        }
        return NumberUtil.divide(
            computeRowCountGivenConds(
                rel.getLeft(),
                rel.getRight(),
                rel.getJoinType(),
                unionPreds),
            rowCount);
    }

    public Double getSelectivity(LcsRowScanRel rel, RexNode predicate)
    {
        return estimateRowScanSelectivity(rel, predicate, false);
    }

    private double estimateRowScanSelectivity(
        LcsRowScanRel rel,
        RexNode predicate,
        boolean excludeCalc)
    {
        double selectivity = 1.0;
        if (predicate == null) {
            return selectivity;
        }

        // if no stats are available and we're not discriminating by
        // which filters require a calculator, return a guess
        RelStatSource tabStats = RelMetadataQuery.getStatistics(rel);
        if ((tabStats == null) && !excludeCalc) {
            return RelMdUtil.guessSelectivity(predicate);
        }

        SargFactory sargFactory =
            new SargFactory(rel.getCluster().getRexBuilder());
        SargRexAnalyzer rexAnalyzer = sargFactory.newRexAnalyzer();

        // determine which predicates are sargable
        List<SargBinding> sargBindingList = rexAnalyzer.analyzeAll(predicate);
        Map<CwmColumn, SargIntervalSequence> col2SeqMap = null;
        if (!sargBindingList.isEmpty()) {
            col2SeqMap = LcsIndexOptimizer.getCol2SeqMap(rel, sargBindingList);
        }

        // TODO jvs 19-Apr-2006:  get LcsIndexGuide to help with
        // mapping columns to ordinals, since it knows about UDT
        // flattening.

        // if the column has sargable predicates, compute the
        // selectivity based on those predicates
        if (col2SeqMap != null) {
            Set<CwmColumn> cols = col2SeqMap.keySet();
            for (CwmColumn col : cols) {
                SargIntervalSequence sargSeq = col2SeqMap.get(col);
                if (sargSeq != null) {
                    int colno = ((FemAbstractColumn) col).getOrdinal();
                    RelStatColumnStatistics colStats = null;
                    if (tabStats != null) {
                        colStats = tabStats.getColumnStatistics(colno, sargSeq);
                    }
                    Double colSel = null;
                    if (colStats != null) {
                        colSel = colStats.getSelectivity();
                    }
                    if (colSel == null) {
                        // if no stats are available for this column, then
                        // just use a guess
                        selectivity *= 0.1;
                    } else {
                        selectivity *= colSel;
                    }
                }
            }
        }

        // compute the selectivity of the non-sargable predicates; if
        // excludeCalc is true, we include only artificial selectivities;
        // otherwise, pass artificialOnly=false, which includes everything
        selectivity *=
            RelMdUtil.guessSelectivity(
                rexAnalyzer.getNonSargFilterRexNode(),
                excludeCalc);

        // selectivity must return at least one row
        Double rowCount = RelMetadataQuery.getRowCount(rel);
        if (rowCount != null) {
            selectivity = Math.max(selectivity, 1.0 / rowCount);
        }

        return selectivity;
    }
    
    public Double getRowCount(LcsRowScanRel rel)
    {
        Double result = FarragoRelMetadataProvider.getRowCountStat(rel, repos);
        if (result == null) {
            return null;
        }
        // Include selectivity of inputs into the rowscan
        return result * rel.getInputSelectivity();
    }

    public Double getRowCount(JoinRel rel)
    {
        return computeRowCountGivenConds(
            rel.getLeft(),
            rel.getRight(),
            rel.getJoinType(),
            rel.getCondition());
    }

    public Double getRowCount(LhxJoinRel rel)
    {
        // TODO zfong 8/29/06 - if we support mapping of physical relnodes to
        // logical relnodes, then this method may not be needed
        Double numRows;
        LhxJoinRelType joinType = rel.getJoinType();
        RelNode left = rel.getLeft();
        RelNode right = rel.getRight();
        List<Integer> leftKeys = rel.getLeftKeys();
        List<Integer> rightKeys = rel.getRightKeys();

        if (joinType == LhxJoinRelType.LEFTSEMI) {
            numRows = RelMetadataQuery.getRowCount(left);
        } else if (
            (joinType == LhxJoinRelType.RIGHTANTI)
            || (joinType == LhxJoinRelType.RIGHTSEMI))
        {
            numRows = RelMetadataQuery.getRowCount(right);
        } else {
            // construct a join condition consisting of the hash join keys
            // and then use that to estimate the number of rows in the join
            // result
            List<RexNode> joinFilterList = new ArrayList<RexNode>();
            RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
            RelDataTypeField [] leftFields = left.getRowType().getFields();
            int nLeftFields = leftFields.length;
            RelDataTypeField [] rightFields = right.getRowType().getFields();
            for (int i = 0; i < leftKeys.size(); i++) {
                RexNode leftInput =
                    rexBuilder.makeInputRef(
                        leftFields[leftKeys.get(i)].getType(),
                        leftKeys.get(i));
                RexNode rightInput =
                    rexBuilder.makeInputRef(
                        rightFields[rightKeys.get(i)].getType(),
                        rightKeys.get(i) + nLeftFields);
                joinFilterList.add(
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.equalsOperator,
                        leftInput,
                        rightInput));
            }
            RexNode condition =
                RexUtil.andRexNodeList(rexBuilder, joinFilterList);
            numRows =
                computeRowCountGivenConds(
                    left,
                    right,
                    joinType.getLogicalJoinType(),
                    condition);
            if (numRows == null) {
                numRows =
                    RelMetadataQuery.getRowCount(left)
                    * RelMetadataQuery.getRowCount(right)
                    * RelMdUtil.guessSelectivity(condition);
            }
        }
        return numRows;
    }

    /**
     * Computes the rowcount from a join, given a join condition and the join
     * inputs
     *
     * @param leftChild left input into the join
     * @param rightChild right input into the join
     * @param joinType join type
     * @param predicate the join condition
     *
     * @return computed join rowcount
     */
    private Double computeRowCountGivenConds(
        RelNode leftChild,
        RelNode rightChild,
        JoinRelType joinType,
        RexNode predicate)
    {
        // locate the columns that participate in equijoins
        BitSet leftJoinCols = new BitSet();
        BitSet rightJoinCols = new BitSet();
        RexNode nonEquiJoin =
            RelMdUtil.findEquiJoinCols(
                leftChild,
                rightChild,
                predicate,
                leftJoinCols,
                rightJoinCols);

        Double nRowsLeft = RelMetadataQuery.getRowCount(leftChild);
        Double nRowsRight = RelMetadataQuery.getRowCount(rightChild);
        if ((nRowsLeft == null) || (nRowsRight == null)) {
            return null;
        }

        // Calculate the number of groups (distinct join values)
        // on each side of the join, taking into account table level
        // filters (including semijoin filters).
        // Try the left and the right side for estimates.
        // getDistinctRowCount() will return null if it has no idea what the
        // cardinality is so don't use that estimate.  Note that by using
        // getDistinctRowCount() instead of getPopulationSize(), we take
        // into account semijoins in both the numerator and denominator
        // when calculating the rowcount
        Double leftCard =
            RelMetadataQuery.getDistinctRowCount(
                leftChild,
                leftJoinCols,
                null);
        Double rightCard =
            RelMetadataQuery.getDistinctRowCount(
                rightChild,
                rightJoinCols,
                null);
        double nGroups =
            ((leftCard != null) && (rightCard != null))
            ? Math.max(
                Math.max(leftCard, rightCard),
                1.0)
            : 0.0;

        double rowCount;

        // Compute expected rowcount using different methods depending on how
        // much information we have.
        if (nGroups > 0.0) {
            // Calculate expected rowcount based on equi-join conditions,
            // assuming we don't have an outer join
            // rowcount = nRowsPerGroupLeft * nRowsPerGroupRight * nGroups
            //          = (nRowsLeft/nGroups) * (nRowsRight/nGroups) * nGroups
            //          = nRowsLeft * nRowsRight / nGroups
            rowCount = nRowsLeft * nRowsRight / nGroups;

            // adjust for non-equijoin filters
            rowCount *= RelMdUtil.guessSelectivity(nonEquiJoin);
        } else {
            // determine which side corresponds to the dimension table
            // and return the number of rows on the fact table side
            if (leftJoinCols.cardinality() > 0) {
                Boolean dimLeft =
                    dimOnLeft(
                        leftChild,
                        rightChild,
                        leftJoinCols,
                        rightJoinCols);
                if (dimLeft == null) {
                    rowCount =
                        nRowsLeft * nRowsRight
                        * RelMdUtil.guessSelectivity(predicate);
                } else if (dimLeft == true) {
                    rowCount = nRowsRight;
                } else {
                    rowCount = nRowsLeft;
                }

                // adjust for non-equijoin filters
                rowCount *= RelMdUtil.guessSelectivity(nonEquiJoin);
            } else {
                rowCount =
                    nRowsLeft * nRowsRight
                    * RelMdUtil.guessSelectivity(predicate);
            }
        }

        if (joinType.generatesNullsOnLeft()) {
            rowCount = Math.max(rowCount, nRowsRight);
        }
        if (joinType.generatesNullsOnRight()) {
            rowCount = Math.max(rowCount, nRowsLeft);
        }

        return RelMdUtil.capInfinity(rowCount);
    }

    /**
     * Returns true if the dimension table is on the LHS of a join
     *
     * @param left left input into the join
     * @param right right input into the join
     * @param leftJoinCols equijoin columns from the left hand side of the join
     * @param rightJoinCols equijoin columns from the right hand side of the
     * join
     *
     * @return true if dimension table is on the left; null if cannot determine
     */
    private Boolean dimOnLeft(
        RelNode left,
        RelNode right,
        BitSet leftJoinCols,
        BitSet rightJoinCols)
    {
        Boolean leftUnique =
            RelMetadataQuery.areColumnsUnique(
                left,
                leftJoinCols);
        Boolean rightUnique =
            RelMetadataQuery.areColumnsUnique(
                right,
                rightJoinCols);

        if ((leftUnique == null) || (rightUnique == null)) {
            return null;
        }

        // if one side has unique columns, then that's the dimension table
        if (leftUnique) {
            return true;
        } else if (rightUnique) {
            return false;
        }

        // if neither side is unique, then whichever side yields a smaller
        // result after filtering is the dimension table
        Double leftPercent = RelMetadataQuery.getPercentageOriginalRows(left);
        Double rightPercent = RelMetadataQuery.getPercentageOriginalRows(right);
        if ((leftPercent == null) || (rightPercent == null)) {
            return null;
        }
        if ((RelMetadataQuery.getRowCount(left) / leftPercent)
            < (RelMetadataQuery.getRowCount(right) / rightPercent))
        {
            return true;
        } else {
            return false;
        }
    }

    public Double getPopulationSize(LhxJoinRel rel, BitSet groupKey)
    {
        // TODO zfong 8/29/06 - if we support mapping of physical relnodes to
        // logical relnodes, then this method may not be needed
        LhxJoinRelType joinType = rel.getJoinType();
        int nFieldsLeft = rel.getLeft().getRowType().getFieldCount();
        if (joinType == LhxJoinRelType.LEFTSEMI) {
            assert (groupKey.nextSetBit(nFieldsLeft) < 0);
            return RelMetadataQuery.getPopulationSize(rel.getLeft(), groupKey);
        } else if (joinType == LhxJoinRelType.RIGHTANTI) {
            // the key references all columns on the left, so we can reuse
            // it to represent the columns on the right
            assert (groupKey.nextSetBit(nFieldsLeft) < 0);
            return RelMetadataQuery.getPopulationSize(rel.getRight(), groupKey);
        } else {
            return RelMdUtil.getJoinPopulationSize(rel, groupKey);
        }
    }

    public Double getPopulationSize(LcsRowScanRel rel, BitSet groupKey)
    {
        return columnMd.getPopulationSize(rel, groupKey);
    }

    public Set<BitSet> getUniqueKeys(LcsRowScanRel rel)
    {
        return columnMd.getUniqueKeys(rel, repos);
    }

    public Boolean areColumnsUnique(LcsRowScanRel rel, BitSet columns)
    {
        return columnMd.areColumnsUnique(rel, columns, repos);
    }

    public Set<RelColumnOrigin> getSimpleColumnOrigins(
        JoinRelBase rel,
        int iOutputColumn)
    {
        return columnOrigins.getColumnOrigins(rel, iOutputColumn);
    }

    public Set<RelColumnOrigin> getSimpleColumnOrigins(
        ProjectRelBase rel,
        int iOutputColumn)
    {
        return columnOrigins.getColumnOrigins(rel, iOutputColumn);
    }

    public Set<RelColumnOrigin> getSimpleColumnOrigins(
        FilterRelBase rel,
        int iOutputColumn)
    {
        return LoptMetadataQuery.getSimpleColumnOrigins(
            rel.getChild(),
            iOutputColumn);
    }

    // Catch-all rule when none of the others apply.
    public Set<RelColumnOrigin> getSimpleColumnOrigins(
        RelNode rel,
        int iOutputColumn)
    {
        // if this RelNode contains inputs and it wasn't already handled by
        // another method, or it's a non-leaf node, then it's not simple
        if ((rel.getInputs().length > 0) || (rel.getTable() == null)) {
            return null;
        }
        return columnOrigins.getColumnOrigins(rel, iOutputColumn);
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * An extension of {@link RelMdColumnOrigins} that invokes {@link
     * LoptMetadataQuery#getSimpleColumnOrigins} instead of {@link
     * RelMetadataQuery#getColumnOrigins}.
     */
    private class SimpleColumnOrigins
        extends RelMdColumnOrigins
    {
        protected Set<RelColumnOrigin> invokeGetColumnOrigins(
            RelNode rel,
            int iOutputColumn)
        {
            return LoptMetadataQuery.getSimpleColumnOrigins(rel, iOutputColumn);
        }
    }
    
    public Boolean isVisibleInExplain(
        LcsIndexSearchRel rel,
        SqlExplainLevel level)
    {
        return
            level == SqlExplainLevel.ALL_ATTRIBUTES ||
            rel.isVisibleInExplain();
    }
    
    public Boolean isVisibleInExplain(
        LcsIndexMinusRel rel,
        SqlExplainLevel level)
    {
        return level == SqlExplainLevel.ALL_ATTRIBUTES;
    }
    
    public Boolean isVisibleInExplain(
        FennelValuesRel rel,
        SqlExplainLevel level)
    {
        // Note that this method is defined here rather than
        // FarragoRelMetadataProvider because only the LucidDbPersonality
        // depends on this.
        return
            level == SqlExplainLevel.ALL_ATTRIBUTES ||
            rel.isVisibleInExplain();
    }
}

// End LoptMetadataProvider.java
