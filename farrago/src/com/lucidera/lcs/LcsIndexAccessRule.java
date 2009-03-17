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
package com.lucidera.lcs;

import java.util.*;

import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.query.*;
import net.sf.farrago.type.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sarg.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.stat.*;


/**
 * LcsIndexAccessRule is a rule for converting FilterRel+LcsRowScanRel into
 * LcsIndexAccessRel (when the filter has the appropriate form).
 *
 * @author Rushan Chen
 * @version $Id$
 */
public class LcsIndexAccessRule
    extends RelOptRule
{
    //~ Constructors -----------------------------------------------------------

    public LcsIndexAccessRule(RelOptRuleOperand rule, String id)
    {
        // This rule is fired for either of the following 4 patterns:
        //
        // RelOptRuleOperand(
        //    FilterRel.class,
        //    new RelOptRuleOperand(LcsRowScanRel.class, ANY))
        // or
        //
        // RelOptRuleOperand(
        //     FilterRel.class,
        //     new RelOptRuleOperand(
        //         LcsRowScanRel.class,
        //             new RelOptRuleOperand(LcsIndexIntersectRel.class, ANY)))
        // or
        //
        // RelOptRuleOperand(
        //     FilterRel.class,
        //     new RelOptRuleOperand(
        //         LcsRowScanRel.class,
        //         new RelOptRuleOperand(LcsIndexSearchRel.class, ANY)))
        // or
        //
        // RelOptRuleOperand(
        //     FilterRel.class,
        //     new RelOptRuleOperand(
        //         LcsRowScanRel.class,
        //         new RelOptRuleOperand(
        //             LcsIndexMergeRel.class,
        //             new RelOptRuleOperand(LcsIndexSearchRel.class, ANY))))

        // TODO: SWZ 11-Sep-2007: This rule is not fired for
        // LcsSamplingRowScanRel.  It could be used without modification for
        // that RelNode type if either of the following is true:
        // 1. Sampling mode is BERNOULLI sampling
        // 2. Sampling mode is SYSTEM and no indexes will be applied (e.g.,
        //    only residual filters)
        // (Making this change would require changing the rule operands to
        // take LcsRowScanRelBase and constructing the correct replacement
        // subclass in considerIndex.)  Note that for system sampling, we
        // could go further and only apply residual filters even when indexes
        // are normally called for.  Not sure if the portion of the filter
        // that would have generated an index scan should become a residual
        // filter or not.

        super(rule);
        description = "LcsIndexAccessRule: " + id;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return null;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        int rowScanRelPosInCall = 1;
        FilterRel filter = (FilterRel) call.rels[0];
        LcsRowScanRel origRowScan =
            (LcsRowScanRel) call.rels[rowScanRelPosInCall];

        // if the rowscan is already being used with an index, then let one
        // of the other rules handle those cases
        if ((call.rels.length == (rowScanRelPosInCall + 1))
            && !origRowScan.isFullScan)
        {
            // Filter
            //   RowScanRel with index access input
            //     some input
            return;
        }

        RexBuilder rexBuilder = origRowScan.getCluster().getRexBuilder();
        SargFactory sargFactory = new SargFactory(rexBuilder);
        SargRexAnalyzer rexAnalyzer = sargFactory.newRexAnalyzer();

        RexNode filterExp = filter.getCondition();

        List<SargBinding> sargBindingList = rexAnalyzer.analyzeAll(filterExp);

        if (sargBindingList.isEmpty()) {
            // No sargable predicates.
            return;
        }

        considerIndex(call, rowScanRelPosInCall, rexAnalyzer, sargBindingList);
    }

    private void considerIndex(
        RelOptRuleCall call,
        int rowScanRelPosInCall,
        SargRexAnalyzer rexAnalyzer,
        List<SargBinding> sargBindingList)
    {
        LcsRowScanRel origRowScan =
            ((LcsRowScanRel) call.rels[rowScanRelPosInCall]);

        RexBuilder rexBuilder = origRowScan.getCluster().getRexBuilder();

        Map<CwmColumn, SargIntervalSequence> col2SeqMap =
            LcsIndexOptimizer.getCol2SeqMap(origRowScan, sargBindingList);

        Double filterCorrelationFactor = 0.5;

        // Combined selectivity for all the sargable filters
        Double sargFilterSelectivity =
            RelMetadataQuery.getSelectivity(
                origRowScan,
                rexAnalyzer.getSargBindingListToRexNode(sargBindingList));

        // Try cost based index selection
        List<List<LcsIndexOptimizer.SargColumnFilter>> colFilterLists =
            getSargColFilterLists(origRowScan, sargBindingList, col2SeqMap);

        LcsIndexOptimizer indexOptimizer = new LcsIndexOptimizer(origRowScan);

        Map<FemLocalIndex, Integer> index2PosMap =
            indexOptimizer.getIndex2MatchedPosByCost(colFilterLists);

        assert (index2PosMap != null);

        // Use a tree set here so that the indexes are searched in a fixed
        // order, to make the plan output stable.
        TreeSet<FemLocalIndex> indexSet =
            new TreeSet<FemLocalIndex>(
                new LcsIndexOptimizer.IndexLengthComparator());

        indexSet.addAll(index2PosMap.keySet());

        List<SargBinding> nonResidualSargBindingList =
            new ArrayList<SargBinding>();

        List<SargBinding> residualSargBindingList =
            getResidualSargBinding(
                origRowScan,
                sargBindingList,
                index2PosMap,
                nonResidualSargBindingList);

        int residualColumnCount = residualSargBindingList.size();
        if ((indexSet.size() == 0) && (residualColumnCount == 0)) {
            // no index usable
            // no residual filtering
            return;
        }

        RexNode nonResidualFilter =
            rexAnalyzer.getSargBindingListToRexNode(
                nonResidualSargBindingList);
        RexNode nonSargFilter = rexAnalyzer.getNonSargFilterRexNode();
        RexNode postFilter = null;

        if ((nonResidualFilter != null) && (nonSargFilter != null)) {
            postFilter =
                rexBuilder.makeCall(
                    SqlStdOperatorTable.andOperator,
                    nonResidualFilter,
                    nonSargFilter);
        } else if (nonResidualFilter != null) {
            postFilter = nonResidualFilter;
        } else if (nonSargFilter != null) {
            postFilter = nonSargFilter;
        }

        // calculate input selectivity now that we know which filters are used
        // for index and residual filtering
        Double rowScanInputSelectivity = 1.0;
        if (sargFilterSelectivity != null) {
            rowScanInputSelectivity = sargFilterSelectivity;
        }

        if (nonResidualFilter != null) {
            // Selectivity for the non residual sarg filters
            Double nonResidualSargFilterSelectivity =
                RelMetadataQuery.getSelectivity(
                    origRowScan,
                    nonResidualFilter);
            if (nonResidualSargFilterSelectivity != null) {
                rowScanInputSelectivity /= nonResidualSargFilterSelectivity;
                rowScanInputSelectivity /= filterCorrelationFactor;
                rowScanInputSelectivity =
                    Math.min(1.0, rowScanInputSelectivity);
            }
        }

        int origResidualColumnCount = origRowScan.residualColumns.length;

        int origIndexRelCount =
            origRowScan.getInputs().length - origResidualColumnCount;
        int indexRelCount =
            ((indexSet.size() > 0) || (origIndexRelCount == 1)) ? 1 : 0;

        RelNode [] rowScanInputRels =
            new RelNode[indexRelCount
                + residualColumnCount
                + origResidualColumnCount];

        // Setup the new index input rel
        if (indexSet.size() > 0) {
            // AND the INDEX rels together.
            RelNode newIndexAccessRel = null;
            if (!origRowScan.isFullScan) {
                assert (call.rels.length > (rowScanRelPosInCall + 1));
                newIndexAccessRel = call.rels[rowScanRelPosInCall + 1];
            }

            for (FemLocalIndex index : indexSet) {
                int matchedPos = index2PosMap.get(index);

                List<SargIntervalSequence> sargSeqList =
                    getIndexSargSeq(index, matchedPos, col2SeqMap);

                LcsIndexOptimizer.CandidateIndex candidate =
                    new LcsIndexOptimizer.CandidateIndex(
                        index,
                        matchedPos,
                        sargSeqList);

                newIndexAccessRel =
                    addNewIndexAccessRel(
                        newIndexAccessRel,
                        call,
                        rowScanRelPosInCall,
                        candidate,
                        rowScanInputSelectivity);
            }
            rowScanInputRels[0] = newIndexAccessRel;
        } else if (origIndexRelCount == 1) {
            rowScanInputRels[0] = origRowScan.getInput(0);
        }

        Integer [] resCols = new Integer[residualColumnCount];
        RelNode [] valueRels = new RelNode[residualColumnCount];
        final RelTraitSet callTraits = call.rels[0].getTraits();

        buildColumnFilters(
            resCols,
            callTraits,
            valueRels,
            residualColumnCount,
            residualSargBindingList,
            origRowScan);

        // Setup the new residual input rels and residual columns
        Integer [] newResCols =
            new Integer[residualColumnCount + origResidualColumnCount];

        for (int i = 0; i < residualColumnCount; i++) {
            rowScanInputRels[i + indexRelCount] = valueRels[i];
            newResCols[i] = resCols[i];
        }

        for (int i = 0; i < origResidualColumnCount; i++) {
            rowScanInputRels[i + indexRelCount + residualColumnCount] =
                origRowScan.getInput(i + origIndexRelCount);
            newResCols[i + residualColumnCount] =
                origRowScan.residualColumns[i];
        }

        // Build a RowScan rel based on index search with no extra filters.
        LcsRowScanRel rowScan =
            new LcsRowScanRel(
                origRowScan.getCluster(),
                rowScanInputRels,
                origRowScan.lcsTable,
                origRowScan.clusteredIndexes,
                origRowScan.getConnection(),
                origRowScan.projectedColumns,
                indexRelCount == 0,
                newResCols,
                rowScanInputSelectivity);

        transformCall(call, rowScan, postFilter);
    }

    private void buildColumnFilters(
        Integer [] residualColumns,
        RelTraitSet callTraits,
        RelNode [] valueRels,
        int residualColumnCount,
        List<SargBinding> residualSargBindingList,
        LcsRowScanRel origRowScan)
    {
        if (residualColumnCount > 0) {
            RelStatSource tabStats =
                RelMetadataQuery.getStatistics(origRowScan);

            // sort the column filters based on selectivity first
            TreeSet<LcsIndexOptimizer.SargColumnFilter> filterSet =
                new TreeSet<LcsIndexOptimizer.SargColumnFilter>(
                    new LcsIndexOptimizer.SargColumnFilterSelectivityComparator(
                        tabStats));

            for (int i = 0; i < residualColumnCount; i++) {
                SargBinding sargBinding = residualSargBindingList.get(i);
                SargIntervalSequence sargSeq =
                    FennelRelUtil.evaluateSargExpr(sargBinding.getExpr());
                RexInputRef fieldAccess = sargBinding.getInputRef();
                filterSet.add(
                    new LcsIndexOptimizer.SargColumnFilter(
                        fieldAccess.getIndex(),
                        sargSeq));
            }

            FemAbstractAttribute [] searchColumns = new FemAbstractAttribute[1];

            int i = 0;

            // then create the actual column filters in that sort order
            for (LcsIndexOptimizer.SargColumnFilter filter : filterSet) {
                // Residual column position is relative to the underlying
                // column store table, same with the project column positions.
                residualColumns[i] =
                    origRowScan.getOriginalColumnOrdinal(filter.columnPos);
                searchColumns[0] =
                    origRowScan.getColumnForFieldAccess(filter.columnPos);

                RelDataType keyRowType =
                    getSearchKeyRowType(
                        origRowScan,
                        searchColumns);

                List<SargIntervalSequence> sargSeqList =
                    new ArrayList<SargIntervalSequence>();
                sargSeqList.add(filter.sargSeq);

                valueRels[i] =
                    FennelRelUtil.convertSargExpr(
                        callTraits,
                        keyRowType,
                        origRowScan.getCluster(),
                        sargSeqList);

                valueRels[i] =
                    mergeTraitsAndConvert(
                        callTraits,
                        FennelRel.FENNEL_EXEC_CONVENTION,
                        valueRels[i]);
                i++;
            }
        }
    }

    private void transformCall(
        RelOptRuleCall call,
        RelNode rel,
        RexNode extraFilter)
    {
        if (extraFilter != null) {
            rel =
                new FilterRel(
                    rel.getCluster(),
                    rel,
                    extraFilter);
        }
        call.transformTo(rel);
    }

    private static List<SargIntervalSequence> getIndexSargSeq(
        FemLocalIndex index,
        int matchedPos,
        Map<CwmColumn, SargIntervalSequence> col2SeqMap)
    {
        List<SargIntervalSequence> seqList =
            new ArrayList<SargIntervalSequence>();

        for (int pos = 0; pos < matchedPos; pos++) {
            FemAbstractColumn col =
                LcsIndexOptimizer.getIndexColumn(index, pos);
            SargIntervalSequence sargSeq = col2SeqMap.get(col);
            seqList.add(sargSeq);
        }

        return seqList;
    }

    private static List<List<LcsIndexOptimizer.SargColumnFilter>>
    getSargColFilterLists(
        LcsRowScanRel origRowScan,
        List<SargBinding> sargBindingList,
        Map<CwmColumn, SargIntervalSequence> col2SeqMap)
    {
        List<List<LcsIndexOptimizer.SargColumnFilter>> retLists =
            new ArrayList<List<LcsIndexOptimizer.SargColumnFilter>>();
        List<LcsIndexOptimizer.SargColumnFilter> pointFilterList =
            new ArrayList<LcsIndexOptimizer.SargColumnFilter>();
        List<LcsIndexOptimizer.SargColumnFilter> intervalFilterList =
            new ArrayList<LcsIndexOptimizer.SargColumnFilter>();

        for (int i = 0; i < sargBindingList.size(); i++) {
            SargBinding sargBinding = sargBindingList.get(i);
            RexInputRef inputRef = sargBinding.getInputRef();
            int colPos = inputRef.getIndex();
            FemAbstractColumn filterColumn =
                origRowScan.getColumnForFieldAccess(inputRef.getIndex());
            if (filterColumn != null) {
                SargIntervalSequence sargSeq = col2SeqMap.get(filterColumn);

                if (sargSeq.isPoint()) {
                    pointFilterList.add(
                        new LcsIndexOptimizer.SargColumnFilter(
                            colPos,
                            sargSeq));
                } else {
                    intervalFilterList.add(
                        new LcsIndexOptimizer.SargColumnFilter(
                            colPos,
                            sargSeq));
                }
            }
        }

        retLists.add(0, pointFilterList);
        retLists.add(1, intervalFilterList);
        return retLists;
    }

    private static List<SargBinding> getResidualSargBinding(
        LcsRowScanRel origRowScan,
        List<SargBinding> sargBindingList,
        Map<FemLocalIndex, Integer> index2PosMap,
        List<SargBinding> nonResidualList)
    {
        List<CwmColumn> sargColList = new ArrayList<CwmColumn>();
        List<SargBinding> retSargBindingList = new ArrayList<SargBinding>();

        for (int i = 0; i < sargBindingList.size(); i++) {
            SargBinding sargBinding = sargBindingList.get(i);
            RexInputRef fieldAccess = sargBinding.getInputRef();
            FemAbstractColumn filterColumn =
                origRowScan.getColumnForFieldAccess(fieldAccess.getIndex());
            if (filterColumn != null) {
                sargColList.add(i, filterColumn);
                retSargBindingList.add(sargBindingList.get(i));
            } else {
                nonResidualList.add(sargBindingList.get(i));
            }
        }

        if (index2PosMap != null) {
            // exclude those columns that are already mapped to index searches
            for (FemLocalIndex index : index2PosMap.keySet()) {
                int maxPos = index2PosMap.get(index);
                for (int pos = 0; pos < maxPos; pos++) {
                    FemAbstractColumn filterColumn =
                        LcsIndexOptimizer.getIndexColumn(index, pos);
                    int i = sargColList.indexOf(filterColumn);
                    retSargBindingList.remove(i);
                    sargColList.remove(i);
                }
            }
        }

        return retSargBindingList;
    }

    private RelNode addNewIndexAccessRel(
        RelNode oldIndexAccessRel,
        RelOptRuleCall call,
        int rowScanRelPosInCall,
        LcsIndexOptimizer.CandidateIndex candidate,
        Double rowScanInputSelectivity)
    {
        assert (call.rels[rowScanRelPosInCall] instanceof LcsRowScanRel);
        LcsRowScanRel rowScanRel =
            (LcsRowScanRel) call.rels[rowScanRelPosInCall];

        FemLocalIndex index = candidate.index;
        int matchedPos = candidate.matchedPos;
        List<SargIntervalSequence> sargSeqList = candidate.sargSeqList;
        boolean requireMerge = candidate.requireMerge();

        RelOptCluster cluster = rowScanRel.getCluster();

        final RelTraitSet callTraits =
            call.rels[rowScanRelPosInCall].getTraits();

        RelDataType keyRowType =
            getIndexInputType(rowScanRel, index, matchedPos);
        RelNode sargRel =
            FennelRelUtil.convertSargExpr(
                callTraits,
                keyRowType,
                cluster,
                sargSeqList);

        RelNode keyInput =
            mergeTraitsAndConvert(
                callTraits,
                FennelRel.FENNEL_EXEC_CONVENTION,
                sargRel);
        assert (keyInput != null);

        // Set up projections for the search directive and key.
        Integer [] inputDirectiveProj = new Integer[] { 0, (matchedPos + 1) };
        Integer [] inputKeyProj = new Integer[matchedPos * 2];
        for (int i = 0; i < matchedPos; i++) {
            inputKeyProj[i] = i + 1;
            inputKeyProj[i + matchedPos] = matchedPos + i + 2;
        }

        RelNode indexRel =
            LcsIndexOptimizer.addNewIndexAccessRel(
                oldIndexAccessRel,
                call,
                rowScanRelPosInCall,
                index,
                keyInput,
                inputKeyProj,
                inputDirectiveProj,
                requireMerge,
                rowScanInputSelectivity);

        return indexRel;
    }

    /**
     * Create a type descriptor for the rows representing search keys along with
     * their directives, given an array of the attributes representing the
     * search columns. Note that we force the key type to nullable because we
     * use null for the representation of infinity (rather than domain-specific
     * values).
     *
     * @param rel the table scan node that is being optimized
     * @param searchColumns array of column attributes representing the search
     * keys
     */
    private static RelDataType getSearchKeyRowType(
        FennelRel rel,
        FemAbstractAttribute [] searchColumns)
    {
        FarragoPreparingStmt stmt = FennelRelUtil.getPreparingStmt(rel);
        FarragoTypeFactory typeFactory = stmt.getFarragoTypeFactory();
        RelDataType directiveType =
            typeFactory.createSqlType(
                SqlTypeName.CHAR,
                1);

        int searchLen = searchColumns.length;
        int keyRowLength = (searchLen * 2) + 2;
        int keyRowMidPoint = keyRowLength / 2;
        int lowerBoundKeyBase = 1;
        int upperBoundKeyBase = keyRowMidPoint + 1;

        RelDataType [] dataTypes = new RelDataType[keyRowLength];
        String [] typeDescriptions = new String[keyRowLength];

        dataTypes[0] = directiveType;
        dataTypes[keyRowMidPoint] = directiveType;

        typeDescriptions[0] = "lowerBoundDirective";
        typeDescriptions[keyRowMidPoint] = "upperBoundDirective";

        for (int pos = 0; pos < searchLen; pos++) {
            RelDataType keyType =
                typeFactory.createTypeWithNullability(
                    typeFactory.createCwmElementType(
                        (FemAbstractAttribute) searchColumns[pos]),
                    true);
            dataTypes[lowerBoundKeyBase + pos] = keyType;
            dataTypes[upperBoundKeyBase + pos] = keyType;

            typeDescriptions[lowerBoundKeyBase + pos] = "lowerBoundKey";
            typeDescriptions[upperBoundKeyBase + pos] = "upperBoundKey";
        }

        RelDataType keyRowType =
            typeFactory.createStructType(dataTypes, typeDescriptions);
        return keyRowType;
    }

    /**
     * Create a type descriptor for the rows representing index search keys
     * along with their directives, given an index and the number of matching
     * keys from the index. Note that we force the key type to nullable because
     * we use null for the representation of infinity (rather than
     * domain-specific values).
     *
     * @param rel the table scan node that is being optimized
     * @param index the index descriptor
     * @param matchedPos the length of matched prefix in the index key
     *
     * @return type descriptor for the index search key
     */
    private static RelDataType getIndexInputType(
        FennelRel rel,
        FemLocalIndex index,
        int matchedPos)
    {
        FemAbstractAttribute [] indexColumns =
            new FemAbstractColumn[matchedPos];

        for (int pos = 0; pos < matchedPos; pos++) {
            indexColumns[pos] =
                (FemAbstractAttribute) LcsIndexOptimizer.getIndexColumn(
                    index,
                    pos);
        }

        RelDataType keyRowType = getSearchKeyRowType(rel, indexColumns);
        return keyRowType;
    }
}

// End LcsIndexAccessRule.java
