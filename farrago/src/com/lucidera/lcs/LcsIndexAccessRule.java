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
class LcsIndexAccessRule
    extends RelOptRule
{

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new LcsIndexAccessRule object.
     */
    public LcsIndexAccessRule()
    {
        super(
            new RelOptRuleOperand(
                FilterRel.class,
                new RelOptRuleOperand[] {
                    new RelOptRuleOperand(LcsRowScanRel.class, null)
                }));
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
        FilterRel filter = (FilterRel) call.rels[0];
        LcsRowScanRel origRowScan = (LcsRowScanRel) call.rels[1];

        // If LcsRowScanRel already has input(s), it means Sarg Analysis has
        // already been done for the predicates in this FilterRel, and
        // LcsIndexSearchRel and/or residual SargRel have been allocated as
        // inputs to LcsRowScanRel.
        if (origRowScan.getInputs().length > 0) {
            return;
        }

        RexBuilder rexBuilder = origRowScan.getCluster().getRexBuilder();
        SargFactory sargFactory = new SargFactory(rexBuilder);
        SargRexAnalyzer rexAnalyzer = sargFactory.newRexAnalyzer();

        RexNode filterExp = filter.getCondition();

        List<SargBinding> sargBindingList = rexAnalyzer.analyzeAll(filterExp);

        if (sargBindingList.isEmpty()) {
            // Predicate was not sargable
            return;
        }

        considerIndex(origRowScan, sargBindingList, call, rexAnalyzer);
    }

    private void considerIndex(
        LcsRowScanRel origRowScan,
        List<SargBinding> sargBindingList,
        RelOptRuleCall call,
        SargRexAnalyzer rexAnalyzer)
    {
        FennelRelImplementor relImplementor =
            FennelRelUtil.getRelImplementor(origRowScan);

        // By default, these parameters are not used:
        FennelRelParamId startRidParamId = null;
        FennelRelParamId rowLimitParamId = null;

        LcsIndexGuide indexGuide = origRowScan.lcsTable.getIndexGuide();
        RexBuilder rexBuilder = origRowScan.getCluster().getRexBuilder();

        Map<CwmColumn, SargIntervalSequence> col2SeqMap =
            indexGuide.getCol2SeqMap(origRowScan, sargBindingList);

        List<List<CwmColumn>> colLists =
            getSargColLists(origRowScan, sargBindingList, col2SeqMap);

        Map<FemLocalIndex, Integer> index2PosMap =
            indexGuide.getIndex2PosMap(colLists);

        // Use a tree set here so that the indexes are searched in a fixed
        // order, to make the plan output stable. Note we could optimize the
        // order by using a different comparator function. For example, search
        // the index with the longest matched keys first, or when proper costing
        // is in place, search the most selective index (wrt to key values)
        // first.
        TreeSet<FemLocalIndex> indexSet =
            new TreeSet<FemLocalIndex>(
                new LcsIndexGuide.IndexLengthComparator());

        indexSet.addAll(index2PosMap.keySet());

        List<SargBinding> nonResidualSargBindingList = new 
            ArrayList<SargBinding>();
        List<SargBinding> residualSargBindingList =
            getResidualSargBinding(
                origRowScan,
                sargBindingList,
                index2PosMap, 
                nonResidualSargBindingList);
        
        int residualColumnCount = residualSargBindingList.size();
        
        if (indexSet.size() == 0 && residualColumnCount == 0) {
            // no index usable
            // no residual filtering
            return;
        }

        RexNode nonResidualRexNode =
            rexAnalyzer.getResidualSargRexNode(nonResidualSargBindingList);

        RexNode postFilterRexNode = rexAnalyzer.getPostFilterRexNode();

        RexNode extraFilter = null;

        if ((nonResidualRexNode != null) && (postFilterRexNode != null)) {
            extraFilter =
                rexBuilder.makeCall(
                    SqlStdOperatorTable.andOperator,
                    nonResidualRexNode,
                    postFilterRexNode);
        } else if (nonResidualRexNode != null) {
            extraFilter = nonResidualRexNode;
        } else if (postFilterRexNode != null) {
            extraFilter = postFilterRexNode;
        }
        
        RelNode [] rowScanInputRels =
            new RelNode[(indexSet.size() > 0 ? 1 : 0) + residualColumnCount];
        
        if (indexSet.size() > 0) {
        
            // AND the INDEX search rels together.
            RelNode [] indexRels = new RelNode[indexSet.size()];
            boolean requireIntersect = indexRels.length > 1;

            int i = 0;

            if (requireIntersect) {
                // Allocate AND here
                rowLimitParamId = relImplementor.allocateRelParamId();
                startRidParamId = relImplementor.allocateRelParamId();
            }

            for (FemLocalIndex index : indexSet) {
                int matchedPos = index2PosMap.get(index);

                List<SargIntervalSequence> sargSeqList =
                    getIndexSargSeq(indexGuide, index, matchedPos, col2SeqMap);

                CandidateIndex candidate =
                    new CandidateIndex(
                        index,
                        matchedPos,
                        sargSeqList);

                FennelRel indexRel =
                    newIndexRel(call,
                        origRowScan,
                        candidate,
                        startRidParamId,
                        rowLimitParamId);
                indexRels[i] = indexRel;
                i++;
            }


            if (requireIntersect) {
                FennelRel intersectRel =
                    new LcsIndexIntersectRel(
                        origRowScan.getCluster(),
                        indexRels,
                        origRowScan.lcsTable,
                        startRidParamId,
                        rowLimitParamId);
                rowScanInputRels[0] = intersectRel;
            } else {
                rowScanInputRels[0] = indexRels[0];
            }
        }
        
        Integer [] resCols = new Integer[residualColumnCount];       
        final RelTraitSet callTraits = call.rels[0].getTraits();

        RelNode [] valueRels = new RelNode[residualSargBindingList.size()];

        buildColumnFilters(
            resCols,        
            callTraits,
            valueRels,
            residualColumnCount,
            residualSargBindingList,
            origRowScan);
        
        int offset = (indexSet.size() > 0) ? 1 : 0;
        for (int i = 0; i < residualColumnCount; i++) {
            rowScanInputRels[i + offset] = valueRels[i];
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
            indexSet.size() == 0,
            residualColumnCount > 0,
            resCols);

        transformCall(call, rowScan, extraFilter);       
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
            ColumnFilter [] columnFilters =
                new ColumnFilter[residualColumnCount];
            for (int i = 0; i < residualColumnCount; i++) {
                SargBinding sargBinding = residualSargBindingList.get(i);
                SargIntervalSequence sargSeq = 
                    FennelRelUtil.evaluateSargExpr(sargBinding.getExpr());
                RexInputRef fieldAccess = sargBinding.getInputRef();
                columnFilters[i] =
                    new ColumnFilter(fieldAccess.getIndex(), sargSeq, tabStats);
            }
            Arrays.sort(columnFilters, columnFilters[0]);

            FemAbstractAttribute [] searchColumns = new FemAbstractAttribute[1];

            // then create the actual column filters in that sort order
            for (int i = 0; i < residualColumnCount; i++) {
                residualColumns[i] = columnFilters[i].columnNumber;
                searchColumns[0] = 
                    origRowScan.getColumnForFieldAccess(residualColumns[i]);

                RelDataType keyRowType =
                    getSearchKeyRowType(
                        origRowScan.lcsTable.getIndexGuide(),
                        origRowScan, 
                        searchColumns);

                List<SargIntervalSequence> sargSeqList = 
                    new ArrayList<SargIntervalSequence>();
                sargSeqList.add(columnFilters[i].sargSeq);

                valueRels[i] =  FennelRelUtil.convertSargExpr(callTraits,
                    keyRowType,origRowScan.getCluster(), sargSeqList);

                valueRels[i] = mergeTraitsAndConvert(callTraits, 
                    FennelRel.FENNEL_EXEC_CONVENTION, valueRels[i]);
            }
        }
    }

    private void transformCall(
        RelOptRuleCall call,
        RelNode rel,
        RexNode extraFilter)
    {
        if (extraFilter != null) {
            rel = new FilterRel(
                    rel.getCluster(),
                    rel,
                    extraFilter);
        }
        call.transformTo(rel);
    }

    private List<SargIntervalSequence> getIndexSargSeq(
        LcsIndexGuide indexGuide,
        FemLocalIndex index,
        int matchedPos,
        Map<CwmColumn, SargIntervalSequence> col2SeqMap)
    {
        List<SargIntervalSequence> seqList =
            new ArrayList<SargIntervalSequence>();

        for (int pos = 0; pos < matchedPos; pos++) {
            FemAbstractColumn col = indexGuide.getIndexColumn(index, pos);
            SargIntervalSequence sargSeq = col2SeqMap.get(col);
            seqList.add(sargSeq);
        }

        return seqList;
    }

    private List<List<CwmColumn>> getSargColLists(
        LcsRowScanRel origRowScan,
        List<SargBinding> sargBindingList,
        Map<CwmColumn, SargIntervalSequence> col2SeqMap)
    {
        List<List<CwmColumn>> retLists = new ArrayList<List<CwmColumn>>();
        ;
        List<CwmColumn> pointColumnList = new ArrayList<CwmColumn>();
        List<CwmColumn> rangeColumnList = new ArrayList<CwmColumn>();

        for (int i = 0; i < sargBindingList.size(); i++) {
            SargBinding sargBinding = sargBindingList.get(i);
            RexInputRef fieldAccess = sargBinding.getInputRef();
            FemAbstractColumn filterColumn =
                origRowScan.getColumnForFieldAccess(fieldAccess.getIndex());
            if (filterColumn != null) {
                SargIntervalSequence sargSeq = col2SeqMap.get(filterColumn);

                if (sargSeq.isPoint()) {
                    pointColumnList.add(filterColumn);
                } else {
                    rangeColumnList.add(filterColumn);
                }
            }
        }

        retLists.add(0, pointColumnList);
        retLists.add(1, rangeColumnList);
        return retLists;
    }

    private List<SargBinding> getResidualSargBinding(
        LcsRowScanRel origRowScan,
        List<SargBinding> sargBindingList,
        Map<FemLocalIndex, Integer> index2PosMap,
        List<SargBinding> nonResidualList)
    {
        LcsIndexGuide indexGuide = origRowScan.lcsTable.getIndexGuide();
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
            }
            else {
                nonResidualList.add(sargBindingList.get(i));
            }
        }

        for (FemLocalIndex index : index2PosMap.keySet()) {
            int maxPos = index2PosMap.get(index);
            for (int pos = 0; pos < maxPos; pos++) {
                int i =
                    sargColList.indexOf(indexGuide.getIndexColumn(index, pos));
                retSargBindingList.remove(i);
                sargColList.remove(i);
            }
        }

        return retSargBindingList;
    }


    private FennelRel newIndexRel(
        RelOptRuleCall call,
        LcsRowScanRel origRowScan,
        CandidateIndex candidate,
        FennelRelParamId startRidParamId,
        FennelRelParamId rowLimitParamId)
    {
        FemLocalIndex index = candidate.index;
        int matchedPos = candidate.matchedPos;
        List<SargIntervalSequence> sargSeqList = candidate.sargSeqList;

        FennelRelImplementor relImplementor =
            FennelRelUtil.getRelImplementor(origRowScan);
        LcsIndexGuide indexGuide = origRowScan.lcsTable.getIndexGuide();
        final RelTraitSet callTraits = call.rels[0].getTraits();

        assert (sargSeqList.size() == matchedPos);
        int indexKeyLength = index.getIndexedFeature().size();
        boolean partialMatch = matchedPos < indexKeyLength;

        RelDataType keyRowType =
            getIndexInputType(indexGuide, origRowScan, index, matchedPos);

        RelNode sargRel;

        sargRel =
            FennelRelUtil.convertSargExpr(
                callTraits,
                keyRowType,
                origRowScan.getCluster(),
                sargSeqList);

        boolean requireUnion =
            (
                sargSeqList.get(sargSeqList.size() - 1).isRange()
                || (
                    sargSeqList.get(sargSeqList.size() - 1).isPoint()
                    && partialMatch
                   )
            );

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

        FennelRelParamId startRidParamIdForSearch =
            requireUnion ? null : startRidParamId;
        FennelRelParamId rowLimitParamIdForSearch =
            requireUnion ? null : rowLimitParamId;

        LcsIndexSearchRel indexSearch =
            new LcsIndexSearchRel(
                origRowScan.getCluster(),
                keyInput,
                origRowScan.lcsTable,
                index,
                false,
                null,
                false,
                false,
                inputKeyProj,
                null,
                inputDirectiveProj,
                startRidParamIdForSearch,
                rowLimitParamIdForSearch);

        FennelRel inputRel = indexSearch;

        if (requireUnion) {
            FennelRelParamId chopperRidLimitParamId =
                relImplementor.allocateRelParamId();

            inputRel =
                new LcsIndexMergeRel(
                    origRowScan.lcsTable,
                    indexSearch,
                    startRidParamId,
                    rowLimitParamId,
                    chopperRidLimitParamId);
        }

        return inputRel;
    }

    /**
     * Create a type descriptor for the rows representing search keys along
     * with their directives, given an array of the attributes representing
     * the search columns. Note that we force the key type to nullable
     * because we use null for the representation of infinity (rather than
     * domain-specific values).
     *
     * @param indexGuide index guide
     * @param rel the table scan node that is being optimized
     * @param searchColumns array of column attributes representing the search
     * keys
     */
    static public RelDataType getSearchKeyRowType(
        LcsIndexGuide indexGuide,
        FennelRel rel,
        FemAbstractAttribute [] searchColumns)
    {
        FarragoPreparingStmt stmt = FennelRelUtil.getPreparingStmt(rel);
        FarragoTypeFactory typeFactory = stmt.getFarragoTypeFactory();
        RelDataType directiveType =
            typeFactory.createSqlType(
                SqlTypeName.Char,
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
     * keys from the index. Note that we force the key type to nullable
     * because we use null for the representation of infinity (rather than 
     * domain-specific values).
     * 
     * @param indexGuide index guide
     * @param rel the table scan node that is being optimized
     * @param index the index descriptor
     * @param matchedPos the length of matched prefix in the index key
     * 
     * @return type descriptor for the index search key
     */
    static public RelDataType getIndexInputType(
        LcsIndexGuide indexGuide,
        FennelRel rel,
        FemLocalIndex index,
        int matchedPos)
    {
        FemAbstractAttribute [] indexColumns = 
            new FemAbstractColumn[matchedPos];

        for (int pos = 0; pos < matchedPos; pos++) {
            indexColumns[pos] = (FemAbstractAttribute)
                indexGuide.getIndexColumn(index, pos);
        }

        RelDataType keyRowType =
            getSearchKeyRowType(indexGuide, rel, indexColumns);
        return keyRowType;
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class CandidateIndex
    {
        FemLocalIndex index;
        int matchedPos;
        List<SargIntervalSequence> sargSeqList;

        CandidateIndex(
            FemLocalIndex index,
            int matchedPos,
            List<SargIntervalSequence> sargSeqList)
        {
            this.index = index;
            this.matchedPos = matchedPos;
            this.sargSeqList = sargSeqList;
        }
    }
    
    /**
     * ColumnFilter is used to sort sargable column filters, based on the
     * selectivity of the filters.  A ColumnFilter is represented by the
     * column number and sargable interval sequence associated with the column.
     */
    private class ColumnFilter implements Comparator<ColumnFilter>
    {
        private int columnNumber;
        private SargIntervalSequence sargSeq;
        private RelStatSource tabStats;
        
        ColumnFilter(
            int columnNumber,
            SargIntervalSequence sargSeq,
            RelStatSource tabStats)
        {
            this.columnNumber = columnNumber;
            this.sargSeq = sargSeq;
            this.tabStats = tabStats;
        }
        
        public int compare(ColumnFilter cf1, ColumnFilter cf2)
        {
            // sort based on the selectivity if stats are available; otherwise,
            // just sort on column number to ensure that results are
            // deterministic
            Double colSel1 = computeSelectivity(cf1);
            Double colSel2 = computeSelectivity(cf2);
            if (colSel1 != null && colSel2 != null) {
                return
                    (colSel1 < colSel2) ? -1 :
                        ((colSel1 == colSel2) ? 0 : 1); 
            } else {
                return
                    (cf1.columnNumber < cf2.columnNumber) ? -1 :
                        ((cf1.columnNumber == cf2.columnNumber) ? 0 : 1);                  
            }
        }
        
        private Double computeSelectivity(ColumnFilter columnFilter)
        {
            RelStatColumnStatistics colStats = null;
            if (tabStats != null) {
                colStats =
                    tabStats.getColumnStatistics(
                        columnFilter.columnNumber,
                        columnFilter.sargSeq);
            }
            Double colSel = null;
            if (colStats != null) {
                colSel = colStats.getSelectivity();
            }
            return colSel;
        }       
    }
}

//End LcsIndexAccessRule.java
