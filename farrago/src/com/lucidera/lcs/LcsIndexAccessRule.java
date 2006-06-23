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

import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.query.*;
import net.sf.farrago.type.*;
import net.sf.farrago.cwm.relational.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sarg.*;
import org.eigenbase.sql.fun.*;

/**
 * LcsIndexAccessRule is a rule for converting FilterRel+LcsRowScanRel
 * into LcsIndexAccessRel (when the filter has the appropriate form).
 *
 * @author Rushan Chen
 * @version $Id$
 */
class LcsIndexAccessRule extends RelOptRule
{
    //~ Constructors ----------------------------------------------------------
    
    /**
     * Creates a new LcsIndexAccessRule object.
     */
    public LcsIndexAccessRule()
    {
        super(new RelOptRuleOperand(
            FilterRel.class,
            new RelOptRuleOperand [] {
                new RelOptRuleOperand(LcsRowScanRel.class, null)
            }));
    }
    
    //~ Methods ---------------------------------------------------------------
    
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
        // already been done for the predicates in this FilterRel, and LcsIndexSearchRel
        // and/or residual SargRel have been allocated as inputs to LcsRowScanRel.
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
    
    //~ Private Methods -------------------------------------------------------

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
    
    private void considerIndex(
        LcsRowScanRel origRowScan,
        List<SargBinding> sargBindingList,
        RelOptRuleCall call,
        SargRexAnalyzer rexAnalyzer)
    {
        FennelRelImplementor relImplementor = 
            FennelRelUtil.getRelImplementor(origRowScan);
        
        // By default, these parameters are not used:
        // ParamId == 0 menas this param is in valid.
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
        
        // Use a tree set here so that the indexes are searched in a fixed order, to
        // make the plan output stable.
        // Note we could optimize the order by using a different comparator function.
        // For example, search the index with the longest matched keys first, or when
        // proper costing is in place, search the most selective index (wrt to key values)
        // first.
        TreeSet<FemLocalIndex> indexSet = 
            new TreeSet<FemLocalIndex>(new LcsIndexGuide.IndexLengthComparator());
        
        indexSet.addAll(index2PosMap.keySet());
        
        if (indexSet.size() == 0) {
            // no index usable
            return;
        }
        
        List<SargBinding> residualSargBindingList = 
            getResidualSargBinding(
                origRowScan,
                sargBindingList,
                index2PosMap);
                
        // TODO: check for possibility of an index only scan
        if (indexSet.size() == 1 && residualSargBindingList.size() == 0) {
            // All filters can be answered by one index
            // check if this index also provides all the output for the query
        }
        
        // TODO since LcsRowScan does not support residual ranges now
        // change the residual into RexNodes and evaluate them in FilterRel
        // after all rows are retrieved.
        RexNode residualRexNode = 
            rexAnalyzer.getResidualSargRexNode(residualSargBindingList);
        
        RexNode postFilterRexNode =
            rexAnalyzer.getPostFilterRexNode();

        RexNode extraFilter = null;
        
        if (residualRexNode != null && postFilterRexNode != null) {
            extraFilter = 
                rexBuilder.makeCall(
                    SqlStdOperatorTable.andOperator,
                    residualRexNode, postFilterRexNode);
        } else if (residualRexNode != null) {
            extraFilter = residualRexNode;
        } else if (postFilterRexNode != null) {
            extraFilter = postFilterRexNode;
        }
        
        // AND the INDEX search rels together.
        RelNode [] indexRels = new RelNode[indexSet.size()];
        boolean requireIntersect = indexRels.length > 1;
        
        Iterator iter = indexSet.iterator();        
        int i = 0;
        
        if (requireIntersect) {
            // Allocate AND here
            rowLimitParamId = relImplementor.allocateRelParamId();
            startRidParamId = relImplementor.allocateRelParamId();
        }
        
        while (iter.hasNext()) {
            FemLocalIndex index = (FemLocalIndex)(iter.next());
            int matchedPos = index2PosMap.get(index);
            
            List<SargIntervalSequence> sargSeqList =
                getIndexSargSeq(indexGuide, index, matchedPos, col2SeqMap);
            
            CandidateIndex candidate = new CandidateIndex(
                index, matchedPos, sargSeqList);
            
            FennelRel indexRel = 
                newIndexRel(call,
                            origRowScan,
                            candidate,
                            startRidParamId,
                            rowLimitParamId);
            indexRels[i] = indexRel;
            i ++;
        }
        
        RelNode [] rowScanInputRels = new RelNode[1];
        
        if (requireIntersect) {
            FennelRel intersectRel = new LcsIndexIntersectRel(
                origRowScan.getCluster(),
                indexRels,
                origRowScan.lcsTable,
                startRidParamId,
                rowLimitParamId);
            rowScanInputRels[0] = intersectRel;
        } else {
            rowScanInputRels[0] = indexRels[0];
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
                false, false);
            
        transformCall(call, rowScan, extraFilter);
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

    private List<SargIntervalSequence> getIndexSargSeq (
        LcsIndexGuide indexGuide,
        FemLocalIndex index,
        int matchedPos,
        Map<CwmColumn, SargIntervalSequence> col2SeqMap)
    {
        List<SargIntervalSequence> seqList =
            new ArrayList<SargIntervalSequence>();
        
        for (int pos = 0; pos < matchedPos; pos++) {
            CwmColumn col = indexGuide.getIndexColumn(index, pos);
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
        List<List<CwmColumn>> retLists = new ArrayList<List<CwmColumn>>();;
        List<CwmColumn> pointColumnList = new ArrayList<CwmColumn>(); 
        List<CwmColumn> rangeColumnList = new ArrayList<CwmColumn>(); 
        
        for (int i = 0; i < sargBindingList.size(); i ++) {
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
        Map<FemLocalIndex, Integer> index2PosMap)
    {
        LcsIndexGuide indexGuide = origRowScan.lcsTable.getIndexGuide();
        List<CwmColumn> sargColList = new ArrayList<CwmColumn>();
        
        for (int i = 0; i < sargBindingList.size(); i ++) {
            SargBinding sargBinding = sargBindingList.get(i);
            RexInputRef fieldAccess = sargBinding.getInputRef();
            FemAbstractColumn filterColumn =
                origRowScan.getColumnForFieldAccess(fieldAccess.getIndex());
            if (filterColumn != null) {
                sargColList.add(i, filterColumn);
            }
        }
        
        Iterator iter = index2PosMap.keySet().iterator();
        
        while (iter.hasNext()) {
            FemLocalIndex index = (FemLocalIndex)(iter.next());
            
            for (int pos = 0; pos < index2PosMap.get(index).intValue(); pos++) {
                int i = sargColList.indexOf(indexGuide.getIndexColumn(index, pos));
                sargBindingList.remove(i);
                sargColList.remove(i);
            }
        }
        
        return sargBindingList;
    }
    
    private FennelRel newIndexRel (
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

        // Create a type descriptor for the rows representing search
        // keys along with their directives.  Note that we force
        // the key type to nullable because we use null for the representation
        // of infinity (rather than domain-specific junk).
        FarragoPreparingStmt stmt = FennelRelUtil.getPreparingStmt(origRowScan);
        FarragoTypeFactory typeFactory = stmt.getFarragoTypeFactory();
        RelDataType directiveType =
            typeFactory.createSqlType(
                SqlTypeName.Char,
                1);
        
        int keyRowLength = matchedPos * 2 + 2;
        int keyRowMidPoint = keyRowLength / 2;
        int lowerBoundKeyBase = 1;
        int upperBoundKeyBase = keyRowMidPoint + 1;
        
        RelDataType[] dataTypes = new RelDataType[keyRowLength];
        String[]      typeDescriptions = new String[keyRowLength];
        
        dataTypes[0] = directiveType;
        dataTypes[keyRowMidPoint] = directiveType;
        
        typeDescriptions[0] = "lowerBoundDirective";
        typeDescriptions[keyRowMidPoint] = "upperBoundDirective";
        
        for (int pos = 0; pos < matchedPos; pos ++) {
            CwmColumn filterColumn = 
                indexGuide.getIndexColumn(index, pos);
        
            RelDataType keyType =
                typeFactory.createTypeWithNullability(
                    typeFactory.createCwmElementType((FemAbstractColumn)filterColumn),
                    true);
            dataTypes[lowerBoundKeyBase + pos] = keyType;
            dataTypes[upperBoundKeyBase + pos] = keyType;
            
            typeDescriptions[lowerBoundKeyBase + pos] = "lowerBoundKey";
            typeDescriptions[upperBoundKeyBase + pos] = "upperBoundKey";
            
        }
            
        RelDataType keyRowType =
            typeFactory.createStructType(dataTypes, typeDescriptions);

        RelNode sargRel;
        
        sargRel = FennelRelUtil.convertSargExpr(
            callTraits,
            keyRowType,
            origRowScan.getCluster(),
            sargSeqList);
        
        boolean requireUnion =
            (sargSeqList.get(sargSeqList.size() - 1).isRange() ||
            (sargSeqList.get(sargSeqList.size() - 1).isPoint() && partialMatch));
        
        RelNode keyInput =
            mergeTraitsAndConvert(
                callTraits, FennelRel.FENNEL_EXEC_CONVENTION,
                sargRel);
        
        assert (keyInput != null);
        
        // Set up projections for the search directive and key.
        Integer [] inputDirectiveProj = new Integer [] { 0, (matchedPos + 1) };
        Integer [] inputKeyProj = new Integer [matchedPos * 2];
        for (int i = 0; i < matchedPos; i ++) {
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
            
            inputRel = new LcsIndexMergeRel(
                origRowScan.lcsTable,
                indexSearch,
                startRidParamId,
                rowLimitParamId,
                chopperRidLimitParamId);
        }
        
        return inputRel;
    }
}

//End LcsIndexAccessRule.java
