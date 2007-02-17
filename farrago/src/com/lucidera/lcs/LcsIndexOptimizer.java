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
import java.util.logging.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.keysindexes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.query.*;
import net.sf.farrago.trace.*;

import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.sarg.*;
import org.eigenbase.sql.*;
import org.eigenbase.stat.*;
import org.eigenbase.trace.*;
import org.eigenbase.util.*;

import com.lucidera.query.*;


/**
 * LcsIndexOptimizer optimizes the access path to use index based on cost.
 * 
 * @author Rushan Chen
 * @version $Id$
 */
public class LcsIndexOptimizer
{
    private static Double IOCostPerBlock = 1.0;
    private static Double SetOpCostPerBlock = 14.0;
    private static Double FilterEvalCostPerMillionRow = 82.0;
    private static Double SortCostConstant = 0.000032;
    private static Double ColumnCorrelationFactor = 0.5;
    
    public LcsIndexOptimizer()
    {
    }
    
    public static List<FemLocalIndex> getUnclusteredIndexes(LcsRowScanRel rowScan)
    {
        return 
        FarragoCatalogUtil.getUnclusteredIndexes(
            rowScan.lcsTable.getPreparingStmt().getRepos(),
            rowScan.lcsTable.getCwmColumnSet());
    }
    
    public static boolean isValid(FemLocalIndex index)
    {
        return index.getVisibility() == VisibilityKindEnum.VK_PUBLIC;
    }
    
    /**
     * Get the column at a given index key position.
     * 
     * @param index the index whose key contains the column
     * @param position the index key position for the column
     * @return null if the position specified is invalid
     */
    public static FemAbstractColumn getIndexColumn(
        FemLocalIndex index,
        int position)
    {
        List<CwmIndexedFeature> indexedFeatures = index.getIndexedFeature();
        
        if ((position < 0) || (position >= indexedFeatures.size())) {
            return null;
        }
        
        CwmIndexedFeature indexedFeature = indexedFeatures.get(position);
        
        return (FemAbstractColumn) indexedFeature.getFeature();
    }
    
    /**
     * Find the index position for an indexed column col
     * 
     * @param index index that could contain col in its key
     * @param col the column for which to look up index position
     * @return index key position, or -1 if the column is not part of the index key
     */
    private static int getIndexColumnPos(
        FemLocalIndex index,
        FemAbstractColumn col)
    {
        List<CwmIndexedFeature> indexedFeatures = index.getIndexedFeature();
        
        int i;
        for (i = 0; i < indexedFeatures.size(); i ++) {
            if (indexedFeatures.get(i).getFeature() == col) {
                return i; 
            }
        }
        
        return -1;
        
    }
    
    private static SargColumnFilter findSargFilterForColumn(
        LcsRowScanRel rowScanRel,
        List<SargColumnFilter> filterList,
        FemAbstractColumn col)
    {
        for (SargColumnFilter filter : filterList) {
            if (rowScanRel.getColumnForFieldAccess(filter.columnPos) == col) {
                return filter;
            }
        }
        
        return null;
    }
    
    /**
     * Search for a projection of a bitmap index that satisfies the row scan. If
     * such a projection exists, return the projection, with bitmap columns
     * appended.
     *
     * @param index the index which is to be projected
     *
     * @return a projection on the index that satisfies the columns of the row
     * scan and includes bitmap data, or null if a satisfying projection could
     * not be found
     */
    public static Integer [] findIndexOnlyProjection(
        LcsRowScanRel rowScan,
        FemLocalIndex index)
    {
        // determine columns to be satisfied
        Integer [] proj = rowScan.projectedColumns;
        if (proj == null) {
            proj =
                FennelRelUtil.newIotaProjection(
                    rowScan.getRowType().getFieldCount());
        }
        
        // find available columns
        List<FemAbstractColumn> idxCols = new LinkedList<FemAbstractColumn>();
        for (CwmIndexedFeature indexedFeature : index.getIndexedFeature()) {
            idxCols.add((FemAbstractColumn) indexedFeature.getFeature());
        }
        
        // find projection
        List<Integer> indexProj = new ArrayList<Integer>();        
        final List<FemAbstractColumn> columns =
            Util.cast(
                rowScan.lcsTable.getCwmColumnSet().getFeature(),
                FemAbstractColumn.class);
        
        for (int i = 0; i < proj.length; i++) {
            // TODO: handle lcs rid
            if (LucidDbSpecialOperators.isLcsRidColumnId(proj[i])) {
                return null;
            }
            FemAbstractColumn keyCol = columns.get(proj[i]);
            int next = idxCols.indexOf(keyCol);
            if (next == -1) {
                return null;
            }
            indexProj.add(next);
        }
        
        // add bitmap columns (which follow the index key columns)
        int nKeys = idxCols.size();
        for (int i = nKeys; i < (nKeys + 3); i++) {
            indexProj.add(i);
        }
        Integer [] projArray = new Integer[indexProj.size()];
        return indexProj.toArray(projArray);
    }
    
    /**
     * This is the algorithm that maps indexes to search key columns. It does so
     * by finding the shortest (in terms of key length) index to map to the
     * longest list of columns(in the case of composite key idnexes). The index
     * selection is expressed using a map where all selected indexes and the
     * matched key positions are remembered.
     *
     * @param colLists two column lists: the first one contains the "point
     * interval" columns, the secod one contains the "range interval" columns.
     *
     * @return a map from selected index to its associated matched key position.
     */
    public static Map<FemLocalIndex, Integer> getIndex2MatchedPosMap(
        LcsRowScanRel rowScan,
        List<List<CwmColumn>> colLists)
        {
        assert (colLists.size() == 2);
        
        List<CwmColumn> pointColumnList = colLists.get(0);
        List<CwmColumn> rangeColumnList = colLists.get(1);
        
        Map<FemLocalIndex, Integer> index2PosMap =
            new HashMap<FemLocalIndex, Integer>();
        boolean matchedAll = false;
        
        // Sort the index based on length(and a unique identifier to break
        // ties), so that index with fewer key columns are searched first(and
        // preferred).
        TreeSet<FemLocalIndex> indexSet =
            new TreeSet<FemLocalIndex>(new IndexLengthComparator());
        
        indexSet.addAll(getUnclusteredIndexes(rowScan));
        
        // First process the columns with point predicates.
        // Objective is to maximize the index key columns matched.
        while ((pointColumnList.size() > 0) && !matchedAll) {
            // TODO: match the shortest index with the maximum matched positions
            int maxMatchedPos = 0;
            FemLocalIndex maxMatchedIndex = null;
            int matchedPos = 0;
            
            for (FemLocalIndex index : indexSet) {
                if (isValid(index)) {
                    matchedPos = 0;
                    
                    CwmColumn col = getIndexColumn(index, matchedPos);
                    
                    while ((col != null) && pointColumnList.contains(col)) {
                        matchedPos++;
                        col = getIndexColumn(index, matchedPos);
                    }
                    
                    // try to match one more column from the interval column
                    // list
                    if (rangeColumnList.contains(
                        getIndexColumn(index, matchedPos))) {
                        matchedPos++;
                    }
                    
                    // Pick the index with the biggest matchedPos.
                    if (maxMatchedPos < matchedPos) {
                        maxMatchedPos = matchedPos;
                        maxMatchedIndex = index;
                    }
                }
            }
            
            if (maxMatchedIndex != null) {
                // Find a maximum matched index, from the set of indexes to use
                // and columns to match
                for (int i = 0; i < maxMatchedPos; i++) {
                    // remember which index a column is mapped to.
                    CwmColumn matchedCol = getIndexColumn(maxMatchedIndex, i);
                    
                    // remove matched columns from the set.
                    if (!pointColumnList.remove(matchedCol)) {
                        // last column might come from the interval list.
                        boolean removed = rangeColumnList.remove(matchedCol);
                        assert (removed);
                    }
                }
                
                // remove matched index from the set.
                indexSet.remove(maxMatchedIndex);
                
                // remember for each matched index, how many positions are
                // matched.
                index2PosMap.put(
                    maxMatchedIndex,
                    new Integer(maxMatchedPos));
            } else {
                // no more match possible, get out of here
                matchedAll = true;
            }
        }
        
        Iterator<FemLocalIndex> iter = indexSet.iterator();
        
        // Process the columns with range predicates:
        // Simply assign the shortest index with matching first key column
        int maxMatchedPos = 1;
        while ((rangeColumnList.size() > 0) && iter.hasNext()) {
            FemLocalIndex index = iter.next();
            CwmColumn firstCol = getIndexColumn(index, maxMatchedPos - 1);
            if ((firstCol != null) && rangeColumnList.contains(firstCol)) {
                index2PosMap.put(
                    index,
                    maxMatchedPos);
                rangeColumnList.remove(firstCol);
                iter.remove();
            }
        }
        
        return index2PosMap;
        }
    
    /**
     * Find the best filter to index mapping based on cost
     * 
     * @param rowScan rowScan to consider index access path for
     * @param colLists list of sarg column filters
     * @return
     */
    public static Map<FemLocalIndex, Integer> getBestIndex2MatchedPosMap(
        LcsRowScanRel rowScanRel,
        List<List<SargColumnFilter>> filterLists)
        {
        final Logger tracer = FarragoTrace.getOptimizerRuleTracer();
        
        assert (filterLists.size() == 2);
        
        List<SargColumnFilter> pointList = filterLists.get(0);
        List<SargColumnFilter> intervalList = filterLists.get(1);
        
        Map<SargColumnFilter, FemLocalIndex> filter2IndexMap =
            new HashMap<SargColumnFilter, FemLocalIndex>();
        Map<FemLocalIndex, Integer> index2MatchedPosMap =
            new HashMap<FemLocalIndex, Integer>();
        
        Map<SargColumnFilter, FemLocalIndex> bestFilter2IndexMap =
            new HashMap<SargColumnFilter, FemLocalIndex>();
        Map<FemLocalIndex, Integer> bestIndex2MatchedPosMap =
            new HashMap<FemLocalIndex, Integer>();
        
        // get residual filter cost and use that as first best cost
        Double bestCost = null;
        
        getBestIndex(
            rowScanRel,
            pointList,
            intervalList,
            filter2IndexMap,
            index2MatchedPosMap,
            bestFilter2IndexMap,
            bestIndex2MatchedPosMap,
            bestCost);
        
        if (bestCost != null &&
            tracer.isLoggable(Level.FINEST)) {
            tracer.finest("The following mapping has the best cost of " + bestCost);
            for (SargColumnFilter filter : bestFilter2IndexMap.keySet()) {
                tracer.finest("Filter Column " + filter.columnPos +
                    " maps to Index " + bestFilter2IndexMap.get(filter).getStorageId());
            }
        }
        
        return bestIndex2MatchedPosMap;
        }
    
    /**
     * 
     * @param rowScan
     * @param colLists
     * @return
     */
    public static void getBestIndex(
        LcsRowScanRel rowScanRel,
        List<SargColumnFilter> pointList,
        List<SargColumnFilter> intervalList,
        Map<SargColumnFilter, FemLocalIndex> filter2IndexMap,
        Map<FemLocalIndex, Integer> index2MatchedPosMap,
        Map<SargColumnFilter, FemLocalIndex> bestFilter2IndexMap,
        Map<FemLocalIndex, Integer> bestIndex2MatchedPosMap,
        Double bestCost)
    {
        boolean matchedAllPointFilters = true;
        
        List<FemLocalIndex> indexList = getUnclusteredIndexes(rowScanRel);
        HashSet<FemAbstractColumn> mappedPointColumns = 
            new HashSet<FemAbstractColumn> ();
        
        for (SargColumnFilter filter : filter2IndexMap.keySet()) {
            mappedPointColumns.add(rowScanRel.getColumnForFieldAccess(
                filter.columnPos));
        }
        
        // match point ranges
        for (SargColumnFilter pointFilter : pointList) {
            if (filter2IndexMap.containsKey(pointFilter)) {
                // if column filter is already mapped
                continue;
            }
            
            FemAbstractColumn pointColumn =
                rowScanRel.getColumnForFieldAccess(pointFilter.columnPos);
            
            // for all indexes try fit this filter column in
            for (FemLocalIndex index : indexList) {
                int matchedPos = getIndexColumnPos(index, pointColumn);
                
                if (matchedPos >= 0) {
                    // find all filters mapped to this index
                    mappedPointColumns.clear();
                    for (SargColumnFilter filter : filter2IndexMap.keySet()) {
                        // only add point filters
                        if (pointList.contains(filter) &&
                            filter2IndexMap.get(filter).equals(index)) {
                            mappedPointColumns.add(rowScanRel.getColumnForFieldAccess(
                                filter.columnPos));
                        }
                    }
                    
                    // Found a matched position.
                    // now check all prior positions have been matched(and are point filters)
                    // for this index
                    int pos;
                    for (pos = 0; pos < matchedPos; pos ++) {
                        CwmStructuralFeature
                        feature = index.getIndexedFeature().get(pos).getFeature();
                        if (!mappedPointColumns.contains(feature)) {
                            break;
                        }
                    }
                    
                    if (pos == matchedPos) {
                        // all prior positions matched, keep going
                        matchedAllPointFilters = false;
                        filter2IndexMap.put(pointFilter, index);
                        index2MatchedPosMap.put(index, matchedPos + 1);
                        getBestIndex(
                            rowScanRel,
                            pointList,
                            intervalList,
                            filter2IndexMap,
                            index2MatchedPosMap,
                            bestFilter2IndexMap,                        
                            bestIndex2MatchedPosMap,
                            bestCost);
                    }
                } // else try the next index
            }
            filter2IndexMap.remove(pointFilter);
        }
        
        // next try to match the rangeFilters if any
        
        // now we've matched every possible index in this recursion
        // calculate the cost and update bestIndex2MatchedPosMap and bestCost
        // if the new cost is lower
        if (matchedAllPointFilters) {
            // Note that if there exist point filters that can be mapped to
            // indexes, this algorithm will always try to match some point
            // filters before mapping the interval filters. A mapping with
            // matches only interval filters to indexes is not generated, even
            // though it is a legitimate candidate mapping. This limitation of
            // cost space is fine because applying additional point filters via
            // indexes should always reduce the cost of scan.
            getBestIndexWithIntervalFilter(
                rowScanRel,
                pointList,
                intervalList,
                filter2IndexMap,
                index2MatchedPosMap,
                bestFilter2IndexMap,
                bestIndex2MatchedPosMap,
                bestCost);
        }
    }
    
    public static void getBestIndexWithIntervalFilter(
        LcsRowScanRel rowScanRel,
        List<SargColumnFilter> pointList,
        List<SargColumnFilter> intervalList,
        Map<SargColumnFilter, FemLocalIndex> filter2IndexMap,
        Map<FemLocalIndex, Integer> index2MatchedPosMap,
        Map<SargColumnFilter, FemLocalIndex> bestFilter2IndexMap,
        Map<FemLocalIndex, Integer> bestIndex2MatchedPosMap,
        Double bestCost)
    {
        final Logger tracer = FarragoTrace.getOptimizerRuleTracer();
        
        boolean matchedAllIntervalFilters = true;
        
        List<FemLocalIndex> indexList = getUnclusteredIndexes(rowScanRel);
        HashSet<FemAbstractColumn> mappedPointColumns = 
            new HashSet<FemAbstractColumn> ();
        
        // match interval filters
        for (SargColumnFilter intervalFilter : intervalList) {
            if (filter2IndexMap.containsKey(intervalFilter)) {
                // if column filter is already mapped
                continue;
            }
            
            FemAbstractColumn intervalColumn =
                rowScanRel.getColumnForFieldAccess(intervalFilter.columnPos);
            
            // for all indexes try fit this filter column in
            for (FemLocalIndex index : indexList) {
                
                int matchedPos = getIndexColumnPos(index, intervalColumn);
                
                if (matchedPos >= 0) {
                    // find all filters mapped to this index
                    mappedPointColumns.clear();
                    for (SargColumnFilter filter : filter2IndexMap.keySet()) {
                        // only add point filters
                        if (pointList.contains(filter) &&
                            filter2IndexMap.get(filter).equals(index)) {
                            mappedPointColumns.add(rowScanRel.getColumnForFieldAccess(
                                filter.columnPos));
                        }
                    }
                    
                    // Found a matched position.
                    // now check all prior positions have been matched(and are point filters)
                    // for this index
                    int pos;
                    for (pos = 0; pos < matchedPos; pos ++) {
                        CwmStructuralFeature
                        feature = index.getIndexedFeature().get(pos).getFeature();
                        if (!mappedPointColumns.contains(feature)) {
                            break;
                        }
                    }
                    
                    if (pos == matchedPos) {
                        // all prior positions matched, keep going
                        matchedAllIntervalFilters = false;
                        filter2IndexMap.put(intervalFilter, index);
                        index2MatchedPosMap.put(index, matchedPos + 1);
                        getBestIndexWithIntervalFilter(
                            rowScanRel,
                            pointList,
                            intervalList,
                            filter2IndexMap,
                            index2MatchedPosMap,
                            bestFilter2IndexMap,                        
                            bestIndex2MatchedPosMap,
                            bestCost);
                    }
                }// else try the next index
            }
            filter2IndexMap.remove(intervalFilter);
        }
        
        if (matchedAllIntervalFilters) {
            Double currentCost = 
                costIndexAccess(rowScanRel, pointList, intervalList,
                    filter2IndexMap, index2MatchedPosMap);
            
            if (currentCost != null) {                
                if (bestCost == null ||
                    bestCost > currentCost) {
                    bestCost = currentCost;
                    
                    bestFilter2IndexMap.clear();
                    bestIndex2MatchedPosMap.clear();
                    
                    bestFilter2IndexMap.putAll(filter2IndexMap);
                    bestIndex2MatchedPosMap.putAll(index2MatchedPosMap);
                    
                    if (tracer.isLoggable(Level.FINEST)) {
                        tracer.finest("Current best cost is " + bestCost);
                    }
                }
            }
        }
        return;
    }
    
    public static Double costIndexAccess(
        LcsRowScanRel rowScanRel,
        List<SargColumnFilter> pointList,
        List<SargColumnFilter> intervalList,
        Map<SargColumnFilter, FemLocalIndex> filter2IndexMap,
        Map<FemLocalIndex, Integer> index2MatchedPosMap)
    {
        final Logger tracer = FarragoTrace.getOptimizerRuleTracer();
        
        if (tracer.isLoggable(Level.FINEST)) {
            tracer.finest("Costing the following candidate mapping ");
            for (SargColumnFilter filter : filter2IndexMap.keySet()) {
                tracer.finest("Filter Column " + filter.columnPos +
                    " maps to Index " + filter2IndexMap.get(filter).getStorageId());
            }
        }
        
        Double cost = 0.0;
        
        List<SargColumnFilter> residualFilterList = new ArrayList<SargColumnFilter> ();
        
        // residual list starts out having all the filters in it
        residualFilterList.addAll(pointList);
        residualFilterList.addAll(intervalList);
        
        // cost the index access
        if (!filter2IndexMap.isEmpty()) {
            Map<FemLocalIndex, SargColumnFilter> index2LastFilterMap =
                new HashMap<FemLocalIndex, SargColumnFilter> ();
            
            for (FemLocalIndex index : index2MatchedPosMap.keySet()) {
                FemAbstractColumn lastMatchedKeyCol = 
                    getIndexColumn(index, index2MatchedPosMap.get(index) - 1);
                SargColumnFilter filter = 
                    findSargFilterForColumn(rowScanRel, residualFilterList, lastMatchedKeyCol);
                
                assert (filter != null);
                index2LastFilterMap.put(index, filter);
            }
            
            Double indexSearchCost =
                getIndexSearchCost(rowScanRel, index2MatchedPosMap, index2LastFilterMap);
            
            if (indexSearchCost == null) {
                return null;
            }
            
            cost += indexSearchCost;
            
            // prepare the residual lists
            residualFilterList.removeAll(filter2IndexMap.keySet());
        }
        
        // cost the residual filter access
        Double residualScanCost =
            getTableScanCostWithResidual(rowScanRel, filter2IndexMap.keySet(), residualFilterList);
        
        if (residualScanCost == null) {
            return null;
        }
        
        cost += residualScanCost;
        
        if (tracer.isLoggable(Level.FINEST)) {
            tracer.finest("This mapping has a cost of " + cost);
        }
        
        return cost;
    }
    
    /**
     * 
     * @param sargBindingList
     * @return
     */
    public static Map<CwmColumn, SargIntervalSequence> getCol2SeqMap(
        LcsRowScanRel rowScan,
        List<SargBinding> sargBindingList)
        {
        Map<CwmColumn, SargIntervalSequence> colMap =
            new HashMap<CwmColumn, SargIntervalSequence>();
        
        for (int i = 0; i < sargBindingList.size(); i++) {
            SargBinding sargBinding = sargBindingList.get(i);
            RexInputRef fieldAccess = sargBinding.getInputRef();
            FemAbstractColumn filterColumn =
                rowScan.getColumnForFieldAccess(fieldAccess.getIndex());
            if (filterColumn != null) {
                SargIntervalSequence sargSeq =
                    FennelRelUtil.evaluateSargExpr(sargBinding.getExpr());
                
                colMap.put(filterColumn, sargSeq);
            }
        }
        
        return colMap;
        }
    
    private static Double getIndexSearchCost(
        LcsRowScanRel rowScanRel,
        Map<FemLocalIndex, Integer> index2MatchedPosMap,
        Map<FemLocalIndex, SargColumnFilter> index2LastFilterMap)
    {        
        Double cost = 0.0;
        
        assert (index2MatchedPosMap.size() == index2LastFilterMap.size());
        
        for (FemLocalIndex index : index2MatchedPosMap.keySet()) {
            int mappedPos = index2MatchedPosMap.get(index);
            SargColumnFilter lastFilter = index2LastFilterMap.get(index);
            
            Double scanCost = 
                getIndexBitmapScanCost(rowScanRel, index, mappedPos, lastFilter);
            
            Double mergeCost =
                getIndexBitmapBitOpCost(rowScanRel, index, mappedPos, lastFilter);
            
            Double sortCost =
                getIndexBitmapSortCost(rowScanRel);
            
            if (scanCost == null || mergeCost == null || sortCost == null) {
                return null;
            }
            cost += scanCost + mergeCost + sortCost; 
        }
        
        // add the cost for scanning the deletion index
        FemLocalIndex deletionIndex =
            FarragoCatalogUtil.getDeletionIndex(
                rowScanRel.lcsTable.getPreparingStmt().getRepos(),
                rowScanRel.lcsTable.getCwmColumnSet());
        
        if (deletionIndex != null) {
            Double scanCost =
                getIndexBitmapScanCost(rowScanRel, deletionIndex, 0, null);
            
            if (scanCost == null) {
                return null;
            }
            
            cost += scanCost;
        }
        
        // add the final cost of intersecting the final bitmaps
        Double intersectCost =
            getIndexBitmapBitOpCost(rowScanRel, index2MatchedPosMap.size());
        
        if (intersectCost == null) {
            return null;
        }
        
        cost += intersectCost;
        
        return cost;
    }
    
    private static Double getIndexBitmapScanCost(
        LcsRowScanRel rowScanRel,
        FemLocalIndex index,
        int mappedPos,
        SargColumnFilter lastFilter)
    {
        Long blockCount = index.getPageCount();
        
        if (blockCount == null) {
            return null;
        }
        
        Double cost = IOCostPerBlock * blockCount;
        
        if (mappedPos <= 0) {
            // scan the entire index
            // for example in the case of deletion index
            return cost;
        }
        
        Double scannedBitmapCount = 
            getIndexBitmapCount(rowScanRel, index, mappedPos, lastFilter);
        Double totalBitmapCount =
            getIndexBitmapCount(rowScanRel, index, 0, null);            
        
        if (scannedBitmapCount == null || totalBitmapCount == null) {
            // assume the entire index is scanned
            return cost;
        }
        
        // only part of an index is scanned
        // NOTE: this formula is not quite accurate in that it assumes
        // the percentage of nonleaf blocks scanned is the same as the
        // percentage of leaf blocks.
        cost = cost * scannedBitmapCount / totalBitmapCount;
        
        return cost;
    }
    
    private static Double getIndexBitmapBitOpCost(
        LcsRowScanRel rowScanRel,
        FemLocalIndex index,
        int mappedPos,
        SargColumnFilter lastFilter)
    {
        Double cost = 0.0;
        Double scannedBitmapCount = 
            getIndexBitmapCount(rowScanRel, index, mappedPos, lastFilter);
        
        if (scannedBitmapCount == null) {
            return null;
        }
        RelStatSource tabStats =
            RelMetadataQuery.getStatistics(rowScanRel);
        
        if (tabStats == null) {
            return null;
        }
        
        Double rowCount = tabStats.getRowCount();
        
        if (rowCount == null) {
            return null;
        }
        
        int blockSize = 
            rowScanRel.lcsTable.getPreparingStmt().getRepos().
            getCurrentConfig().getFennelConfig().getCachePageSize();
        
        cost = SetOpCostPerBlock * scannedBitmapCount * rowCount / blockSize;
        
        return cost;
    }
    
    private static Double getIndexBitmapBitOpCost(
        LcsRowScanRel rowScanRel,
        int indexesUsed)
    {
        Double cost = 0.0;
        
        RelStatSource tabStats =
            RelMetadataQuery.getStatistics(rowScanRel);
        
        if (tabStats == null) {
            return null;
        }
        
        Double rowCount = tabStats.getRowCount();
        
        if (rowCount == null) {
            return null;
        }
        
        int blockSize = 
            rowScanRel.lcsTable.getPreparingStmt().getRepos().
            getCurrentConfig().getFennelConfig().getCachePageSize();
        
        cost = SetOpCostPerBlock * indexesUsed * rowCount / blockSize;
        
        return cost;
    }
    
    private static Double getIndexBitmapSortCost(
        LcsRowScanRel rowScanRel)
    {
        Double cost = 0.0;
        
        RelStatSource tabStats =
            RelMetadataQuery.getStatistics(rowScanRel);
        
        if (tabStats == null) {
            return null;
        }
        
        Double rowCount = tabStats.getRowCount();
        
        if (rowCount == null) {
            return null;
        }
        
        if (rowCount <= 1.0) {
            // no sort needed
            cost = 0.0;
        } else {
            cost = SortCostConstant * rowCount * Math.log(rowCount);
        }
        
        return cost;
    }
    
    private static Double getIndexBitmapCount(
        LcsRowScanRel rowScanRel,
        FemLocalIndex index,
        int mappedPos,
        SargColumnFilter lastFilter)
    {
        Double bitmapCount = null;
        RelStatSource tabStats =
            RelMetadataQuery.getStatistics(rowScanRel);
        
        List<CwmIndexedFeature> indexedFeatures = index.getIndexedFeature();
        
        int i;
        int indexKeyLength = indexedFeatures.size();
        
        // starting from the last mapped postition (the index is mappedPos - 1)
        // multiple the current bitmap count by the CND of the index column with a
        // discount of correlationFactor.
        if (mappedPos > 0) {
            FemAbstractColumn lastFilterColumn = 
                rowScanRel.getColumnForFieldAccess(lastFilter.columnPos);
            assert (indexedFeatures.get(mappedPos - 1).getFeature() == lastFilterColumn);
            // this is the last filter
            if (lastFilter.isPoint()) {
                // is point filter
                bitmapCount = 1.0;
            } else {
                // is interval filter
                if (tabStats == null) {
                    return null;
                }
                
                RelStatColumnStatistics colStats = null;
                
                colStats =
                    tabStats.getColumnStatistics(
                        lastFilter.columnPos,
                        lastFilter.sargSeq);
                if (colStats == null) {
                    // no stats
                    // assume a single bitmap
                    bitmapCount = 1.0;
                } else {
                    Double lastFilterColumnCND = colStats.getCardinality();
                    if (lastFilterColumnCND == null) {
                        // no stats
                        // assume a single bitmap
                        bitmapCount = 1.0;
                    } else {
                        bitmapCount = lastFilterColumnCND;
                    }
                }
            }
        } else {
            bitmapCount = 1.0;
        }
        
        if (mappedPos < indexKeyLength) {
            if (tabStats == null) {
                return null;
            }
            
            for (i = mappedPos; i < indexKeyLength; i ++) {
                RelStatColumnStatistics colStats = null;
                
                int ordinal =
                    rowScanRel.lcsTable.getCwmColumnSet().getFeature().indexOf(
                        indexedFeatures.get(i).getFeature());
                
                colStats =
                    tabStats.getColumnStatistics(ordinal, null);
                
                if (colStats == null) {
                    return null;
                } else {
                    Double indexColumnCND = colStats.getCardinality();
                    if (indexColumnCND == null) {
                        return null;
                    }
                    if (indexColumnCND < 1/ColumnCorrelationFactor) {
                        indexColumnCND = 1.0;
                    } else {
                        indexColumnCND *= ColumnCorrelationFactor;
                    }
                    bitmapCount *= indexColumnCND;
                }
            }
        }
        
        return bitmapCount;
    }
    
    private static Double getTableScanCostWithResidual(
        LcsRowScanRel rowScanRel,
        Set<SargColumnFilter> indexSearchFilterList,
        List<SargColumnFilter> residualFilterList)
    {
        Double cost = 0.0;
        
        int tableColCount =
            rowScanRel.lcsTable.getCwmColumnSet().getFeature().size();
        int residualColCount = residualFilterList.size();
        int nonResidualColCount = tableColCount - residualColCount;
        
        RelStatSource tabStat = 
            RelMetadataQuery.getStatistics(rowScanRel);
        
        Double indexSearchSelectivity = 1.0;
        Double residualFilterSelectivity = 1.0;
        
        if (tabStat == null) {
            return null;
        }
        
        for (SargColumnFilter filter : indexSearchFilterList) {
            Double filterSelectivity = filter.getSelectivity(tabStat);
            
            if (filterSelectivity == null) {
                return null;
            }
            
            if (filterSelectivity > ColumnCorrelationFactor) {
                filterSelectivity = 1.0;
            } else {
                filterSelectivity /= ColumnCorrelationFactor;
                indexSearchSelectivity *= filterSelectivity;
            }
        }
        
        for (SargColumnFilter filter : residualFilterList) {
            Double filterSelectivity = filter.getSelectivity(tabStat);
            
            if (filterSelectivity == null) {
                return null;
            }
            
            if (filterSelectivity > ColumnCorrelationFactor) {
                filterSelectivity = 1.0;
            } else {
                filterSelectivity /= ColumnCorrelationFactor;
            }
            residualFilterSelectivity *= filterSelectivity;
        }
        
        Double rowCountWithIndexSearch =
            tabStat.getRowCount() * indexSearchSelectivity;
        
        int blockSize = 
            rowScanRel.lcsTable.getPreparingStmt().getRepos().
            getCurrentConfig().getFennelConfig().getCachePageSize();
        
        // estimate "average col size"
        // NOTE: lcsTableBlockCount here includes both index leaf blocks as well
        // as non-leaf blocks. This is okay because the non-leaf blocks contribute
        // to the scan cost too.
        Double lcsTableBlockCount = 0.0;
        for (FemLocalIndex index : rowScanRel.lcsTable.getClusteredIndexes()) {
            Long pageCount = index.getPageCount();
            
            if (pageCount == null) {
                return null;
            }
            lcsTableBlockCount += pageCount;
        }
        
        Double avgColLength = 
            lcsTableBlockCount * blockSize / (tableColCount * tabStat.getRowCount());
        
        Double scanCost =
            IOCostPerBlock *
            rowCountWithIndexSearch * avgColLength *
            (((residualColCount + 1) * (1 + residualFilterSelectivity) / 2 
                + (nonResidualColCount - 1) * residualFilterSelectivity)
                / blockSize);
        
        Double filterEvalCost =
            (FilterEvalCostPerMillionRow / 1000000.0) *
            rowCountWithIndexSearch * residualColCount * (1 + residualFilterSelectivity) / 2;
        
        cost = scanCost + filterEvalCost;
        
        return cost;
    }
    
    /**
     * Selects from a list of indexes the one with the fewest number of pages.
     * If more than one has the fewest pages, pick based on the one that sorts
     * alphabetically earliest, based on the index names.
     *
     * @param indexList list of indexes to choose from
     *
     * @return the best index
     */
    public static FemLocalIndex getIndexWithMinDiskPages(
        List<FemLocalIndex> indexList)
    {
        if (indexList.isEmpty()) {
            return null;
        } else {
            TreeSet<FemLocalIndex> indexSet =
                new TreeSet<FemLocalIndex>(
                    new IndexPageCountComparator());
            
            indexSet.addAll(indexList);
            
            return indexSet.first();
        }
    }
    
    //~ Inner Classes ----------------------------------------------------------
    
    /*
     * A comparator class to sort index based on number of index keys.
     */
    public static class IndexLengthComparator
    implements Comparator<FemLocalIndex>
    {
        IndexLengthComparator()
        {
        }
        
        public int compare(FemLocalIndex index1, FemLocalIndex index2)
        {
            int compRes =
                (
                    index1.getIndexedFeature().size()
                    - index2.getIndexedFeature().size()
                );
            
            if (compRes == 0) {
                compRes =
                    index1.getStorageId().compareTo(index2.getStorageId());
            }
            
            return compRes;
        }
        
        public boolean equals(Object obj)
        {
            return (obj instanceof IndexLengthComparator);
        }
    }
    
    /*
     * A comparator class to sort index based on number of disk pages used.
     */
    public static class IndexPageCountComparator
    implements Comparator<FemLocalIndex>
    {
        IndexPageCountComparator()
        {
        }
        
        public int compare(FemLocalIndex index1, FemLocalIndex index2)
        {
            Long pageCount1 = index1.getPageCount();
            Long pageCount2 = index2.getPageCount();
            
            int compRes = 0;
            
            if (pageCount1 != null && pageCount2 != null) {
                compRes = pageCount1.compareTo(pageCount2);
            } else if (pageCount1 != null) {
                compRes = 1;
            } else if (pageCount2 != null) {
                compRes = -1;
            }
            
            if (compRes == 0) {
                compRes =
                    index1.getName().compareTo(index2.getName());
            }
            
            return compRes;
        }
        
        public boolean equals(Object obj)
        {
            return (obj instanceof IndexPageCountComparator);
        }
    }
    
    /*
     * SargColumnFilter represents sargable column filters, by using the
     * column number and sargable interval sequence associated with the column.
     */
    public static class SargColumnFilter
    {
        public int columnPos;
        public SargIntervalSequence sargSeq;
        
        SargColumnFilter(
            int columnPos,
            SargIntervalSequence sargSeq)
            {
            this.columnPos = columnPos;
            this.sargSeq = sargSeq;
            }
        
        Double getSelectivity(RelStatSource tabStats)
        {
            RelStatColumnStatistics colStats = null;
            Double colSel = null;
            
            if (tabStats != null) {
                colStats =
                    tabStats.getColumnStatistics(columnPos, sargSeq);
                if (colStats != null) {
                    colSel = colStats.getSelectivity();
                }
            }
            return colSel;            
        }
        
        boolean isPoint() 
        {
            return sargSeq.isPoint();
        }
    }
    
    /*
     * SargColumnFilterComparator is used to sort SargColumnFilters based on the selectivity
     * of the filter. If the selectivity is unknown, it then sorts based on column position.
     */
    public static class SargColumnFilterSelectivityComparator
    implements Comparator<SargColumnFilter>
    {
        private RelStatSource tabStats;
        
        SargColumnFilterSelectivityComparator(RelStatSource tabStats)
        {
            this.tabStats = tabStats;
        }   
        
        public int compare(SargColumnFilter filter1, SargColumnFilter filter2)
        {
            // sort based on the selectivity if stats are available; otherwise,
            // just sort on column number to ensure that results are
            // deterministic
            Double colSel1 = filter1.getSelectivity(tabStats);
            Double colSel2 = filter2.getSelectivity(tabStats);
            if (colSel1 != null && colSel2 != null) {
                return
                (colSel1 < colSel2) ? -1 :
                    ((colSel1 == colSel2) ? 0 : 1); 
            } else {
                return (filter1.columnPos - filter2.columnPos);                  
            }
        }
        
    }
    
    /*
     * CandidateIndex represents an index chosen to evaluate a predicate expressed
     * in SargIntervalSequence.
     */
    public static class CandidateIndex
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
    
}
//End LcsIndexOptimizer.java
