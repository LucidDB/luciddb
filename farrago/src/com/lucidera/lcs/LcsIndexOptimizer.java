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

import com.lucidera.query.*;

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

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.sarg.*;
import org.eigenbase.stat.*;
import org.eigenbase.util.*;


/**
 * LcsIndexOptimizer optimizes the access path to use index based on cost.
 *
 * @author Rushan Chen
 * @version $Id$
 */
public class LcsIndexOptimizer
{
    //~ Static fields/initializers ---------------------------------------------

    // Some constants to calculate index access cost.
    private static Double IOCostPerBlock = 1.0;
    private static Double SetOpCostPerBlock = 4.4;
    private static Double ResidualFilterEvalCostPerMillionRow = 82.0;
    private static Double SortCostConstant = 0.000032;
    private static Double ColumnCorrelationFactor = 0.5;
    private static int ByteLength = 8;
    private static int SmallTableRowCount = 10;
    private static Double IndexSearchSeletivityThreshold = 0.001;

    //~ Instance fields --------------------------------------------------------

    // Information on the underlying row scan
    private LcsRowScanRel rowScanRel;
    private List<FemLocalIndex> usableIndexes;
    private int tableColumnCount;
    private int dbBlockSize;

    // Source stats
    RelStatSource tableStats;
    private Double tableBlockCount;
    private Double tableRowCount;
    private Double rowScanRelRowCount;
    private Double deletionIndexScanCost;
    private boolean useCost;

    // Derived stats
    private Double avgColumnLength;
    private Double estimatedBitmapBlockCount;
    private Double estimatedBitmapRowCount;

    // Temporary data structures used during costing
    private Set<SargColumnFilter> tmpResidualFilterSet;
    private Map<FemLocalIndex, SargColumnFilter> tmpIndex2LastFilterMap;

    // best index mappings and its cost
    private Filter2IndexMapping bestMapping;
    private Double bestCost;

    Logger tracer;

    //~ Constructors -----------------------------------------------------------

    public LcsIndexOptimizer()
    {
    }

    public LcsIndexOptimizer(LcsRowScanRel rowScanRel)
    {
        this.rowScanRel = rowScanRel;
        usableIndexes = new ArrayList<FemLocalIndex>();

        for (
            FemLocalIndex index
            : FarragoCatalogUtil.getUnclusteredIndexes(
                rowScanRel.lcsTable.getPreparingStmt().getRepos(),
                rowScanRel.lcsTable.getCwmColumnSet()))
        {
            if (isValid(index)) {
                usableIndexes.add(index);
            }
        }

        dbBlockSize =
            rowScanRel.lcsTable.getPreparingStmt().getRepos().getCurrentConfig()
            .getFennelConfig().getCachePageSize();
        tableColumnCount =
            rowScanRel.lcsTable.getCwmColumnSet().getFeature().size();
        tmpResidualFilterSet = new HashSet<SargColumnFilter>();
        tmpIndex2LastFilterMap = new HashMap<FemLocalIndex, SargColumnFilter>();

        tracer = FarragoTrace.getOptimizerRuleTracer();

        useCost = true;
        tableStats = RelMetadataQuery.getStatistics(rowScanRel);

        tableBlockCount = getLcsTableBlockCount(rowScanRel.lcsTable);

        tableRowCount =
            RelMetadataQuery.getRowCount(
                rowScanRel.lcsTable.toRel(
                    rowScanRel.getCluster(),
                    rowScanRel.getConnection()));

        rowScanRelRowCount = RelMetadataQuery.getRowCount(rowScanRel);

        FemLocalIndex deletionIndex =
            FarragoCatalogUtil.getDeletionIndex(
                rowScanRel.lcsTable.getPreparingStmt().getRepos(),
                rowScanRel.lcsTable.getCwmColumnSet());

        if (deletionIndex != null) {
            deletionIndexScanCost = getIndexBitmapScanCost(deletionIndex);
        } else {
            deletionIndexScanCost = 0.0;
        }

        if ((tableStats == null)
            || (tableBlockCount == null)
            || (tableRowCount == null)
            || (rowScanRelRowCount == null)
            || (deletionIndexScanCost == null))
        {
            useCost = false;
            avgColumnLength = null;
            estimatedBitmapBlockCount = null;
            estimatedBitmapRowCount = null;
        } else {
            // Estimate "average col size" NOTE: tableBlockCount here includes
            // both clustered index leaf blocks as well as non-leaf blocks. This
            // is okay because the non-leaf blocks contribute to the cost too.
            avgColumnLength =
                tableBlockCount * dbBlockSize
                / (tableColumnCount * tableRowCount);

            // Number of blocks a bitmap for the table will occupy.
            estimatedBitmapBlockCount =
                tableRowCount / (dbBlockSize * ByteLength);

            // Number of bitmap entries in a single bitmap
            estimatedBitmapRowCount =
                tableRowCount
                / (ByteLength * LcsIndexGuide.LbmBitmapSegMaxSize);
        }
        bestMapping = new Filter2IndexMapping();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Get a list of all unclustered indexes on the table scanned by a
     * LcsRowScanRel
     *
     * @param rowScan
     *
     * @return list of all unclustered indexes
     */
    public static List<FemLocalIndex> getUnclusteredIndexes(
        LcsRowScanRel rowScan)
    {
        return FarragoCatalogUtil.getUnclusteredIndexes(
            rowScan.lcsTable.getPreparingStmt().getRepos(),
            rowScan.lcsTable.getCwmColumnSet());
    }

    /**
     * Checks if an index is valid
     *
     * @param index
     *
     * @return if an index is valid
     */
    private static boolean isValid(FemLocalIndex index)
    {
        return index.getVisibility() == VisibilityKindEnum.VK_PUBLIC;
    }

    /**
     * Get the column at a given index key position.
     *
     * @param index the index whose key contains the column
     * @param position the index key position for the column
     *
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
     *
     * @return index key position, or -1 if the column is not part of the index
     * key
     */
    private static int getIndexColumnPos(
        FemLocalIndex index,
        FemAbstractColumn col)
    {
        List<CwmIndexedFeature> indexedFeatures = index.getIndexedFeature();

        int i;
        for (i = 0; i < indexedFeatures.size(); i++) {
            if (indexedFeatures.get(i).getFeature() == col) {
                return i;
            }
        }

        return -1;
    }

    /**
     * From a list of filters on distinct columns, find the one on a given
     * column.
     *
     * @param rowScanRel
     * @param filterSet
     * @param col
     *
     * @return the filter on the given column
     */
    private static SargColumnFilter findSargFilterForColumn(
        LcsRowScanRel rowScanRel,
        Set<SargColumnFilter> filterSet,
        FemAbstractColumn col)
    {
        for (SargColumnFilter filter : filterSet) {
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
     * @param colFilterLists two SargColumnFilter lists: the first one contains
     * the "point" column filters, the second one contains the "interval" column
     * filters.
     *
     * @return a map from selected index to its associated matched key position.
     */
    private Map<FemLocalIndex, Integer> getIndex2MatchedPos(
        List<List<SargColumnFilter>> colFilterLists)
    {
        List<CwmColumn> pointColumnList = new ArrayList<CwmColumn>();
        List<CwmColumn> intervalColumnList = new ArrayList<CwmColumn>();

        List<SargColumnFilter> pointList = colFilterLists.get(0);
        List<SargColumnFilter> intervalList = colFilterLists.get(1);

        for (SargColumnFilter filter : pointList) {
            pointColumnList.add(
                rowScanRel.getColumnForFieldAccess(filter.columnPos));
        }

        for (SargColumnFilter filter : intervalList) {
            intervalColumnList.add(
                rowScanRel.getColumnForFieldAccess(filter.columnPos));
        }

        Map<FemLocalIndex, Integer> index2PosMap =
            new HashMap<FemLocalIndex, Integer>();
        boolean matchedAll = false;

        // Sort the index based on length(and a unique identifier to break
        // ties), so that index with fewer key columns are searched first(and
        // preferred).
        TreeSet<FemLocalIndex> indexSet =
            new TreeSet<FemLocalIndex>(new IndexLengthComparator());

        indexSet.addAll(usableIndexes);

        // First process the columns with point predicates.
        // Objective is to maximize the index key columns matched.
        while ((pointColumnList.size() > 0) && !matchedAll) {
            // TODO: A better rule could be to match the shortest index with
            // the maximum matched positions
            int maxMatchedPos = 0;
            FemLocalIndex maxMatchedIndex = null;
            int matchedPos = 0;

            for (FemLocalIndex index : indexSet) {
                matchedPos = 0;

                CwmColumn col = getIndexColumn(index, matchedPos);

                while ((col != null) && pointColumnList.contains(col)) {
                    matchedPos++;
                    col = getIndexColumn(index, matchedPos);
                }

                // try to match one more column from the interval column
                // list
                if (intervalColumnList.contains(
                        getIndexColumn(index, matchedPos)))
                {
                    matchedPos++;
                }

                // Pick the index with the max matchedPos.
                if (maxMatchedPos < matchedPos) {
                    maxMatchedPos = matchedPos;
                    maxMatchedIndex = index;
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
                        boolean removed = intervalColumnList.remove(matchedCol);
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
        while ((intervalColumnList.size() > 0) && iter.hasNext()) {
            FemLocalIndex index = iter.next();
            CwmColumn firstCol = getIndexColumn(index, maxMatchedPos - 1);
            if ((firstCol != null) && intervalColumnList.contains(firstCol)) {
                index2PosMap.put(
                    index,
                    maxMatchedPos);
                intervalColumnList.remove(firstCol);
                iter.remove();
            }
        }

        return index2PosMap;
    }

    /**
     * Find the best filter to index mapping based on cost. If cost information
     * is not available, pick indexes with the longest matching keys.
     *
     * @param filterLists two lists of sarg column filters: one for point
     * filters, and one for interval filters.
     *
     * @return a map which contains the indexes picked for index access path and
     * the respective leading key postitions for each index.
     */
    public final Map<FemLocalIndex, Integer> getIndex2MatchedPosByCost(
        List<List<SargColumnFilter>> filterLists)
    {
        assert (filterLists.size() == 2);
        Map<FemLocalIndex, Integer> resultMapping = null;

        List<SargColumnFilter> pointList = filterLists.get(0);
        List<SargColumnFilter> intervalList = filterLists.get(1);

        // If scanning a small table, skip index access analysis.
        // Use residual filters only.
        if (rowScanRelRowCount < SmallTableRowCount) {
            // return empty mapping
            return bestMapping.index2MatchedPosMap;
        }

        if (useCost) {
            // Enumerate through possible index combinations and cost each one
            // of them.
            getBestIndex(pointList, intervalList);
        }

        if (bestCost != null) {
            if (tracer.isLoggable(Level.FINEST)) {
                String nl = System.getProperty("line.separator");

                if (bestMapping.filter2IndexMap.isEmpty()) {
                    tracer.finest(
                        "No index is found for the filters. " + nl
                        + "Residual filtering has the cost of "
                        + bestCost + nl);
                } else {
                    String msg =
                        "The following filter->index mapping for table scan ("
                        + rowScanRel.lcsTable.getName()
                        + ") has the best cost of " + bestCost + nl;
                    for (
                        SargColumnFilter filter
                        : bestMapping.filter2IndexMap.keySet())
                    {
                        FemLocalIndex index =
                            bestMapping.filter2IndexMap.get(filter);
                        msg +=
                            "  [filter on(" + rowScanRel.lcsTable.getName()
                            + "."
                            + rowScanRel.getColumnForFieldAccess(
                                filter.columnPos).getName() + ") "
                            + "index(" + index.getName() + ") "
                            + "indexpos("
                            + bestMapping.index2MatchedPosMap.get(index) + ")]"
                            + nl;
                    }
                    tracer.finest(msg);
                }
            }
            resultMapping = bestMapping.index2MatchedPosMap;
        } else {
            // If cost based index selection did not find any index.
            // Try rule based.
            resultMapping = getIndex2MatchedPos(filterLists);

            if (tracer.isLoggable(Level.FINEST)) {
                String nl = System.getProperty("line.separator");

                String msg =
                    "Couldn't use cost based algorithm to find index path"
                    + " for table scan(" + rowScanRel.lcsTable.getName()
                    + ")" + nl;

                if (!resultMapping.keySet().isEmpty()) {
                    msg +=
                        "Find these indexes(and leading key pos) using the"
                        + " rule of longest matching keys" + nl;
                } else {
                    msg += "No index is found using rule either" + nl;
                }

                for (FemLocalIndex index : resultMapping.keySet()) {
                    msg +=
                        "  [index(" + index.getName() + ") indexpos("
                        + resultMapping.get(index) + ")]" + nl;
                }

                tracer.finest(msg);
            }
        }

        return resultMapping;
    }

    /**
     * Find the set of indexes that gives the lowest access time for the query.
     * This method greedily searchs for local optimum; it may end up picking a
     * sub-optimal set of indexes.
     *
     * @param pointList list of point filters
     * @param intervalList list of interval filters
     */
    private void getBestIndex(
        List<SargColumnFilter> pointList,
        List<SargColumnFilter> intervalList)
    {
        TreeSet<IndexFilterTuple> indexFilterSet =
            new TreeSet<IndexFilterTuple>(
                new MappedFilterSelectivityComparator());
        Set<SargColumnFilter> mappedIndexSet = new HashSet<SargColumnFilter>();

        // map each index to a list of matched filters
        // add tuples <index, matched filters> to indexFilterSet
        boolean disjoint =
            mapIndexToFilter(
                usableIndexes,
                pointList,
                intervalList,
                mappedIndexSet,
                indexFilterSet);

        Double selectivity = 1.0;
        Filter2IndexMapping currentMapping = new Filter2IndexMapping();
        Filter2IndexMapping currBestMapping = new Filter2IndexMapping();
        HashSet<IndexFilterTuple> toDelete = new HashSet<IndexFilterTuple>();

        bestCost = costIndexAccess(pointList, intervalList, currentMapping);

        if (bestCost == null) {
            return;
        }

        while (
            !indexFilterSet.isEmpty()
            && (selectivity > IndexSearchSeletivityThreshold))
        {
            currBestMapping.copyFrom(bestMapping);
            Double currentCost = bestCost;
            toDelete.clear();
            IndexFilterTuple bestTupThisRound = null;

            for (IndexFilterTuple tup : indexFilterSet) {
                //add current mapping to currBestMapping and try to cost
                currentMapping.copyFrom(currBestMapping);
                currentMapping.add(tup);
                if (tracer.isLoggable(Level.FINEST)) {
                    String msg =
                        "Index: "
                        + tup.getIndex().getName()
                        + " selectivity: "
                        + tup.getEffectiveSelectivity();
                    tracer.finest(msg);
                }
                Double newCost =
                    costIndexAccess(
                        pointList,
                        intervalList,
                        currentMapping);

                if ((newCost != null) && (newCost < bestCost)) {
                    bestCost = newCost;
                    bestTupThisRound = tup;
                    bestMapping.copyFrom(currentMapping);
                } else if ((newCost == null) || (newCost >= currentCost)) {
                    toDelete.add(tup);
                }
            }
            if (bestTupThisRound != null) {
                if (tracer.isLoggable(Level.FINEST)) {
                    String msg =
                        "Found a new index in this round that reduces cost: "
                        + bestTupThisRound.getIndex().getName();
                    tracer.finest(msg);
                }
                selectivity *= bestTupThisRound.getEffectiveSelectivity();
                toDelete.add(bestTupThisRound);
            } else {
                break;
            }

            // purge the tree to prepare for next round
            indexFilterSet.removeAll(toDelete);

            // rebuild the tree with changed selectivity and trim if desired
            if (!(disjoint || indexFilterSet.isEmpty())) {
                rebuildTreeSet(
                    indexFilterSet,
                    bestMapping.filter2IndexMap.keySet());
            }
        }
    }

    /**
     * maps each index in the list of usable indexes to filters satisfiable by
     * the index and populates the indexFilterSet with tuples (index,
     * satisfiable filters). The set is ordered by effective selectivity of the
     * index.
     *
     * @param usableIndexes list of indexes to be mapped
     * @param pointList list of point filters
     * @param intervalList list of interval filters
     * @param mappedIndexSet
     * @param indexFilterSet the tree set to be populated
     *
     * @return true if the sets of satisfiable filters are disjoint
     */
    private boolean mapIndexToFilter(
        List<FemLocalIndex> usableIndexes,
        List<SargColumnFilter> pointList,
        List<SargColumnFilter> intervalList,
        Set<SargColumnFilter> mappedIndexSet,
        TreeSet<IndexFilterTuple> indexFilterSet)
    {
        boolean disjoint = true;
        Set<SargColumnFilter> mappedSoFar = new HashSet<SargColumnFilter>();

        for (FemLocalIndex index : usableIndexes) {
            IndexFilterTuple tup =
                new IndexFilterTuple(
                    index,
                    pointList,
                    intervalList,
                    mappedIndexSet);

            if (!tup.getFilterList().isEmpty()) {
                boolean added = indexFilterSet.add(tup);
                if ((!added) && (tracer.isLoggable(Level.FINEST))) {
                    String msg = "Failed to add " + tup.getIndex().getName();
                    tracer.finest(msg);
                }

                // check if any of newly added filters have already been mapped
                List<SargColumnFilter> newFilters = tup.getFilterList();
                for (SargColumnFilter filter : newFilters) {
                    if (mappedSoFar.contains(filter)) {
                        disjoint = false;
                        break;
                    }
                }
                mappedSoFar.addAll(newFilters);
            }
        }
        return disjoint;
    }

    /**
     * recalculate effective selectivity and rebuild the tree set effective
     * selectivity of an index may be changed if some other indexes has
     * satisfied some of the filters
     *
     * @param indexFilterSet
     * @param mappedFilters
     */
    private void rebuildTreeSet(
        TreeSet<IndexFilterTuple> indexFilterSet,
        Set<SargColumnFilter> mappedFilters)
    {
        for (IndexFilterTuple indexFilter : indexFilterSet) {
            indexFilter.reCalculateEffectiveSelectivity(mappedFilters);
        }
    }

    /**
     * Calculate the cost of using index access and residual filtering with a
     * row scan.
     *
     * @param pointList list of point filters
     * @param intervalList list of interval filters
     * @param candidateMapping the candidate mapping from filter to index
     *
     * @return cost of index access
     */
    private Double costIndexAccess(
        List<SargColumnFilter> pointList,
        List<SargColumnFilter> intervalList,
        Filter2IndexMapping candidateMapping)
    {
        Double cost = 0.0;

        // residual list starts out having all the filters in it
        tmpResidualFilterSet.clear();
        tmpResidualFilterSet.addAll(pointList);
        tmpResidualFilterSet.addAll(intervalList);

        Map<SargColumnFilter, FemLocalIndex> filter2IndexMap =
            candidateMapping.filter2IndexMap;
        Map<FemLocalIndex, Integer> index2MatchedPosMap =
            candidateMapping.index2MatchedPosMap;

        Double indexSearchCost = null;

        // cost the index access
        if (!filter2IndexMap.isEmpty()) {
            tmpIndex2LastFilterMap.clear();

            for (FemLocalIndex index : index2MatchedPosMap.keySet()) {
                FemAbstractColumn lastMatchedKeyCol =
                    getIndexColumn(index, index2MatchedPosMap.get(index) - 1);
                SargColumnFilter filter =
                    findSargFilterForColumn(
                        rowScanRel,
                        tmpResidualFilterSet,
                        lastMatchedKeyCol);
                tmpIndex2LastFilterMap.put(index, filter);
            }

            indexSearchCost =
                getIndexSearchCost(
                    index2MatchedPosMap,
                    tmpIndex2LastFilterMap);

            if (indexSearchCost == null) {
                if (tracer.isLoggable(Level.FINEST)) {
                    tracer.finest(
                        "Check if table is analyzed:"
                        + rowScanRel.lcsTable.getName());
                }
                return null;
            }

            cost += indexSearchCost;

            // prepare the residual lists
            tmpResidualFilterSet.removeAll(filter2IndexMap.keySet());
        }

        // cost the residual filter access
        Double residualScanCost =
            getTableScanCostWithResidual(
                filter2IndexMap.keySet(),
                tmpResidualFilterSet);

        if (residualScanCost == null) {
            if (tracer.isLoggable(Level.FINEST)) {
                tracer.finest(
                    "Check if table is analyzed:"
                    + rowScanRel.lcsTable.getName());
            }
            return null;
        }

        cost += residualScanCost;

        if (tracer.isLoggable(Level.FINEST)) {
            String nl = System.getProperty("line.separator");

            String msg =
                "Access path for table scan("
                + rowScanRel.lcsTable.getName() + ") ";

            msg +=
                "has a cost of " + cost + nl
                + "(index access cost=" + indexSearchCost
                + " residual scan cost=" + residualScanCost + ")" + nl;

            for (
                SargColumnFilter filter
                : candidateMapping.filter2IndexMap.keySet())
            {
                FemLocalIndex index =
                    candidateMapping.filter2IndexMap.get(filter);
                msg +=
                    "  [filter on(" + rowScanRel.lcsTable.getName()
                    + "."
                    + rowScanRel.getColumnForFieldAccess(filter.columnPos)
                    .getName() + ") "
                    + "index(" + index.getName() + ") "
                    + "indexpos("
                    + candidateMapping.index2MatchedPosMap.get(index) + ")]"
                    + nl;
            }

            tracer.finest(msg);
        }

        if ((bestCost == null) || (bestCost > cost)) {
            if (tracer.isLoggable(Level.FINEST)) {
                tracer.finest("New best cost is " + cost);
            }
        }
        return cost;
    }

    /**
     * Calculate the cost of using index access, driven by an input rel, on a
     * row scan.
     *
     * @param dimRel input rel that drives the index search
     * @param factKeyList keys on which the row scan is filtered
     * @param dimKeyList keys which the input rel produces to drive the index
     * search
     * @param index index to be used in the index search
     * @param matchedPos leading key positions of this index
     *
     * @return the cost of scanning factRowScanRel with the proposed index.
     */
    private Double costIndexAccessWithInputRel(
        RelNode dimRel,
        List<Integer> factKeyList,
        List<Integer> dimKeyList,
        FemLocalIndex index,
        int matchedPos)
    {
        Double cost = 0.0;

        // cost the index access
        Map<FemLocalIndex, Integer> index2MatchedPosMap =
            new HashMap<FemLocalIndex, Integer>();

        index2MatchedPosMap.put(index, matchedPos);

        Double indexSearchCost =
            getIndexSearchCost(
                index2MatchedPosMap,
                null);

        if (indexSearchCost == null) {
            return null;
        }

        BitSet dimKeys = new BitSet();
        for (int dimCol : dimKeyList) {
            dimKeys.set(dimCol);
        }

        Double dimKeyCND =
            RelMetadataQuery.getDistinctRowCount(
                dimRel,
                dimKeys,
                null);

        if (dimKeyCND == null) {
            return null;
        }

        // this search is repeated for every distinct keys from the inputRel
        cost += indexSearchCost * dimKeyCND;

        double indexSearchSelectivity =
            RelMdUtil.computeSemiJoinSelectivity(
                rowScanRel,
                dimRel,
                factKeyList,
                dimKeyList);
        ;

        // cost the residual filter access
        Double tableScanCost =
            getTableScanCostWithIndexSearch(indexSearchSelectivity);

        if (tableScanCost == null) {
            return null;
        }
        cost += tableScanCost;
        return cost;
    }

    /**
     * Converts a list of SargBidning to a map of column to sarg sequence.
     *
     * @param sargBindingList list of sarg binding.
     *
     * @return converted map
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

    /**
     * Find the index with the best cost to filter the LHS of a join that
     * originates from a single LcsRowScanRel. Typical usage is to filter the
     * fact table joining to a dimension table.
     *
     * @param dimRel RHS of a join, e.g. scanning the dimension table.
     * @param factKeyList LHS join key positions
     * @param dimKeyList RHS join key positions
     * @param bestFactKeyOrder join keys that can be filtered by the index
     *
     * @return the best index to filter the join LHS
     */
    public final FemLocalIndex findSemiJoinIndexByCost(
        RelNode dimRel,
        List<Integer> factKeyList,
        List<Integer> dimKeyList,
        List<Integer> bestFactKeyOrder)
    {
        // loop through the indexes and either find the one that has the
        // longest matching keys, or the first one that matches all the
        // semijoin keys
        Integer [] bestKeyOrder = {};
        FemLocalIndex bestIndex = null;
        Double bestCost = null;
        int bestNKeys = 0;

        if (useCost) {
            for (FemLocalIndex index : usableIndexes) {
                Integer [] keyOrder = new Integer[factKeyList.size()];
                int nKeys =
                    rowScanRel.lcsTable.getIndexGuide().matchIndexKeys(
                        index,
                        factKeyList,
                        keyOrder);

                // Only calculate index access cost if factKeyList matches as
                // least one index key
                if (nKeys > 0) {
                    List<Integer> factKeysMatched = new ArrayList<Integer>();
                    List<Integer> dimKeysMatched = new ArrayList<Integer>();

                    // use only the matched join keys to decide selectivity
                    for (int i = 0; i < nKeys; i++) {
                        factKeysMatched.add(keyOrder[i]);
                        dimKeysMatched.add(keyOrder[i]);
                    }

                    Double curCost =
                        costIndexAccessWithInputRel(
                            dimRel,
                            factKeysMatched,
                            dimKeysMatched,
                            index,
                            nKeys);

                    if (tracer.isLoggable(Level.FINEST)) {
                        String msg =
                            "Scanning the fact table "
                            + rowScanRel.lcsTable.getName()
                            + ", using the index (" + index.getName() + " "
                            + nKeys
                            + ") ";

                        if (curCost == null) {
                            msg +=
                                "does not have a cost(hint: check if the table is"
                                + " analyzed)";
                        } else {
                            msg += "has a cost of " + curCost;
                        }

                        tracer.finest(msg);
                    }

                    if (curCost != null) {
                        if ((bestCost == null) || (bestCost > curCost)) {
                            bestCost = curCost;
                            bestNKeys = nKeys;
                            bestKeyOrder = keyOrder;
                            bestIndex = index;
                            if (tracer.isLoggable(Level.FINEST)) {
                                tracer.finest("New best cost is " + bestCost);
                            }
                        }
                    }
                }
            }

            for (int i = 0; i < bestNKeys; i++) {
                bestFactKeyOrder.add(bestKeyOrder[i]);
            }
        }

        if (bestIndex == null) {
            // Try using rule to pick the indexes.
            LcsTable factTable = rowScanRel.lcsTable;
            LcsIndexGuide indexGuide = factTable.getIndexGuide();
            bestIndex =
                indexGuide.findSemiJoinIndex(factKeyList, bestFactKeyOrder);
            if (tracer.isLoggable(Level.FINEST)) {
                String msg =
                    "Scanning the fact table "
                    + rowScanRel.lcsTable.getName();
                if (bestIndex == null) {
                    msg += " cannot be optimized using any index";
                } else {
                    msg +=
                        " can be optimized using index " + bestIndex.getName();
                }
                tracer.finest(msg);
            }
        }

        return bestIndex;
    }

    /**
     * Calculate the cost of an index search.
     *
     * @param index2MatchedPosMap map from index to searched position
     * @param index2LastFilterMap map from index to last filter satisfied by
     * this index.
     *
     * @return the cost of performing index search.
     */
    private Double getIndexSearchCost(
        Map<FemLocalIndex, Integer> index2MatchedPosMap,
        Map<FemLocalIndex, SargColumnFilter> index2LastFilterMap)
    {
        assert (useCost);

        Double cost = 0.0;

        if (index2LastFilterMap != null) {
            assert (index2MatchedPosMap.size() == index2LastFilterMap.size());
        }

        for (FemLocalIndex index : index2MatchedPosMap.keySet()) {
            int mappedPos = index2MatchedPosMap.get(index);
            SargColumnFilter lastFilter = null;

            if (index2LastFilterMap != null) {
                lastFilter = index2LastFilterMap.get(index);
            }

            Double scannedBitmapCount =
                getIndexBitmapCount(index, mappedPos, lastFilter);

            if (scannedBitmapCount == null) {
                return null;
            }

            Double scanCost = getIndexBitmapScanCost(index, scannedBitmapCount);

            Double mergeCost =
                getIndexBitmapBitOpCost(scannedBitmapCount.intValue());

            Double sortCost = getIndexBitmapSortCost(scannedBitmapCount);

            if ((scanCost == null)
                || (mergeCost == null)
                || (sortCost == null))
            {
                return null;
            }

            cost += scanCost + mergeCost + sortCost;
        }

        // add the cost for scanning the deletion index
        cost += deletionIndexScanCost;

        // add the final cost of intersecting the final bitmaps
        Double intersectCost =
            getIndexBitmapBitOpCost(index2MatchedPosMap.size());

        if (intersectCost == null) {
            return null;
        }

        cost += intersectCost;

        return cost;
    }

    /**
     * Calculate the cost of scanning an entire bitmap index.
     *
     * @param index index to be scanned.
     *
     * @return the cost of performing index search.
     */
    private static Double getIndexBitmapScanCost(
        FemLocalIndex index)
    {
        Long blockCount = index.getPageCount();

        if (blockCount == null) {
            return null;
        }

        Double cost = IOCostPerBlock * blockCount;

        return cost;
    }

    /**
     * Calculate the cost of scanning part of a bitmap index.
     *
     * @param index index to be scanned.
     * @param scannedBitmapCount number of bitmaps scanned
     *
     * @return the cost of performing index search.
     */
    private Double getIndexBitmapScanCost(
        FemLocalIndex index,
        Double scannedBitmapCount)
    {
        assert (useCost);
        Long blockCount = index.getPageCount();

        if (blockCount == null) {
            return null;
        }

        Double cost = IOCostPerBlock * blockCount;

        Double totalBitmapCount = getIndexBitmapCount(index, 0, null);

        if ((scannedBitmapCount == null) || (totalBitmapCount == null)) {
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

    /**
     * Calculates the cost of performing bitmap operations for a bitmapped
     * index.
     *
     * @param numIndexesUsed number of indexes used in the index access path.
     *
     * @return cost of AND'ing the bitmaps from difference indexes.
     */
    private Double getIndexBitmapBitOpCost(
        int numIndexesUsed)
    {
        assert (useCost);

        Double cost =
            SetOpCostPerBlock * numIndexesUsed * estimatedBitmapBlockCount;

        return cost;
    }

    /**
     * Calculate the cost of sorting the bitmap entries in an index.
     *
     * @param scannedBitmapCount number of bitmaps scaned
     *
     * @return cost of sorting the bitmap entries.
     */
    private Double getIndexBitmapSortCost(
        Double scannedBitmapCount)
    {
        assert (useCost);

        Double cost = 0.0;

        if (tableRowCount <= 1.0) {
            // no sort needed
            cost = 0.0;
        } else {
            //Assume index entries are all max length
            Double estimatedIndexRowCount =
                scannedBitmapCount * estimatedBitmapRowCount;

            cost =
                SortCostConstant
                * estimatedIndexRowCount * Math.log(estimatedIndexRowCount);
        }

        return cost;
    }

    /**
     * Calculate the number of bitmaps(distinct key values) satisfying the index
     * search keys.
     *
     * @param index index to be scanned.
     * @param mappedPos key postitions mapped to this index.
     * @param lastFilter the last filter satisfied by this index
     *
     * @return number of bitmaps matching the index search keys.
     */
    private Double getIndexBitmapCount(
        FemLocalIndex index,
        int mappedPos,
        SargColumnFilter lastFilter)
    {
        assert (useCost);

        Double bitmapCount = null;

        List<CwmIndexedFeature> indexedFeatures = index.getIndexedFeature();

        int i;
        int indexKeyLength = indexedFeatures.size();

        // starting from the last mapped postition (the index is mappedPos - 1)
        // multiple the current bitmap count by the CND of the index column
        // with an adjustment of correlationFactor.
        if (mappedPos > 0) {
            // this is the last filter
            if ((lastFilter == null) || lastFilter.isPoint()) {
                // is point filter
                bitmapCount = 1.0;
            } else {
                // is interval filter
                FemAbstractColumn lastFilterColumn =
                    rowScanRel.getColumnForFieldAccess(lastFilter.columnPos);
                assert (indexedFeatures.get(mappedPos - 1).getFeature()
                    == lastFilterColumn);

                RelStatColumnStatistics colStats = null;

                colStats =
                    tableStats.getColumnStatistics(
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
            for (i = mappedPos; i < indexKeyLength; i++) {
                RelStatColumnStatistics colStats = null;

                int ordinal =
                    rowScanRel.lcsTable.getCwmColumnSet().getFeature().indexOf(
                        indexedFeatures.get(i).getFeature());

                colStats = tableStats.getColumnStatistics(ordinal, null);

                if (colStats == null) {
                    return null;
                } else {
                    Double indexColumnCND = colStats.getCardinality();
                    if (indexColumnCND == null) {
                        return null;
                    }
                    if (indexColumnCND < (1 / ColumnCorrelationFactor)) {
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

    /**
     * Calculate the total number of disk blocks used by a lcs table.
     *
     * @param lcsTable
     *
     * @return disk blocks used o null if the clustered indexes are not
     * analyzed.
     */
    private static Double getLcsTableBlockCount(LcsTable lcsTable)
    {
        Double lcsTableBlockCount = 0.0;
        Long pageCount;
        for (FemLocalIndex index : lcsTable.getClusteredIndexes()) {
            pageCount = index.getPageCount();
            if (pageCount == null) {
                return null;
            }
            lcsTableBlockCount += pageCount;
        }

        return lcsTableBlockCount;
    }

    /**
     * Calculate the combined selectivity of a set of sargable filters.
     *
     * @param filterSet set of filters
     * @param tabStat stat for underlying table these filters are based on
     *
     * @return combined selectivity or null if selectivity stat is not available
     */
    private static Double getCombinedSelectivity(
        Set<SargColumnFilter> filterSet,
        RelStatSource tabStat)
    {
        Double combinedSelectivity = 1.0;

        for (SargColumnFilter filter : filterSet) {
            Double filterSelectivity = filter.getSelectivity(tabStat);

            if (filterSelectivity == null) {
                return null;
            }

            if (filterSelectivity > ColumnCorrelationFactor) {
                filterSelectivity = 1.0;
            } else {
                filterSelectivity /= ColumnCorrelationFactor;
                combinedSelectivity *= filterSelectivity;
            }
        }

        return combinedSelectivity;
    }

    /**
     * Calculate the combined selectivity of a set of sargable filters.
     *
     * @param filterSet set of filters
     *
     * @return combined selectivity or null if selectivity stat is not available
     */
    private Double getCombinedSelectivity(Set<SargColumnFilter> filterSet)
    {
        return getCombinedSelectivity(filterSet, tableStats);
    }

    /**
     * Calculate the cost of scanning a table with residual filters applied.
     *
     * @param indexSearchFilterSet index search filters
     * @param residualFilterSet residual column filters
     *
     * @return the cost of row scan with residuals
     */
    private Double getTableScanCostWithResidual(
        Set<SargColumnFilter> indexSearchFilterSet,
        Set<SargColumnFilter> residualFilterSet)
    {
        assert (useCost);

        Double cost = 0.0;

        int residualColCount = residualFilterSet.size();
        int nonResidualColCount = tableColumnCount - residualColCount;

        Double indexSearchSelectivity = 1.0;
        Double residualFilterSelectivity = 1.0;

        indexSearchSelectivity = getCombinedSelectivity(indexSearchFilterSet);

        if (indexSearchSelectivity == null) {
            return null;
        }

        Double rowCountWithIndexSearch =
            rowScanRelRowCount * indexSearchSelectivity;

        if (rowCountWithIndexSearch < 1.0) {
            // Prior index search has filtered the table down to less than
            // one row. return 0.0
            return cost;
        }

        // Index search returns some rows.
        // They will be filtered by the residual filters.
        residualFilterSelectivity = getCombinedSelectivity(residualFilterSet);

        if (residualFilterSelectivity == null) {
            return null;
        }

        Double scanCost =
            IOCostPerBlock
            * rowCountWithIndexSearch * avgColumnLength
            * ((((residualColCount + 1) * (1 + residualFilterSelectivity)
                        / 2)
                    + ((nonResidualColCount - 1) * residualFilterSelectivity))
                / dbBlockSize);

        Double filterEvalCost =
            (ResidualFilterEvalCostPerMillionRow / 1000000.0)
            * rowCountWithIndexSearch
            * residualColCount * (1 + residualFilterSelectivity) / 2;

        cost = scanCost + filterEvalCost;

        return cost;
    }

    /**
     * Calculate the cost of scanning a table with index search applied.
     *
     * @param indexSearchSelectivity
     *
     * @return the cost of row scan with index search.
     */
    private Double getTableScanCostWithIndexSearch(
        Double indexSearchSelectivity)
    {
        assert (useCost);

        // Note that the blocks scanned could actually be smaller than
        // tableBlockCount due to existing filtering. However, the combined
        // selectivity might not be derived easily. Since the cost calculated
        // here will only be used in comparison, it is acceptable to assume
        // a common case which is no existing filtering.
        Double cost = IOCostPerBlock * tableBlockCount * indexSearchSelectivity;

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
                new TreeSet<FemLocalIndex>(new IndexPageCountComparator());

            indexSet.addAll(indexList);

            return indexSet.first();
        }
    }

    /**
     * Create a new index search rel using an given index. Add merge rel if
     * required. Merge rels are always added if search rel is driven by an input
     * other than FennelValuesRel.
     *
     * @param cluster Cluster
     * @param relImplementor Implementor
     * @param lcsTable table filtered by the index access path
     * @param index the index to search on
     * @param keyInput input rel containing index key values to search on.
     * @param inputKeyProj key locations from the input.
     * @param inputDirectiveProj key directives for each key value
     * @param startRidParamId
     * @param rowLimitParamId
     * @param requireMerge
     *
     * @return the new index access rel created
     */
    public static FennelSingleRel newIndexRel(
        FennelRelImplementor relImplementor,
        RelOptCluster cluster,
        LcsTable lcsTable,
        FemLocalIndex index,
        RelNode keyInput,
        Integer [] inputKeyProj,
        Integer [] inputDirectiveProj,
        FennelRelParamId startRidParamId,
        FennelRelParamId rowLimitParamId,
        boolean requireMerge)
    {
        FennelRelParamId startRidParamIdForSearch =
            requireMerge ? null : startRidParamId;
        FennelRelParamId rowLimitParamIdForSearch =
            requireMerge ? null : rowLimitParamId;

        LcsIndexSearchRel indexSearch =
            new LcsIndexSearchRel(
                cluster,
                keyInput,
                lcsTable,
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

        FennelSingleRel indexRel = indexSearch;

        if (requireMerge) {
            FennelRelParamId chopperRidLimitParamId =
                relImplementor.allocateRelParamId();

            indexRel =
                new LcsIndexMergeRel(
                    lcsTable,
                    indexSearch,
                    startRidParamId,
                    rowLimitParamId,
                    chopperRidLimitParamId);
        }

        return indexRel;
    }

    /**
     * Add new index access rel and intersect that with the oldIndexAccessRel.
     * This method is only called from RelOptRule class, as it requires the the
     * rule call as a parameter. Additionally, this rule has to match
     * LcsRowScanRel as part of the pattern.
     *
     * @param oldIndexAccessRel existing index access rel
     * @param call call matched by a rule
     * @param rowScanRelPosInCall position of LcsRowScanRel in the sequence of
     * rels matched by the rule
     * @param index index to add to the access path
     * @param keyInput input rel to the index search
     * @param inputKeyProj key projection from index search input
     * @param inputDirectiveProj directive projection from index search input
     * @param startRidParamId parameter ID for RID skipping optimization
     * @param rowLimitParamId parameter ID for
     * @param requireMerge whether a LcsIndexMergeRel should be added on top of
     * the newly created LcsIndexSearchRel. If the input to LcsIndexSearchRel is
     * known to search to just one bitmap, then no merge is required. All other
     * cases, for example, when the input comes from a sort, a merge is
     * required.
     *
     * @return the new index intersect rel created
     */
    private static LcsIndexIntersectRel addIntersect(
        RelNode oldIndexAccessRel,
        RelOptRuleCall call,
        int rowScanRelPosInCall,
        FemLocalIndex index,
        RelNode keyInput,
        Integer [] inputKeyProj,
        Integer [] inputDirectiveProj,
        FennelRelParamId startRidParamId,
        FennelRelParamId rowLimitParamId,
        boolean requireMerge)
    {
        RelNode rowScanRel = call.rels[rowScanRelPosInCall];
        assert (rowScanRel instanceof LcsRowScanRel);

        FennelRelImplementor relImplementor =
            FennelRelUtil.getRelImplementor((LcsRowScanRel) rowScanRel);
        RelOptCluster cluster = rowScanRel.getCluster();
        LcsTable lcsTable = ((LcsRowScanRel) rowScanRel).lcsTable;

        RelNode [] inputRelsToIntersect;
        int numInputRelsToIntersect;

        // if there's already an intersect, get the existing children
        // of that intersect
        if (oldIndexAccessRel instanceof LcsIndexIntersectRel) {
            // Do not have to recreate the existing index access rels
            // because they will have the correct paramids
            numInputRelsToIntersect = oldIndexAccessRel.getInputs().length + 1;
            inputRelsToIntersect = new RelNode[numInputRelsToIntersect];
            for (int i = 0; i < (numInputRelsToIntersect - 1); i++) {
                inputRelsToIntersect[i] = oldIndexAccessRel.getInputs()[i];
            }
        } else {
            numInputRelsToIntersect = 2;
            inputRelsToIntersect = new RelNode[numInputRelsToIntersect];

            // There are a few cases depending on if the oldIndexAccessRel
            // was newly created.
            if (oldIndexAccessRel instanceof LcsIndexMergeRel) {
                LcsIndexMergeRel oldIndexMergeRel =
                    (LcsIndexMergeRel) oldIndexAccessRel;
                LcsIndexSearchRel oldIndexSearchRel;

                if (call.rels.length == (rowScanRelPosInCall + 3)) {
                    /*
                     * call tree shape:
                     * ....
                     *   RowScanRel
                     *     origIndexMerge
                     *       origIndexSearch
                     */

                    // recreate the index merge rel with the appropriate dynamic
                    // params
                    assert (call.rels[rowScanRelPosInCall + 2]
                        instanceof LcsIndexSearchRel);
                    LcsIndexMergeRel origIndexMergeRel =
                        (LcsIndexMergeRel) call.rels[rowScanRelPosInCall + 1];

                    // merge rel should have come from the call tree because the
                    // call tree length is (rowScanRelPosInCall + 3)
                    assert (oldIndexMergeRel == origIndexMergeRel);
                    oldIndexSearchRel =
                        (LcsIndexSearchRel) call.rels[rowScanRelPosInCall + 2];
                } else {
                    // row scan has no input or inputs are all residual filters
                    // and the merge rel is recently created, i.e, does not come
                    // from the call tree call tree shape:
                    /*
                     * ....
                     *   RowScanRel
                     */

                    assert (call.rels.length == (rowScanRelPosInCall + 1));
                    assert (oldIndexAccessRel.getInputs()[0]
                        instanceof LcsIndexSearchRel);
                    oldIndexSearchRel =
                        (LcsIndexSearchRel) oldIndexAccessRel.getInputs()[0];
                }

                // NOTE: The new merge can use the parameters to advance rids.
                // However, its input insersect cannot use RID in the search
                // key, because this insersect does not search to a "point" key
                // (hence the merge is required). Index lookups can include RID
                // in search key only when all key positions have been used and
                // the search key maps to one concatanted key value.
                // For example, an index on (a,b,c) can be searched using keys
                //  (a=10, b=2, c=6, RID=10001)
                // but not
                //  (a=10, b=2, RID=10001)
                // nor
                //  (a=10, b=2, c>6, RID=10001)
                inputRelsToIntersect[0] =
                    new LcsIndexMergeRel(
                        lcsTable,
                        oldIndexSearchRel,
                        startRidParamId,
                        rowLimitParamId,
                        oldIndexMergeRel.ridLimitParamId);
            } else {
                // recreate the index search with the appropriate dynamic
                // params
                assert (oldIndexAccessRel instanceof LcsIndexSearchRel);
                LcsIndexSearchRel oldIndexSearch =
                    (LcsIndexSearchRel) oldIndexAccessRel;
                inputRelsToIntersect[0] =
                    oldIndexSearch.cloneWithNewParams(
                        startRidParamId,
                        rowLimitParamId);
            }
        }

        FennelSingleRel newIndexAccessRel =
            newIndexRel(
                relImplementor,
                cluster,
                lcsTable,
                index,
                keyInput,
                inputKeyProj,
                inputDirectiveProj,
                startRidParamId,
                rowLimitParamId,
                requireMerge);

        inputRelsToIntersect[numInputRelsToIntersect - 1] = newIndexAccessRel;

        LcsIndexIntersectRel intersectRel =
            new LcsIndexIntersectRel(
                cluster,
                inputRelsToIntersect,
                lcsTable,
                startRidParamId,
                rowLimitParamId);

        return intersectRel;
    }

    /**
     * Add new index access rel using a given index to the input of the row scan
     * rel matched by the call. This method is only called from RelOptRule
     * class, as it requires the the rule call as a parameter. Additionally,
     * this rule has to match LcsRowScanRel as part of the pattern.
     *
     * @param oldIndexAccessRel existing index access rel
     * @param call call matched by a rule
     * @param rowScanRelPosInCall position of LcsRowScanRel in the sequence of
     * rels matched by the rule
     * @param index index to add to the access path
     * @param keyInput input rel to the index search
     * @param inputKeyProj key projection from index search input
     * @param inputDirectiveProj directive projection from index search input
     * @param requireMerge whether a LcsIndexMergeRel should be added on top of
     * the newly created LcsIndexSearchRel. If the input to LcsIndexSearchRel is
     * known to search to just one bitmap, then no merge is required. All other
     * cases, for example, when the input comes from a sort, a merge is
     * required.
     *
     * @return the new index access rel created
     */
    public static RelNode addNewIndexAccessRel(
        RelNode oldIndexAccessRel,
        RelOptRuleCall call,
        int rowScanRelPosInCall,
        FemLocalIndex index,
        RelNode keyInput,
        Integer [] inputKeyProj,
        Integer [] inputDirectiveProj,
        boolean requireMerge)
    {
        RelNode rowScanRel = call.rels[rowScanRelPosInCall];
        assert (rowScanRel instanceof LcsRowScanRel);
        FennelRelImplementor relImplementor =
            FennelRelUtil.getRelImplementor((LcsRowScanRel) rowScanRel);
        RelOptCluster cluster = rowScanRel.getCluster();
        LcsTable lcsTable = ((LcsRowScanRel) rowScanRel).lcsTable;

        // if there already is a child underneath the rowscan, then we'll
        // need to create an intersect; for the intersect, we'll need dynamic
        // parameters; either create new ones if there is no intersect yet or
        // reuse the existing ones
        FennelRelParamId startRidParamId = null;
        FennelRelParamId rowLimitParamId = null;

        boolean needIntersect = false;
        if (oldIndexAccessRel != null) {
            if (oldIndexAccessRel instanceof LcsIndexIntersectRel) {
                startRidParamId =
                    ((LcsIndexIntersectRel) oldIndexAccessRel)
                    .getStartRidParamId();
                rowLimitParamId =
                    ((LcsIndexIntersectRel) oldIndexAccessRel)
                    .getRowLimitParamId();
                needIntersect = true;
            } else {
                if ((oldIndexAccessRel instanceof LcsIndexMergeRel)
                    || (oldIndexAccessRel instanceof LcsIndexSearchRel))
                {
                    startRidParamId = relImplementor.allocateRelParamId();
                    rowLimitParamId = relImplementor.allocateRelParamId();
                    needIntersect = true;
                }
            }
        }

        // All other cases:
        // (1) no input to rowScanRel
        // (2) input is residual column filter(union rel, or values rel)
        // Intersect is not required, and startRid/rowLimit parameters are not
        // used.

        RelNode newIndexAccessRel;

        // Now create the new index access rel
        if (needIntersect) {
            newIndexAccessRel =
                addIntersect(
                    oldIndexAccessRel,
                    call,
                    rowScanRelPosInCall,
                    index,
                    keyInput,
                    inputKeyProj,
                    inputDirectiveProj,
                    startRidParamId,
                    rowLimitParamId,
                    requireMerge);
        } else {
            newIndexAccessRel =
                newIndexRel(
                    relImplementor,
                    cluster,
                    lcsTable,
                    index,
                    keyInput,
                    inputKeyProj,
                    inputDirectiveProj,
                    startRidParamId,
                    rowLimitParamId,
                    requireMerge);
        }

        return newIndexAccessRel;
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
                (index1.getIndexedFeature().size()
                    - index2.getIndexedFeature().size());

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
    private static class IndexPageCountComparator
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

            if ((pageCount1 != null) && (pageCount2 != null)) {
                compRes = pageCount1.compareTo(pageCount2);
            } else if (pageCount1 != null) {
                compRes = 1;
            } else if (pageCount2 != null) {
                compRes = -1;
            }

            if (compRes == 0) {
                compRes = index1.getName().compareTo(index2.getName());
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
                colStats = tabStats.getColumnStatistics(columnPos, sargSeq);
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
     * SargColumnFilterComparator is used to sort SargColumnFilters based on the
     * selectivity of the filter. If the selectivity is unknown, it then sorts
     * based on column position.
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
            int compRes = 0;
            if ((colSel1 != null) && (colSel2 != null)) {
                compRes = colSel1.compareTo(colSel2);
            }

            if (compRes == 0) {
                compRes = filter1.columnPos - filter2.columnPos;
            }

            return compRes;
        }
    }

    /*
     * CandidateIndex represents an index chosen to evaluate a predicate
     * expressed in SargIntervalSequence.
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
            assert (sargSeqList.size() == matchedPos);
            this.index = index;
            this.matchedPos = matchedPos;
            this.sargSeqList = sargSeqList;
        }

        boolean requireMerge()
        {
            boolean isPartialMatch =
                matchedPos < index.getIndexedFeature().size();

            return (sargSeqList.get(sargSeqList.size() - 1).isRange()
                || (sargSeqList.get(sargSeqList.size() - 1).isPoint()
                    && isPartialMatch));
        }
    }

    /*
     *  Filter2IndexMapping describes the set of indexes that can be used to
     *  evaluate the filter conditions, as well as the cost of using these
     *  indexes.
     */
    private static class Filter2IndexMapping
    {
        Map<SargColumnFilter, FemLocalIndex> filter2IndexMap;
        Map<FemLocalIndex, Integer> index2MatchedPosMap;

        Filter2IndexMapping()
        {
            filter2IndexMap = new HashMap<SargColumnFilter, FemLocalIndex>();
            index2MatchedPosMap = new HashMap<FemLocalIndex, Integer>();
        }

        void add(SargColumnFilter filter, FemLocalIndex index)
        {
            filter2IndexMap.put(filter, index);

            Integer matchedPos = index2MatchedPosMap.get(index);

            if (matchedPos == null) {
                index2MatchedPosMap.put(index, 1);
            } else {
                index2MatchedPosMap.put(index, matchedPos + 1);
            }
        }

        /**
         * adds a tuple(index,listOfFilters). a tuple (filter, index) is always
         * added regardless the existence of the filter in the filter2IndexMap
         * existing (filter,index) will be overridden, but the mapped position
         * of the overridden index will not be changed Example:
         *
         * <ul>
         * <li>with point filters on columns A,B,C
         * <li>Index X(A,B) and Y(B,C)
         * <li>generated tuples will be (X,A,B) and (Y,B,C)
         * </ul>
         *
         * <ul>
         * <li>adding first tuple:
         * <li>filter2IndexMap will have (A,X) and (B,X)
         * <li>index2MatchedPosMap will have (X,2)
         * </ul>
         *
         * <ul>
         * <li>adding second tuple:
         * <li>filter2IndexMap will have (A,X) and (B,Y) and (C,Y)
         * <li>index2MatchedPosMap will have (X,2) and (Y,2)
         * </ul>
         *
         * @param tup the tuple to add
         */
        void add(IndexFilterTuple tup)
        {
            Iterator iter = tup.getFilterList().iterator();
            SargColumnFilter filter;
            while (iter.hasNext()) {
                filter = (SargColumnFilter) iter.next();
                add(filter, tup.getIndex());
            }
        }

        /**
         * Remove a filter from the current mapping. Note this method removes
         * the mapping of a filter that maps to an index with all prior
         * positions still mapped to other filters. The recursion that callss
         * this method ensures filters are "popped" in the reverse order they
         * are matched to index positions, and the adjustment to "mapped index
         * position" is therefore -1.
         *
         * <p>Example:
         *
         * <ul>
         * <li>Filters on A, B, C
         * <li>Index X on C,A,B
         * <li>Index Y on C,B,A
         * </ul>
         *
         * <p>Filter2Indexmapping goes through the following state change in one
         * call of getBestIndex(). The fields in () are the index and its
         * current mapped position. The mapping in [] are the ones costed. C
         * (X,1) -&gt; C,A (X,2) -&gt; [C,A,B (X,3)] -&gt; C,A (X,2) -&gt; C
         * (X,1) -&gt; C (Y,1) -&gt; C,B(Y,2) -&gt; [C,B,A (Y,3)]
         *
         * @param filter filter to unmap
         */
        void remove(SargColumnFilter filter)
        {
            FemLocalIndex index = filter2IndexMap.get(filter);

            filter2IndexMap.remove(filter);

            Integer matchedPos = index2MatchedPosMap.get(index);

            if (matchedPos == null) {
                return;
            } else if (matchedPos == 1) {
                //Remove the only filter mapped to this index
                index2MatchedPosMap.remove(index);
            } else {
                // Remove the last mapped filter to this index
                index2MatchedPosMap.put(index, matchedPos - 1);
            }
        }

        void clear()
        {
            filter2IndexMap.clear();
            index2MatchedPosMap.clear();
        }

        void copyFrom(Filter2IndexMapping srcMapping)
        {
            filter2IndexMap.clear();
            index2MatchedPosMap.clear();

            filter2IndexMap.putAll(srcMapping.filter2IndexMap);
            index2MatchedPosMap.putAll(srcMapping.index2MatchedPosMap);
        }

        Double getSelectivity(RelStatSource tabStats)
        {
            return getCombinedSelectivity(filter2IndexMap.keySet(), tabStats);
        }

        // Override Object.hashCode()
        public int hashCode()
        {
            return index2MatchedPosMap.hashCode();
        }
    }

    /*
     * A tuple of (index, filters satisfiable by that index)
     * Notes:
     * Filter Map contains the ALL filters satisfiable by the index.
     * Effective selectivity is the combined selectivity of filters
     * satisfied by the index EXCLUDING those satisfied by other indexes that
     * have been previously added to the candidate list
     *
     *
     */
    public class IndexFilterTuple
    {
        private FemLocalIndex index;
        private List<SargColumnFilter> filterList;
        private Double effectiveSelectivity;

        /**
         * Constructs a tuple (index, filters satisfiable by this index) for
         * each indexed column, search in pointList and intervalList to see if
         * that indexed column satisfies any filters. Uses prefix matching rule
         * with one restriction: matching stops at the first matched range
         * filter. Example:
         *
         * <table>
         * <tr>
         * <td>point filters</td>
         * <td>range filters</td>
         * <td>index on</td>
         * <td>index satisfies</td>
         * </tr>
         * <tr>
         * <td>A,B,C</td>
         * <td>&nbsp;</td>
         * <td>(A,B,D,C)</td>
         * <td>A,B</td>
         * </tr>
         * <tr>
         * <td>B,C</td>
         * <td>&nbsp;</td>
         * <td>(A,B,C)</td>
         * <td>none</td>
         * </tr>
         * <tr>
         * <td>B</td>
         * <td>A</td>
         * <td>(A,B,C)</td>
         * <td>A</td>
         * </tr>
         * <tr>
         * <td>A,C</td>
         * <td>B</td>
         * <td>(A,B,C)</td>
         * <td>A,B</td>
         * </tr>
         * <tr>
         * <td>&nbsp;</td>
         * <td>A,B</td>
         * <td>(A,B,C)</td>
         * <td>A</td>
         * </tr>
         * </table>
         *
         * @param index the index to be matched with filters
         * @param pointList list of point filters
         * @param intervalList list of interval filters
         * @param mappedFilterSet set of filters previously mapped to some other
         * indexes
         */
        public IndexFilterTuple(
            FemLocalIndex index,
            List<SargColumnFilter> pointList,
            List<SargColumnFilter> intervalList,
            Set<SargColumnFilter> mappedFilterSet)
        {
            this.index = index;
            this.filterList = new LinkedList<SargColumnFilter>();

            List<CwmIndexedFeature> indexedFeatures = index.getIndexedFeature();
            boolean mappedToPointFilter = true;

            Iterator iter = indexedFeatures.iterator();
            while (iter.hasNext() && mappedToPointFilter) {
                CwmIndexedFeature indexedFeature =
                    (CwmIndexedFeature) iter.next();
                mappedToPointFilter = false;
                FemAbstractColumn col =
                    (FemAbstractColumn) indexedFeature.getFeature();

                for (SargColumnFilter pointFilter : pointList) {
                    FemAbstractColumn pointColumn =
                        rowScanRel.getColumnForFieldAccess(
                            pointFilter.columnPos);
                    if (col.equals(pointColumn)) {
                        mappedToPointFilter = true;
                        filterList.add(pointFilter);
                        break;
                    }
                }
                if (mappedToPointFilter) {
                    continue;
                }
                for (SargColumnFilter intervalFilter : intervalList) {
                    FemAbstractColumn intervalColumn =
                        rowScanRel.getColumnForFieldAccess(
                            intervalFilter.columnPos);
                    if (col.equals(intervalColumn)) {
                        filterList.add(intervalFilter);
                    }
                }
            }

            // compute selectivity excluding those in mappedFilterSet
            if (filterList.isEmpty()) {
                effectiveSelectivity = null;
            } else if ((mappedFilterSet == null) || mappedFilterSet.isEmpty()) {
                effectiveSelectivity =
                    getCombinedSelectivity(
                        new HashSet<SargColumnFilter>(filterList));
            } else {
                reCalculateEffectiveSelectivity(mappedFilterSet);
            }
        }

        public FemLocalIndex getIndex()
        {
            return index;
        }

        public List<SargColumnFilter> getFilterList()
        {
            return filterList;
        }

        public Double getEffectiveSelectivity()
        {
            return effectiveSelectivity;
        }

        public void reCalculateEffectiveSelectivity(
            Set<SargColumnFilter> mappedFilterSet)
        {
            HashSet<SargColumnFilter> difference =
                new HashSet<SargColumnFilter>(filterList);
            difference.removeAll(mappedFilterSet);
            effectiveSelectivity = getCombinedSelectivity(difference);
        }
    }

    /*
     * MappedFilterSelectivityComparator is used to sort IndexFilterTuple
     * by effective selectivity of the tuple. If either tuple has null
     * selectivity or the two tuples have same selectivity, they will be
     * sorted by StorageId of the index that constitute the tuple.
     *
     */
    public static class MappedFilterSelectivityComparator
        implements Comparator<IndexFilterTuple>
    {
        public MappedFilterSelectivityComparator()
        {
        }

        public int compare(IndexFilterTuple t1, IndexFilterTuple t2)
        {
            Double sel1 = t1.getEffectiveSelectivity();
            Double sel2 = t2.getEffectiveSelectivity();

            int compRes = 0;

            if ((sel1 != null) && (sel2 != null)) {
                compRes = sel1.compareTo(sel2);
            }

            if (compRes == 0) {
                compRes =
                    t1.getIndex().getStorageId().compareTo(
                        t2.getIndex().getStorageId());
            }

            return compRes;
        }

        public boolean equals(Object obj)
        {
            return (obj instanceof MappedFilterSelectivityComparator);
        }
    }
}
//End LcsIndexOptimizer.java
