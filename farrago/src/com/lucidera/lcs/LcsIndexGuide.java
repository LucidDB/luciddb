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

import java.math.*;
import java.util.*;
import java.lang.Integer;

import com.lucidera.farrago.*;

import net.sf.farrago.catalog.FarragoCatalogUtil;
import net.sf.farrago.catalog.FarragoRepos;
import net.sf.farrago.cwm.core.VisibilityKindEnum;
import net.sf.farrago.cwm.keysindexes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.fennel.tuple.FennelStandardTypeDescriptor;
import net.sf.farrago.fennel.tuple.FennelStoredTypeDescriptor;
import net.sf.farrago.query.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.JmiUtil;

import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sarg.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.Util;

/**
 * LcsIndexGuide provides information about the mapping from catalog
 * definitions for LCS tables and their clusters to their Fennel representation.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class LcsIndexGuide
{
    protected static final int LbmBitmapSegMaxSize = 512;
    
    private FarragoTypeFactory typeFactory;
    
    private FarragoRepos repos;
    
    private CwmColumnSet table;
    
    private RelDataType unflattenedRowType;
    
    private RelDataType flattenedRowType;
    
    private int [] flatteningMap;
    
    private List<FemLocalIndex> clusteredIndexes;
    
    private List<FemLocalIndex> unclusteredIndexes;
    
    private List<Integer> clusterMap;
    
    private int numFlattenedCols;
    
    private int numUnFlattenedCols;
    
    /**
     * Construct an IndexGuide using a specific list of clustered indexes
     * 
     * @param typeFactory
     * @param table the column store table
     * @param clusteredIndexes list of clustered indexes
     */
    LcsIndexGuide(
        FarragoTypeFactory typeFactory,
        CwmColumnSet table,
        List<FemLocalIndex> clusteredIndexes)
    {
        this.typeFactory = typeFactory;
        repos = typeFactory.getRepos();
        
        this.table = table;
        
        unflattenedRowType =
            typeFactory.createStructTypeFromClassifier(table);      
        numUnFlattenedCols = unflattenedRowType.getFieldList().size();
        flatteningMap = new int[numUnFlattenedCols];
        
        flattenedRowType =
            SqlTypeUtil.flattenRecordType(
                typeFactory,
                unflattenedRowType,
                flatteningMap);
        numFlattenedCols = flattenedRowType.getFieldList().size();
        
        this.clusteredIndexes = clusteredIndexes;
        this.unclusteredIndexes = 
            FarragoCatalogUtil.getUnclusteredIndexes(repos, table);
        
        createClusterMap(clusteredIndexes);
    }
    
    /**
     * Construct an IndexGuide using the default list of indexes
     * @param typeFactory
     * @param table the column store table
     */
    LcsIndexGuide(
        FarragoTypeFactory typeFactory,
        CwmColumnSet table)
    {
        this(
            typeFactory, table, 
            FarragoCatalogUtil.getClusteredIndexes(
                typeFactory.getRepos(),
                table));
    }
    
    /**
     * Construct an IndexGuide using an unclustered index
     * 
     * @param typeFactory
     * @param table the column store table
     * @param unclusteredIndex an unclustered index
     */
    LcsIndexGuide(
        FarragoTypeFactory typeFactory,
        CwmColumnSet table,
        FemLocalIndex unclusteredIndex)
    {
        this(
            typeFactory, 
            table, 
            getUnclusteredCoverageSet(
                typeFactory.getRepos(), table, unclusteredIndex));
    }
    
    public static List<FemLocalIndex> getUnclusteredCoverageSet(
        FarragoRepos repos,
        CwmColumnSet table,
        FemLocalIndex unclusteredIndex)
    {
        //
        // Get the columns of the index
        //
        Set<CwmColumn> requiredColumns = new HashSet<CwmColumn>();
        for (Object f : unclusteredIndex.getIndexedFeature()) {
            CwmIndexedFeature indexedFeature = (CwmIndexedFeature) f;
            requiredColumns.add((CwmColumn) indexedFeature.getFeature());
        }
        
        //
        // Get the clustered indexes of the table
        //
        List<FemLocalIndex> clusteredIndexes = 
            FarragoCatalogUtil.getClusteredIndexes(repos, table);
        
        //
        // Find clustered indexes which cover the columns of the index
        //
        List<FemLocalIndex> coverageIndexes = 
            new ArrayList<FemLocalIndex>();
        for (FemLocalIndex clusteredIndex : clusteredIndexes) {
            boolean include = false;
            for (Object f: clusteredIndex.getIndexedFeature()) {
                CwmIndexedFeature indexedFeature = (CwmIndexedFeature) f;
                if (requiredColumns.contains(indexedFeature.getFeature())) {
                    include = true;
                    requiredColumns.remove(indexedFeature.getFeature());
                }
            }
            if (include) {
                coverageIndexes.add(clusteredIndex);
            }
        }
        
        if (! requiredColumns.isEmpty()) {
            // TODO: user error message
            throw Util.newInternal("unclustered index could not be covered");
        }
        
        return coverageIndexes;
    }
    
    /**
     * @return the flattened row type for the indexed table
     */
    public RelDataType getFlattenedRowType()
    {
        return flattenedRowType;
    }
    
    /**
     * Creates an array mapping cluster columns to table columns, the order of
     * the array matching the order of the cluster columns in an ordered list
     * of clusters.  E.g.,
     * 
     * <pre><code>
     * 
     * create table t(a int, b int, c int, d int);
     * create clustered index it_c on t(c);
     * create clustered index it_ab on t(a, b);
     * create clustered index it_d on t(d);
     * 
     * clusterMap[] = { 2, 0, 1, 3 }
     * 
     * </code></pre>
     * 
     * @param clusteredIndexes ordered list of clusters
     * 
     * @return mapping array created
     */
    private void createClusterMap(List<FemLocalIndex> clusteredIndexes)
    {
        clusterMap = new ArrayList<Integer>();
        
        for (FemLocalIndex index : clusteredIndexes) {
            for (Object f : index.getIndexedFeature()) {
                CwmIndexedFeature indexedFeature = (CwmIndexedFeature) f;
                FemAbstractColumn column = 
                    (FemAbstractColumn) indexedFeature.getFeature();
                addClusterCols(column.getOrdinal());
            }
        }
    }
    
    /**
     * Flattens a column and adds an entry for each subcolumn within the
     * flattened column into clusterMap
     * 
     * @param colOrdinal 0-based ordinal representing an unflattened column
     */
    private void addClusterCols(int colOrdinal)
    {
        int nColsToAdd = getNumFlattenedSubCols(colOrdinal);
        
        colOrdinal = flattenOrdinal(colOrdinal);
        
        for (int i = colOrdinal; i < colOrdinal + nColsToAdd; i++) {
            clusterMap.add(i);
        }
    }
    
    /**
     * Returns number of subcolumns corresponding to a column once it is
     * flattened.
     * 
     * @param colOrdinal 0-based ordinal representing an unflattened column
     * 
     * @return number of subcolumns in flattened column
     */
    public int getNumFlattenedSubCols(int colOrdinal)
    {
        int nCols;
        
        if (colOrdinal == numUnFlattenedCols - 1) {
            nCols = numFlattenedCols - flatteningMap[colOrdinal];
        } else {
            nCols = flatteningMap[colOrdinal + 1] - flatteningMap[colOrdinal];
        }
        return nCols;
    }
    
    /**
     * Retrieves number of columns in a clustered index
     * 
     * @param index the clustered index
     * 
     * @return number of columns 
     */
    public int getNumFlattenedClusterCols(FemLocalIndex index)
    {
        int nCols = 0;
        
        assert(index.isClustered());
        for (Object f : index.getIndexedFeature()) {
            CwmIndexedFeature indexedFeature = (CwmIndexedFeature) f;
            FemAbstractColumn column = 
                (FemAbstractColumn) indexedFeature.getFeature();
            nCols += getNumFlattenedSubCols(column.getOrdinal());
        }
        return nCols;
    }
    
    /**
     * Retrieves number of columns in all the clustered indexes accessed
     * 
     * @return number of columns 
     */
    public int getNumFlattenedClusterCols()
    {
        int nCols = 0;
        
        for (FemLocalIndex index : clusteredIndexes) {
            for (Object f : index.getIndexedFeature()) {
                CwmIndexedFeature indexedFeature = (CwmIndexedFeature) f;
                FemAbstractColumn column = 
                    (FemAbstractColumn) indexedFeature.getFeature();
                nCols += getNumFlattenedSubCols(column.getOrdinal());
            }
        }
        return nCols;
    }
    
    /**
     * Creates a tuple descriptor corresponding to a clustered index
     * 
     * @param index clustered index
     *
     * @return tuple descriptor for cluster
     */
    public FemTupleDescriptor getClusterTupleDesc(FemLocalIndex index)
    {
        FemTupleDescriptor tupleDesc = repos.newFemTupleDescriptor();
        List flattenedColList = flattenedRowType.getFieldList();
        
        for (Object f : index.getIndexedFeature()) {
            
            CwmIndexedFeature indexedFeature = (CwmIndexedFeature) f;
            FemAbstractColumn column = 
                (FemAbstractColumn) indexedFeature.getFeature();
            int numSubCols = getNumFlattenedSubCols(column.getOrdinal());
            int colOrd = flattenOrdinal(column.getOrdinal());
            
            // add an entry for each subcolumn within a complex type
            for (int i = colOrd; i < colOrd + numSubCols; i++) {
                RelDataTypeField field = 
                    (RelDataTypeField) flattenedColList.get(i);
                FennelRelUtil.addTupleAttrDescriptor(
                    repos,
                    tupleDesc,
                    field.getType());
            }
        }
        return tupleDesc;
    }
    
    /**
     * Creates a projection list relative to the cluster columns
     * 
     * @param origProj original projection list relative to the table; if null,
     * project all columns from table
     * 
     * @return projection list created
     */
    public Integer [] computeProjectedColumns(Integer [] origProj)
    {
        Integer [] proj;
        int i;
        
        // use the inverse of the cluster map to locate the corresponding
        // cluster column number
        if (origProj != null) {
            proj = new Integer[origProj.length];
            for (i = 0; i < origProj.length; i++) {
                proj[i] = computeProjectedColumn(origProj[i].intValue());
            }
        } else {
            proj = new Integer[numFlattenedCols];
            for (i = 0; i < proj.length; i++) {
                proj[i] = computeProjectedColumn(i);
            }
        }
        return proj;
    }
    
    private Integer computeProjectedColumn(int i)
    {
        if (LucidDbOperatorTable.ldbInstance().isSpecialColumnId(i)) {
            return new Integer(i);
        } else {
            int j = clusterMap.indexOf(i);
            assert(j != -1);
            return new Integer(j);
        }
    }
    
    /**
     * Determines if an index is referenced by projection list
     * 
     * @param index clustered index being checked
     * @param projection array of flattened ordinals of projected columns
     * 
     * @return true if at least one column in the clustered index is
     * referenced in the projection list
     */
    public boolean testIndexCoverage(
        FemLocalIndex index,
        Integer [] projection)
    {
        for (Object f : index.getIndexedFeature()) {
            CwmIndexedFeature indexedFeature = (CwmIndexedFeature) f;
            FemAbstractColumn column = 
                (FemAbstractColumn) indexedFeature.getFeature();
            if (testColumnCoverage(column, projection)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Determines if a column is referenced by projection list.
     * 
     * @param column column being checked
     * @param projection array of flattened ordinals of projected columns
     * 
     * @return true if the column (or one of its sub-fields for a column with
     * structured type) is referenced in the projection list
     */
    public boolean testColumnCoverage(
        FemAbstractColumn column,
        Integer [] projection)
    {
        int n = flattenOrdinal(column.getOrdinal());
        int nEnd = n + getNumFlattenedSubCols(column.getOrdinal());
        for (int i = 0; i < projection.length; i++) {
            if ((projection[i] >= n) && (projection[i] < nEnd)) {
                return true;
            }
        }
        return false;
    }
    
    int flattenOrdinal(int columnOrdinal)
    {
        int i = flatteningMap[columnOrdinal];
        assert(i != -1);
        return i;
    }
    
    /**
     * Creates a tuple descriptor for the BTree index corresponding to a
     * clustered index.  For LCS clustered indexes, the stored tuple is always
     * the same: [RID, PageId]; and the key is just the RID.  In Fennel, both
     * attributes are represented as 64-bit ints.
     *
     * @return btree tuple descriptor
     */
    public FemTupleDescriptor createClusteredBTreeTupleDesc()
    {
        FemTupleDescriptor tupleDesc = repos.newFemTupleDescriptor();
        
        // add RID
        appendInt64Attr(tupleDesc);
        
        // add PageId
        appendInt64Attr(tupleDesc);
        
        return tupleDesc;
    }
    
    private void appendInt64Attr(FemTupleDescriptor tupleDesc)
    {
        FennelStoredTypeDescriptor typeDesc =
            FennelStandardTypeDescriptor.INT_64;
        FemTupleAttrDescriptor attrDesc = repos.newFemTupleAttrDescriptor();
        tupleDesc.getAttrDescriptor().add(attrDesc);
        attrDesc.setTypeOrdinal(typeDesc.getOrdinal());
    }
    
    private void appendBitmapAttr(FemTupleDescriptor tupleDesc)
    {
        FennelStoredTypeDescriptor typeDesc =
            FennelStandardTypeDescriptor.VARBINARY;
        FemTupleAttrDescriptor attrDesc = repos.newFemTupleAttrDescriptor();
        tupleDesc.getAttrDescriptor().add(attrDesc);
        attrDesc.setTypeOrdinal(typeDesc.getOrdinal());
        attrDesc.setNullable(true);
        
        // REVIEW jvs 6-Jan-2006: this is based on a 32K page size with a
        // maximum entry size of 1/8 of a page.  Should probably make it
        // communicate with native code about this to get the right number
        // automatically.
        attrDesc.setByteLength(LbmBitmapSegMaxSize);
    }
    
    // TODO jvs 6-Jan-2005:  use this in LcsTableAppendRel.
    
    /**
     * Creates a tuple projection for the RID attribute of the BTree index
     * corresponding to a clustered index.
     *
     * @see createClusteredBTreeTupleDesc
     *
     * @return RID attribute projection
     */
    public FemTupleProjection createClusteredBTreeRidDesc()
    {
        return FennelRelUtil.createTupleProjection(
            repos,
            new Integer [] { 0 });
    }
    
    /**
     * Creates a tuple projection for the PageId attribute of the BTree index
     * tuple corresponding to a clustered index.
     *
     * @see createClusteredBTreeTupleDesc
     *
     * @return PageId attribute projection
     */
    public FemTupleProjection createClusteredBTreePageIdDesc()
    {
        return FennelRelUtil.createTupleProjection(
            repos,
            new Integer [] { 1 });
    }
    
    /**
     * Creates a tuple descriptor for the BTree index corresponding to an
     * unclustered index.
     *
     * <p>
     *
     * For LCS unclustered indexes, the stored tuple is
     * [K1, K2, ..., RID, BITMAP, BITMAP], and the key is [K1, K2, ..., RID]
     *
     * @param index unclustered index 
     *
     * @return btree tuple descriptor
     */
    public FemTupleDescriptor createUnclusteredBTreeTupleDesc(
        FemLocalIndex index)
    {
        assert(!index.isClustered());
        
        FemTupleDescriptor tupleDesc = repos.newFemTupleDescriptor();
        
        // add K1, K2, ...
        Iterator iter = index.getIndexedFeature().iterator();
        while (iter.hasNext()) {
            CwmIndexedFeature indexedFeature = (CwmIndexedFeature) iter.next();
            FemAbstractColumn column =
                (FemAbstractColumn) indexedFeature.getFeature();
            FennelRelUtil.addTupleAttrDescriptor(
                repos,
                tupleDesc,
                typeFactory.createCwmElementType(column));
        }
        
        // add RID
        appendInt64Attr(tupleDesc);
        
        // add BITMAP
        appendBitmapAttr(tupleDesc);
        appendBitmapAttr(tupleDesc);
        
        return tupleDesc;
    }
    
    public FemTupleDescriptor createUnclusteredBTreeBitmapDesc()
    {
        FemTupleDescriptor tupleDesc = repos.newFemTupleDescriptor();
        
        // add RID
        appendInt64Attr(tupleDesc);
        
        // add BITMAP
        appendBitmapAttr(tupleDesc);
        appendBitmapAttr(tupleDesc);
        
        return tupleDesc;
    }
    
    /**
     * Creates a tuple projection for the key attributes of the BTree index
     * corresponding to an unclustered index.
     *
     * @param index unclustered index 
     *
     * @see createUnclusteredBTreeTupleDesc
     *
     * @return key attribute projection
     */
    public FemTupleProjection createUnclusteredBTreeKeyProj(
        FemLocalIndex index)
    {
        // number of key fields = number of columns plus RID
        int n = index.getIndexedFeature().size() + 1;
        
        return FennelRelUtil.createTupleProjection(
            repos,
            FennelRelUtil.newIotaProjection(n));
    }
    
    public FemTupleProjection createUnclusteredBTreeBitmapProj(
        FemLocalIndex index)
    {
        List<Integer> bitmapProj = new ArrayList<Integer>();
        
        // bitmap tuple format is 
        //    [key0, key1..., keyN, RID, SegmentDesc, Segment]
        // the bitmap fields are:      
        //                         {RID, SegmentDesc, Segment]
        int startPos = index.getIndexedFeature().size();
        
        bitmapProj.add(startPos);
        bitmapProj.add(startPos + 1);
        bitmapProj.add(startPos + 2);
        
        return FennelRelUtil.createTupleProjection(
            repos, bitmapProj);
    }
    
    //~ Exec Streams -------------------------------------------------------
    
    protected FemSplitterStreamDef newSplitter(SingleRel rel)
    {
        FemSplitterStreamDef splitter = repos.newFemSplitterStreamDef();
        
        splitter.setOutputDesc(
            FennelRelUtil.createTupleDescriptorFromRowType(
                repos,
                typeFactory,
                rel.getChild().getRowType()));
        return splitter;          
    }
    
    protected FemBarrierStreamDef newBarrier(FennelRel rel)
    {
        FemBarrierStreamDef barrier = repos.newFemBarrierStreamDef();
        
        barrier.setOutputDesc(
            FennelRelUtil.createTupleDescriptorFromRowType(
                repos,
                typeFactory,
                rel.getRowType()));
        
        return barrier;
    }
    
    protected FemLcsClusterAppendStreamDef newClusterAppend(
        FennelRel rel,
        FemLocalIndex clusterIndex)
    {
        FemLcsClusterAppendStreamDef clusterAppend = 
            repos.newFemLcsClusterAppendStreamDef();
        
        defineIndexStream(clusterAppend, rel, clusterIndex, true);
        
        //
        // Set up FemLcsClusterAppendStreamDef
        //        - setOverwrite
        //        - setClusterColProj
        //
        clusterAppend.setOverwrite(false);
        
        Integer[] clusterColProj;
        clusterColProj = 
            new Integer[getNumFlattenedClusterCols(clusterIndex)];
        
        //
        // Figure out the projection covering columns contained in each index.
        //
        int i = 0;
        for (Object f : clusterIndex.getIndexedFeature()) {
            CwmIndexedFeature indexedFeature = (CwmIndexedFeature) f;
            FemAbstractColumn column = 
                (FemAbstractColumn) indexedFeature.getFeature();
            int n = getNumFlattenedSubCols(column.getOrdinal());
            for (int j = 0; j < n; ++j) {
                clusterColProj[i] =
                    flattenOrdinal(column.getOrdinal()) + j;
                i++;
            }
        }
        
        clusterAppend.setClusterColProj(
            FennelRelUtil.createTupleProjection(repos, clusterColProj));
        
        return clusterAppend;
        
    }
    
    protected FemLcsRowScanStreamDef newRowScan(
        LcsRowScanRel rel,
        Integer[] projectedColumns)
    {
        FemLcsRowScanStreamDef scanStream = repos.newFemLcsRowScanStreamDef();
        
        defineScanStream(scanStream, rel);
        
        // setup the output projection relative to the ordered list of
        // clustered indexes
        Integer [] clusterProjection = 
            computeProjectedColumns(projectedColumns);
        scanStream.setOutputProj(
            FennelRelUtil.createTupleProjection(repos, clusterProjection));
        scanStream.setFullScan(rel.isFullScan());
        scanStream.setHasExtraFilter(rel.hasExtraFilter());
        
        return scanStream;
    }
    
    /**
     * Creates a set of streams for updating a bitmap index
     */
    protected LcsCompositeStreamDef newBitmapAppend(
        FennelRel rel,
        FemLocalIndex index,
        FennelRelImplementor implementor,
        boolean createIndex,
        FennelRelParamId dynParamId)
    {
        // create the streams
        FemExecutionStreamDef generator = 
            newGenerator(rel, index, createIndex, 
                implementor.translateParamId(dynParamId).intValue());
        FemExecutionStreamDef sorter = newSorter(index);
        FemExecutionStreamDef splicer = 
            newSplicer(
                rel, index, 
                implementor.translateParamId(dynParamId).intValue());
        
        // link them up
        implementor.addDataFlowFromProducerToConsumer(generator, sorter);
        implementor.addDataFlowFromProducerToConsumer(sorter, splicer);
        
        return new LcsCompositeStreamDef(generator, splicer);
    }
    
    private FemLbmGeneratorStreamDef newGenerator(
        FennelRel rel,
        FemLocalIndex index,
        boolean createIndex,
        int dynParamId)
    {
        FemLbmGeneratorStreamDef generator = 
            repos.newFemLbmGeneratorStreamDef();
        
        //
        // Setup cluster scans
        //
        defineScanStream(generator, rel);
        
        //
        // Setup projection used by unclustered index
        //
        List indexFeatures = index.getIndexedFeature();
        Integer[] indexProjection = new Integer[indexFeatures.size()];
        for (int i = 0; i < indexFeatures.size(); i++) {
            CwmIndexedFeature feature = 
                (CwmIndexedFeature) indexFeatures.get(i);
            FemAbstractColumn column = 
                (FemAbstractColumn) feature.getFeature();
            indexProjection[i] = new Integer(column.getOrdinal());
        }
        generator.setOutputProj(
            FennelRelUtil.createTupleProjection(
                repos,
                computeProjectedColumns(indexProjection)));
        
        // 
        // Setup index creation flag
        //
        generator.setCreateIndex(createIndex);
        
        //
        // Setup dynamic param id
        //
        generator.setRowCountParamId(dynParamId);
        
        //
        // Setup Btree accessor parameters
        //
        defineIndexAccessor(generator, rel, index, false);
        
        //
        // Set up FemExecutionStreamDef
        //        - setOutputDesc (same as tupleDesc)
        //
        generator.setOutputDesc(
            createUnclusteredBTreeTupleDesc(index));
        
        return generator;
    }
    
    protected FemSortingStreamDef newSorter(
        FemLocalIndex index)
    {
        FemSortingStreamDef sortingStream =
            repos.newFemSortingStreamDef();
        
        //
        // Bitmap entry keys should be unique, but we save the effort 
        // of enforcing uniqueness
        //
        sortingStream.setDistinctness(DistinctnessEnum.DUP_ALLOW);
        sortingStream.setKeyProj(createUnclusteredBTreeKeyProj(index));
        sortingStream.setOutputDesc(createUnclusteredBTreeTupleDesc(index));
        return sortingStream;
    }
    
    protected FemLbmSplicerStreamDef newSplicer(
        FennelRel rel,
        FemLocalIndex index,
        int dynParamId)
    {
        FemLbmSplicerStreamDef splicer =
            repos.newFemLbmSplicerStreamDef();
        
        //
        // The splicer is the terminal stream of a bitmap stream set.
        // It's output type is the same as the rel's: the standard 
        // Dml output type.
        //
        defineIndexStream(splicer, rel, index, false);
        
        splicer.setRowCountParamId(dynParamId);
        
        return splicer;
    }
    
    protected FemIndexScanDef newIndexScan(
        FennelRel rel,
        FemLocalIndex index)
    {
        FemIndexScanDef scanStream = repos.newFemIndexScanDef();
        defineIndexScan(scanStream, rel, index);
        
        return scanStream;
    }
    
    protected FemLbmIndexScanStreamDef newIndexSearch(
        FennelRel rel,
        FemLocalIndex index,
        boolean isUniqueKey,
        boolean isOuter,
        Integer [] inputKeyProj,
        Integer [] inputJoinProj,
        Integer [] inputDirectiveProj,
        FennelDynamicParamId startRidParamId,
        FennelDynamicParamId rowLimitParamId)
    {
        FemLbmIndexScanStreamDef searchStream =
            repos.newFemLbmIndexScanStreamDef();
        defineIndexScan(searchStream, rel, index);
        
        searchStream.setStartRidParamId(startRidParamId.intValue());
        searchStream.setRowLimitParamId(rowLimitParamId.intValue());
        
        searchStream.setUniqueKey(isUniqueKey);
        searchStream.setOuterJoin(isOuter);
        
        if (inputKeyProj != null) {
            searchStream.setInputKeyProj(
                FennelRelUtil.createTupleProjection(repos, inputKeyProj));
        }
        
        if (inputJoinProj != null) {
            searchStream.setInputJoinProj(
                FennelRelUtil.createTupleProjection(repos, inputJoinProj));
        }
        
        if (inputDirectiveProj != null) {
            searchStream.setInputDirectiveProj(
                FennelRelUtil.createTupleProjection(repos, inputDirectiveProj));
        }
        
        return searchStream;
    }
    
    private void defineIndexScan(
        FemIndexScanDef scanStream,
        FennelRel rel,
        FemLocalIndex index)
    {
        
        defineIndexStream(scanStream, rel, index, false);
        
        // set FemIndexScanDef
        //
        // TODO: handle the case where the index scan satisfy the 
        // projection out of the LcsRowScanRel. For now, output projection 
        // maps to [RID, bitmapfield1, bitmapfield2].
        scanStream.setOutputProj(
            createUnclusteredBTreeBitmapProj(index));
    }
    
    private void defineIndexStream(
        FemIndexStreamDef indexStream,
        FennelRel rel,
        FemLocalIndex index,
        boolean clustered)
    {
        //
        // Set up FemExecutionStreamDef
        //        - setOutputDesc
        //
        indexStream.setOutputDesc(
            FennelRelUtil.createTupleDescriptorFromRowType(
                repos,
                typeFactory,
                rel.getRowType()));
        
        defineIndexAccessor(indexStream, rel, index, clustered);
    }
    
    private void defineIndexAccessor(
        FemIndexAccessorDef indexAccessor,
        FennelRel rel,
        FemLocalIndex index,
        boolean clustered) 
    {
        final FarragoPreparingStmt stmt =
            FennelRelUtil.getPreparingStmt(rel);
        
        //
        // Set up FemIndexAccessorDef
        //        - setRootPageId
        //        - setSegmentId
        //        - setTupleDesc
        //        - setKeyProj
        //
        indexAccessor.setRootPageId(
            stmt.getIndexMap().getIndexRoot(index));
        
        indexAccessor.setSegmentId(
            LcsDataServer.getIndexSegmentId(index));
        
        long indexId = JmiUtil.getObjectId(index);
        
        indexAccessor.setIndexId(indexId);
        
        FemTupleDescriptor indexTupleDesc;
        if (clustered) {
            indexTupleDesc = createClusteredBTreeTupleDesc();
        } else {
            indexTupleDesc = createUnclusteredBTreeTupleDesc(index);
        }
        indexAccessor.setTupleDesc(indexTupleDesc);
        
        FemTupleProjection femProj;
        if (clustered) {
            //
            // The key is simply the RID from the [RID, PageId] mapping stored in
            // this index.
            //
            Integer[] keyProj ={0};
            femProj = FennelRelUtil.createTupleProjection(repos, keyProj);
        } else {
            femProj = createUnclusteredBTreeKeyProj(index);
        }
        indexAccessor.setKeyProj(femProj);
    }
    
    /**
     * Fills in a stream definition for this scan.
     *
     * @param scanStream stream definition to fill in
     */
    private void defineScanStream(
        FemLcsRowScanStreamDef scanStream,
        FennelRel rel)
    {  
        // setup each cluster scan def
        for (FemLocalIndex index : clusteredIndexes) {
            FemLcsClusterScanDef clusterScan = repos.newFemLcsClusterScanDef();
            defineClusterScan(index, rel, clusterScan);
            scanStream.getClusterScan().add(clusterScan);
        }
    }
    
    /**
     * Fills in a cluster scan def for this scan
     * 
     * @param index clustered index corresponding to this can
     * @param clusterScan clustered scan to fill in
     */
    private void defineClusterScan(
        FemLocalIndex index,
        FennelRel rel,
        FemLcsClusterScanDef clusterScan)
    {
        final FarragoPreparingStmt stmt =
            FennelRelUtil.getPreparingStmt(rel);
        
        // setup cluster tuple descriptor and add to cluster map
        FemTupleDescriptor clusterDesc =
            getClusterTupleDesc(index);
        clusterScan.setClusterTupleDesc(clusterDesc);
        
        // setup index accessor def fields
        
        if (!FarragoCatalogUtil.isIndexTemporary(index)) {
            clusterScan.setRootPageId(stmt.getIndexMap().getIndexRoot(index));
        } else {
            // For a temporary index, each execution needs to bind to
            // a session-private root.  So don't burn anything into
            // the plan.
            clusterScan.setRootPageId(-1);
        }
        
        clusterScan.setSegmentId(LcsDataServer.getIndexSegmentId(index));
        clusterScan.setIndexId(JmiUtil.getObjectId(index));
        
        clusterScan.setTupleDesc(
            createClusteredBTreeTupleDesc());
        
        clusterScan.setKeyProj(
            createClusteredBTreeRidDesc());
    }
    
    /**
     * Creates input values for a bitmap index generator
     * 
     * <p>
     * 
     * The inputs are two columns: [rowCount startRid] 
     */
    protected RexNode [] getUnclusteredInputs(RexBuilder builder)
    {
        // First obtain the row count and starting row id
        //
        // TODO: Make this work for the incremental case
        BigDecimal rowCount = BigDecimal.ZERO;
        BigDecimal startRid = BigDecimal.ZERO;
        
        // TODO: These types must match the fennel types for
        // RecordNum and LcsRowId
        RelDataType rowCountType = 
            typeFactory.createSqlType(SqlTypeName.Bigint);
        RelDataType ridType = rowCountType;
        
        return new RexNode[] {
            builder.makeExactLiteral(rowCount, rowCountType), 
            builder.makeExactLiteral(startRid, ridType)
        };
    }
    
    protected RelDataType getUnclusteredInputType() 
    {
        // TODO: These types must match the fennel types for
        // RecordNum and LcsRowId
        RelDataType rowCountType = 
            typeFactory.createSqlType(SqlTypeName.Bigint);
        RelDataType ridType = rowCountType;
        
        return typeFactory.createStructType(
            new RelDataType[] { rowCountType, ridType },
            new String [] { "ROWCOUNT", "SRID" }
        );
    }
    
    protected FemTupleDescriptor getUnclusteredInputDesc() 
    {
        return FennelRelUtil.createTupleDescriptorFromRowType(
            repos,
            typeFactory,
            getUnclusteredInputType());
    }
    
    // ~ Methods for unclustered(bitmap) index ------------------------
    
    /**
     * Creates a bitmap data row type, [SRID, SegmentDir, Segments]
     */
    public RelDataType createUnclusteredBitmapRowType()
    {
        RelDataType ridType = 
            typeFactory.createSqlType(SqlTypeName.Bigint);
        RelDataType bitmapType =
            typeFactory.createSqlType(SqlTypeName.Varbinary,
                LbmBitmapSegMaxSize);
        
        RelDataType segDescType = 
            typeFactory.createTypeWithNullability(bitmapType, true);
        
        RelDataType segType = 
            typeFactory.createTypeWithNullability(bitmapType, true);
        
        return typeFactory.createStructType(
            new RelDataType[] {ridType, segDescType, segType},
            new String [] {"SRID", "SegmentDesc","Segment"}
        );
    }
    
    /**
     * Creates a tuple descriptor for bitmap data
     */
    public FemTupleDescriptor createUnclusteredBitmapTupleDesc()
    {
        return FennelRelUtil.createTupleDescriptorFromRowType(
            repos,
            typeFactory,
            createUnclusteredBitmapRowType());
    }
    
    /**
     * Creates a key projection for bitmap data. The only key column 
     * is the first column, SRID
     */
    public FemTupleProjection createUnclusteredBitmapKeyProj()
    {
        return FennelRelUtil.createTupleProjection(
            repos,
            FennelRelUtil.newIotaProjection(1));
    }
    
    public boolean isValid(FemLocalIndex index)
    {
        return index.getVisibility() == VisibilityKindEnum.VK_PUBLIC;
    }    
    
    protected FemSortingStreamDef newBitmapSorter()
    {
        FemSortingStreamDef sortingStream =
            repos.newFemSortingStreamDef();
        sortingStream.setDistinctness(DistinctnessEnum.DUP_ALLOW);
        sortingStream.setKeyProj(createUnclusteredBitmapKeyProj());
        sortingStream.setOutputDesc(createUnclusteredBitmapTupleDesc());
        return sortingStream;
    }
    
    private void setBitmapStreamParams(
        FemLbmBitOpStreamDef bitOpStream, FennelDynamicParamId startRidParamId,
        FennelDynamicParamId rowLimitParamId)
    {
        bitOpStream.setStartRidParamId(startRidParamId.intValue());
        bitOpStream.setRowLimitParamId(rowLimitParamId.intValue());
        
        bitOpStream.setOutputDesc(createUnclusteredBitmapTupleDesc());
    }
    
    protected FemLbmIntersectStreamDef newBitmapIntersect(
        FennelDynamicParamId startRidParamId,
        FennelDynamicParamId rowLimitParamId)
    {
        FemLbmIntersectStreamDef intersectStream =
            repos.newFemLbmIntersectStreamDef();
        
        setBitmapStreamParams(
            intersectStream, startRidParamId, rowLimitParamId);
        
        return intersectStream;  
    }
    
    protected FemLbmMinusStreamDef newBitmapMinus(
        FennelDynamicParamId startRidParamId,
        FennelDynamicParamId rowLimitParamId)
    {
        FemLbmMinusStreamDef minusStream =
            repos.newFemLbmMinusStreamDef();
        
        setBitmapStreamParams(
            minusStream, startRidParamId, rowLimitParamId);
        
        return minusStream;  
    }
    
    public Map<CwmColumn, SargIntervalSequence> getCol2SeqMap(
        LcsRowScanRel origRowScan,
        List<SargBinding> sargBindingList)
    {
        Map<CwmColumn, SargIntervalSequence> colMap =
            new HashMap<CwmColumn, SargIntervalSequence>();
        
        for (int i = 0; i < sargBindingList.size(); i ++) {
            SargBinding sargBinding = sargBindingList.get(i);
            RexInputRef fieldAccess = sargBinding.getInputRef();
            FemAbstractColumn filterColumn =
                origRowScan.getColumnForFieldAccess(fieldAccess.getIndex());
            if (filterColumn != null) {
            
                SargIntervalSequence sargSeq = 
                    FennelRelUtil.evaluateSargExpr(sargBinding.getExpr());
            
                colMap.put(filterColumn, sargSeq);
            }
        }            
        
        return colMap;
    }
    
    /**
     * This is the algorithm that maps indexes to search key columns.
     * It does so by finding the shortest (in terms of key length) index to map to the
     * longest list of columns(in the case of composite key idnexes). The index selection is
     * expressed using a map where all selected indexes and  the matched key positions are
     * remembered. 
     * 
     * @param colLists two column lists: 
     *     the first one contains the "point interval" columns,
     *     the secod one contains the "range interval" columns.
     * @return a map from selected index to its associated matched key position.
     */
    public Map<FemLocalIndex, Integer> getIndex2PosMap(
        List<List<CwmColumn>> colLists)
    {
        assert (colLists.size() == 2);
        
        List<CwmColumn> pointColumnList = colLists.get(0);
        List<CwmColumn> rangeColumnList = colLists.get(1);
        
        Map<CwmColumn, FemLocalIndex> col2IndexMap = new HashMap<CwmColumn, FemLocalIndex>();
        Map<FemLocalIndex, Integer> index2PosMap = new HashMap<FemLocalIndex, Integer>();
        boolean matchedAll = false;
        
        // Sort the index based on length(and a unique identifier to break ties),
        // so that index with fewer key columns are searched first(and preferred).
        TreeSet<FemLocalIndex> indexSet = 
            new TreeSet<FemLocalIndex>(new IndexLengthComparator());
        
        indexSet.addAll(unclusteredIndexes);
        
        // First process the columns with point predicates.
        // Objective is to maximize the index key oclumns matched.
        while (pointColumnList.size() > 0 && !matchedAll) {
            
            // TODO: match the shortest index with the maximum matched positions
            Iterator iter = indexSet.iterator();
            int maxMatchedPos = 0;
            FemLocalIndex maxMatchedIndex = null;
            int matchedPos = 0;
            
            while (iter.hasNext()) {
                FemLocalIndex index = (FemLocalIndex) iter.next();
                
                if (isValid(index)) {
                    matchedPos = 0;
                    
                    CwmColumn col = getIndexColumn(index, matchedPos);
                    
                    while ((col != null) && pointColumnList.contains(col)) {
                        matchedPos ++;
                        col = getIndexColumn(index, matchedPos);
                    }
                    
                    // try to match one more column from the interval column list
                    if (rangeColumnList.contains(getIndexColumn(index, matchedPos))) {
                        matchedPos ++;
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
                    col2IndexMap.put(matchedCol, maxMatchedIndex);
                    
                    // remove matched columns from the set.
                    if (!pointColumnList.remove(matchedCol)) {
                        // last column might come from the interval list.
                        boolean removed = rangeColumnList.remove(matchedCol);
                        assert(removed);
                    }
                }
                
                // remove matched index from the set.
                indexSet.remove(maxMatchedIndex);
                
                // remember for each matched index, how many positions are matched.
                index2PosMap.put(maxMatchedIndex, new Integer(maxMatchedPos));
            } else {
                // no more match possible, get out of here
                matchedAll = true;
            }
        }
        
        Iterator iter = indexSet.iterator();
        
        // Process the columns with range predicates:
        // Simple assign the shortest index with matching first key column
        int maxMatchedPos = 1;
        while (rangeColumnList.size() > 0 && iter.hasNext()) {
            FemLocalIndex index = (FemLocalIndex) iter.next();
            CwmColumn firstCol = getIndexColumn(index, maxMatchedPos - 1);
            if ((firstCol != null) && rangeColumnList.contains(firstCol)) {
                col2IndexMap.put(firstCol, index);
                index2PosMap.put(index, new Integer(maxMatchedPos));
                rangeColumnList.remove(firstCol);
                iter.remove();
            }
        }
        
        return index2PosMap;
    }
    
    public static class IndexLengthComparator implements Comparator<FemLocalIndex>
    {
        IndexLengthComparator()
        {
        }
        
        public int compare(FemLocalIndex index1, FemLocalIndex index2)
        {
            int compRes =
                (((FemLocalIndex)index1).getIndexedFeature().size() -
                    ((FemLocalIndex)index2).getIndexedFeature().size());
            
            if (compRes == 0) {
                compRes = index1.getStorageId().compareTo(index2.getStorageId());
            }
            
            return compRes;
        }
        
        public boolean equals(Object obj)
        {
            return (obj instanceof IndexLengthComparator);
        }
    }
    
    public CwmColumn getIndexColumn(
        FemLocalIndex index,
        int position)
    {
        List indexedFeatures = index.getIndexedFeature();
        
        if (position < 0 || position >= indexedFeatures.size()) {
            return null;
        }
        
        CwmIndexedFeature indexedFeature =
            (CwmIndexedFeature) indexedFeatures.get(position);
        
        return ((CwmColumn) indexedFeature.getFeature());
    }
    
    public boolean testIndexColumn(
        FemLocalIndex index,
        int position,
        CwmColumn column)
    {
        if (!column.equals(getIndexColumn(index, position))) {
            return false;
        }
        return true;
    }
    
    /**
     * Determines the best index to be used to process a semijoin
     * 
     * @param semiJoinKeys keys of the semijoin that we are trying to find an
     * index for.
     * @param bestKeyList appends the list of specific keys within the input
     * paramter that match an index if an appropriate index is available
     * @return best index, if available; else null is returned
     */
    public FemLocalIndex findSemiJoinIndex(
        List<Integer> semiJoinKeys, List<Integer> bestKeyList)
    {
        // loop through the indexes and either find the one that has the 
        // longest matching keys, or the first one that matches all the
        // semijoin keys
        Iterator iter = getUnclusteredIndexes().iterator();
        Integer[] bestKeyOrder = {};
        FemLocalIndex bestIndex = null;
        int maxNkeys = 0;
        while (iter.hasNext()) {
            FemLocalIndex index = (FemLocalIndex) iter.next();
            Integer[] keyOrder = new Integer[semiJoinKeys.size()];
            int nKeys = matchIndexKeys(index, semiJoinKeys, keyOrder);
            if (nKeys > maxNkeys) {
                maxNkeys = nKeys;
                bestKeyOrder = keyOrder;
                bestIndex = index;
                if (maxNkeys == semiJoinKeys.size()) {
                    break;
                }
            }
        }
        for (int i = 0; i < maxNkeys; i++) {
            bestKeyList.add(bestKeyOrder[i]);
        }
        
        return bestIndex;
    }
    
    /**
     * Determines if an index matches a set of keys representing RexInputRefs
     *
     * @param index index being matched against
     * @param keys keys representing the inputRefs that need to be matched
     * against the index
     * @param keyOrder returns the positions of the matching RexInputRefs
     * in the order in which they match the index
     * @return number of matching keys
     */
    private int matchIndexKeys(
        FemLocalIndex index, 
        List<Integer> keys,
        Integer[] keyOrder)
    {
        int nMatches = 0;
        
        for (int i = 0; i < index.getIndexedFeature().size(); i++) {
            keyOrder[i] = -1;
            FemAbstractColumn idxCol = 
                (FemAbstractColumn) getIndexColumn(index, i);
            for (int j = 0; j < keys.size(); j++) {
                FemAbstractColumn keyCol = 
                    (FemAbstractColumn) table.getFeature().get(keys.get(j));
                if (idxCol == keyCol) {
                    keyOrder[i] = j;
                    nMatches++;
                    break;
                }
            }
            // if no match was found for the index key or we've matched 
            // every RexInputRef, stop searching
            if (keyOrder[i] == -1 || nMatches == keys.size()) {
                break;
            }
        }
        return nMatches;
    }

    
    public List<FemLocalIndex> getUnclusteredIndexes()
    {
        return unclusteredIndexes;
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
    public FemLocalIndex pickBestIndex(List<FemLocalIndex> indexList)
    {
        FemLocalIndex bestIndex = indexList.get(0);
        Long minPageCount = bestIndex.getPageCount();
        String bestName = bestIndex.getName();
        for (int i = 1; i < indexList.size(); i++) {
            FemLocalIndex index = indexList.get(i);
            Long count = index.getPageCount();
            String name = index.getName();
            boolean found = false;
            if (minPageCount == null && count == null) {
                if (name.compareTo(bestName) < 0) {
                    found = true;                
                }
            } else if (minPageCount == null && count != null) {
                found = true;
            } else if (minPageCount != null && count != null) {
                int countCompare = count.compareTo(minPageCount);
                if (countCompare < 0) {
                    found = true;
                } else if (countCompare == 0 && name.compareTo(bestName) < 0) {
                    found = true;
                }
            }
            if (found) {
                bestIndex = index;
                minPageCount = count;
                bestName = name;
            }
        }
        return bestIndex;
    }
}

//End LcsIndexGuide.java
