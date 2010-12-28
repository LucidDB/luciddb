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

import java.math.*;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.keysindexes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.fennel.rel.*;
import net.sf.farrago.fennel.tuple.*;
import net.sf.farrago.query.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.type.*;

import org.eigenbase.jmi.*;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sarg.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * LcsIndexGuide provides information about the mapping from catalog definitions
 * for LCS tables and their clusters to their Fennel representation.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class LcsIndexGuide
{
    //~ Static fields/initializers ---------------------------------------------

    public static final int LbmBitmapSegMaxSize = 512;

    //~ Instance fields --------------------------------------------------------

    private FarragoTypeFactory typeFactory;

    private FarragoRepos repos;

    private CwmColumnSet table;

    private RelDataType unflattenedRowType;

    private RelDataType flattenedRowType;

    private int [] flatteningMap;

    private List<FemLocalIndex> clusteredIndexes;

    private List<Integer> clusterMap;

    private int numFlattenedCols;

    private int numUnFlattenedCols;

    private Map<FemLocalIndex, Integer> clusterToRootPageIdParamIdMap;

    private Map<Integer, FemLocalIndex> ordinalToClusterMap;

    //~ Constructors -----------------------------------------------------------

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

        unflattenedRowType = typeFactory.createStructTypeFromClassifier(table);
        numUnFlattenedCols = unflattenedRowType.getFieldList().size();
        flatteningMap = new int[numUnFlattenedCols];

        flattenedRowType =
            SqlTypeUtil.flattenRecordType(
                typeFactory,
                unflattenedRowType,
                flatteningMap);
        numFlattenedCols = flattenedRowType.getFieldList().size();

        this.clusteredIndexes = clusteredIndexes;

        clusterToRootPageIdParamIdMap = new HashMap<FemLocalIndex, Integer>();

        createClusterMap(clusteredIndexes);

        // Create a mapping from flattened ordinal to its corresponding cluster
        ordinalToClusterMap = new HashMap<Integer, FemLocalIndex>();
        for (FemLocalIndex cluster : clusteredIndexes) {
            for (CwmIndexedFeature indexedFeature
                : cluster.getIndexedFeature())
            {
                FemAbstractColumn column =
                    (FemAbstractColumn) indexedFeature.getFeature();
                int start = flattenOrdinal(column.getOrdinal());
                int end = start + getNumFlattenedSubCols(column.getOrdinal());
                for (int i = start; i < end; i++) {
                    ordinalToClusterMap.put(i, cluster);
                }
            }
        }
    }

    /**
     * Construct an IndexGuide using the default list of indexes
     *
     * @param typeFactory
     * @param table the column store table
     */
    LcsIndexGuide(
        FarragoTypeFactory typeFactory,
        CwmColumnSet table)
    {
        this(
            typeFactory,
            table,
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
            getIndexCoverageSet(
                typeFactory.getRepos(),
                unclusteredIndex,
                FarragoCatalogUtil.getClusteredIndexes(
                    typeFactory.getRepos(),
                    table),
                true,
                true));
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Determines the list of indexes from a candidate index list those indexes
     * that contain at least one column matching the columns of a specified
     * index.
     *
     * @param repos repository
     * @param index the specified index
     * @param candidateIndexes candidate indexes
     * @param singleMatch if true, find only the first candidate index that
     * covers each column in the specified index; otherwise, find all candidate
     * indexes that cover each column
     * @param requireNonEmpty if true, the return list must be non-empty
     *
     * @return the list of indexes from the candidate list covering the
     * specified index
     */
    public static List<FemLocalIndex> getIndexCoverageSet(
        FarragoRepos repos,
        FemLocalIndex index,
        List<FemLocalIndex> candidateIndexes,
        boolean singleMatch,
        boolean requireNonEmpty)
    {
        //
        // Get the columns of the specified index
        //
        Set<CwmColumn> requiredColumns = new HashSet<CwmColumn>();
        for (CwmIndexedFeature indexedFeature : index.getIndexedFeature()) {
            requiredColumns.add((CwmColumn) indexedFeature.getFeature());
        }

        //
        // Find the candidate indexes which cover the columns of the index
        //
        List<FemLocalIndex> coverageIndexes = new ArrayList<FemLocalIndex>();
        for (FemLocalIndex candidateIndex : candidateIndexes) {
            boolean include = false;
            for (
                CwmIndexedFeature indexedFeature
                : candidateIndex.getIndexedFeature())
            {
                CwmColumn column = (CwmColumn) indexedFeature.getFeature();
                if (requiredColumns.contains(column)) {
                    include = true;
                    if (singleMatch) {
                        requiredColumns.remove(column);
                    }
                }
            }
            if (include) {
                coverageIndexes.add(candidateIndex);
            }
        }

        if (requireNonEmpty && !requiredColumns.isEmpty()) {
            throw Util.newInternal("index could not be covered");
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
     * the array matching the order of the cluster columns in an ordered list of
     * clusters. E.g.,
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
     */
    private void createClusterMap(List<FemLocalIndex> clusteredIndexes)
    {
        clusterMap = new ArrayList<Integer>();

        for (FemLocalIndex index : clusteredIndexes) {
            for (CwmIndexedFeature indexedFeature : index.getIndexedFeature()) {
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

        for (int i = colOrdinal; i < (colOrdinal + nColsToAdd); i++) {
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

        if (colOrdinal == (numUnFlattenedCols - 1)) {
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

        assert (index.isClustered());
        for (CwmIndexedFeature indexedFeature : index.getIndexedFeature()) {
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
            for (CwmIndexedFeature indexedFeature : index.getIndexedFeature()) {
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
        List<RelDataTypeField> flattenedColList =
            flattenedRowType.getFieldList();

        for (CwmIndexedFeature indexedFeature : index.getIndexedFeature()) {
            FemAbstractColumn column =
                (FemAbstractColumn) indexedFeature.getFeature();
            int numSubCols = getNumFlattenedSubCols(column.getOrdinal());
            int colOrd = flattenOrdinal(column.getOrdinal());

            // add an entry for each subcolumn within a complex type
            for (int i = colOrd; i < (colOrd + numSubCols); i++) {
                RelDataTypeField field = flattenedColList.get(i);
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
            assert (j != -1);
            return new Integer(j);
        }
    }

    /**
     * Determines if an index is referenced by projection list
     *
     * @param index clustered index being checked
     * @param projection array of flattened ordinals of projected columns
     *
     * @return true if at least one column in the clustered index is referenced
     * in the projection list
     */
    public boolean testIndexCoverage(
        FemLocalIndex index,
        Integer [] projection)
    {
        for (CwmIndexedFeature indexedFeature : index.getIndexedFeature()) {
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

    private int flattenOrdinal(int columnOrdinal)
    {
        int i = flatteningMap[columnOrdinal];
        assert (i != -1);
        return i;
    }

    /**
     * Returns the unflattened column ordinal corresponding to a flattened field
     * ordinal
     *
     * @param fieldOrdinal flattened ordinal
     *
     * @return unflattened ordinal
     */
    public int unFlattenOrdinal(int fieldOrdinal)
    {
        int columnOrdinal = Arrays.binarySearch(flatteningMap, fieldOrdinal);
        if (columnOrdinal >= 0) {
            return columnOrdinal;
        } else {
            // When an inexact match is found, binarySearch returns
            // (-(insertion point) - 1).  We want (insertion point) - 1.
            return -(columnOrdinal + 1) - 1;
        }
    }

    /**
     * Creates a tuple descriptor for the BTree index corresponding to a
     * clustered index. For LCS clustered indexes, the stored tuple is always
     * the same: [RID, PageId]; and the key is just the RID. In Fennel, both
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
     * @return RID attribute projection
     *
     * @see #createClusteredBTreeTupleDesc
     */
    public FemTupleProjection createClusteredBTreeRidDesc()
    {
        return FennelRelUtil.createTupleProjection(
            repos,
            new Integer[] { 0 });
    }

    /**
     * Creates a tuple projection for the PageId attribute of the BTree index
     * tuple corresponding to a clustered index.
     *
     * @return PageId attribute projection
     *
     * @see #createClusteredBTreeTupleDesc
     */
    public FemTupleProjection createClusteredBTreePageIdDesc()
    {
        return FennelRelUtil.createTupleProjection(
            repos,
            new Integer[] { 1 });
    }

    /**
     * Creates a tuple descriptor for the BTree index corresponding to an
     * unclustered index.
     *
     * <p>For LCS unclustered indexes, the stored tuple is [K1, K2, ..., RID,
     * BITMAP, BITMAP], and the key is [K1, K2, ..., RID]
     *
     * @param index unclustered index
     *
     * @return btree tuple descriptor
     */
    public FemTupleDescriptor createUnclusteredBTreeTupleDesc(
        FemLocalIndex index)
    {
        assert (!index.isClustered());

        FemTupleDescriptor tupleDesc = repos.newFemTupleDescriptor();

        // add K1, K2, ...
        for (CwmIndexedFeature indexedFeature : index.getIndexedFeature()) {
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
     * @return key attribute projection
     *
     * @see #createUnclusteredBTreeTupleDesc
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
            repos,
            bitmapProj);
    }

    /**
     * Creates a row data type for a projection of an index. The projection may
     * include both key columns and bitmap columns.
     *
     * @param index the index to be projected
     * @param proj a projection on the index
     *
     * @return row data type based on projections of an index
     *
     * @see #createUnclusteredBTreeTupleDesc
     */
    public RelDataType createUnclusteredRowType(
        FemLocalIndex index,
        Integer [] proj)
    {
        List<CwmIndexedFeature> indexFeatures = index.getIndexedFeature();
        int nKeys = indexFeatures.size();
        RelDataType bitmapRowType = createUnclusteredBitmapRowType();

        if (proj == null) {
            return bitmapRowType;
        }

        RelDataType [] types = new RelDataType[proj.length];
        String [] names = new String[proj.length];
        for (int i = 0; i < proj.length; i++) {
            int j = proj[i];
            if (j < nKeys) {
                FemAbstractColumn column =
                    (FemAbstractColumn) indexFeatures.get(j).getFeature();
                names[i] = column.getName();
                types[i] = typeFactory.createCwmElementType(column);
            } else {
                names[i] = bitmapRowType.getFields()[j - nKeys].getName();
                types[i] = bitmapRowType.getFields()[j - nKeys].getType();
            }
        }
        return typeFactory.createStructType(types, names);
    }

    /**
     * Creates the row data type of error records produced by a splicer when
     * there unique constraint violations. The row type generated contains the
     * same column types as the btree key [K1, K2, ..., RID]
     *
     * @param uniqueIndex the unique index updated by the splicer
     *
     * @return row data type for splicer error records
     */
    public RelDataType createSplicerErrorType(FemLocalIndex uniqueIndex)
    {
        // get projections for [keys ..., rid]
        int nKeys = uniqueIndex.getIndexedFeature().size();
        Integer [] keyProj = FennelRelUtil.newIotaProjection(nKeys + 1);
        return createUnclusteredRowType(uniqueIndex, keyProj);
    }

    FemSplitterStreamDef newSplitter(RelDataType outputRowType)
    {
        FemSplitterStreamDef splitter = repos.newFemSplitterStreamDef();

        splitter.setOutputDesc(
            FennelRelUtil.createTupleDescriptorFromRowType(
                repos,
                typeFactory,
                outputRowType));
        return splitter;
    }

    FemBarrierStreamDef newBarrier(
        RelDataType outputRowType,
        BarrierReturnMode returnMode,
        int deleteRowCountParamId)
    {
        FemBarrierStreamDef barrier = repos.newFemBarrierStreamDef();

        // returnMode indicates which input stream or streams contain the
        // data that the barrier should produce
        barrier.setReturnMode(returnMode);

        // if this is the final barrier in a MERGE statement, setup the
        // barrier to receive the upstream deletion rowcount
        if (deleteRowCountParamId > 0) {
            FemDynamicParameter dynParam = repos.newFemDynamicParameter();
            dynParam.setParameterId(deleteRowCountParamId);
            barrier.getDynamicParameter().add(dynParam);
        }

        barrier.setOutputDesc(
            FennelRelUtil.createTupleDescriptorFromRowType(
                repos,
                typeFactory,
                outputRowType));

        return barrier;
    }

    /**
     * Creates a cluster append stream for a specific clustered index.
     *
     * @param rel the RelNode that the cluster belongs to
     * @param clusterIndex the clustered index
     * @param hasIndexes if true, indexes will also be inserted into as part of
     * execution of this cluster append
     * @param rootPageIdParamId the dynamic parameter id of the root page of the
     * cluster; only set > 0 if this is a cluster replace
     * @param clusterPos only used if this is a cluster replace, in which case,
     * it corresponds to the position of this cluster from the list of clusters
     * that will be replaced
     * @param alterTable whether we are doing ALTER TABLE ADD COLUMN
     *
     * @return the constructed cluster append stream
     */
    FemLcsClusterAppendStreamDef newClusterAppend(
        FennelRel rel,
        FemLocalIndex clusterIndex,
        boolean hasIndexes,
        int rootPageIdParamId,
        int clusterPos,
        boolean alterTable)
    {
        FemLcsClusterAppendStreamDef clusterAppend;
        if (rootPageIdParamId > 0) {
            clusterAppend = repos.newFemLcsClusterReplaceStreamDef();
        } else {
            clusterAppend = repos.newFemLcsClusterAppendStreamDef();
        }

        defineIndexStream(
            clusterAppend,
            rel,
            clusterIndex,
            true,
            true,
            rootPageIdParamId);

        //
        // Set up FemLcsClusterAppendStreamDef
        //        - setClusterColProj
        //

        Integer [] clusterColProj;
        if (rootPageIdParamId > 0) {
            // The rid column always appears first in the input tuple and
            // is projected as the first column, followed by the only column
            // in the cluster.
            clusterColProj = new Integer[2];
            clusterColProj[0] = 0;
            clusterColProj[1] = clusterPos;

            // Keep track of the dynamic parameter for later use
            clusterToRootPageIdParamIdMap.put(clusterIndex, rootPageIdParamId);
        } else {
            clusterColProj =
                new Integer[getNumFlattenedClusterCols(clusterIndex)];

            //
            // Figure out the projection covering columns contained in each
            // index.
            //
            int i = 0;
            for (
                CwmIndexedFeature indexedFeature
                : clusterIndex.getIndexedFeature())
            {
                FemAbstractColumn column =
                    (FemAbstractColumn) indexedFeature.getFeature();
                int n = getNumFlattenedSubCols(column.getOrdinal());
                for (int j = 0; j < n; ++j) {
                    if (alterTable) {
                        // If we're doing ALTER TABLE ADD COLUMN, the
                        // new column is the only thing in the input;
                        // if it's a UDT, its fields should match the
                        // order of the input row.
                        clusterColProj[i] = i;
                    } else {
                        clusterColProj[i] =
                            flattenOrdinal(column.getOrdinal()) + j;
                    }
                    i++;
                }
            }
        }

        clusterAppend.setClusterColProj(
            FennelRelUtil.createTupleProjection(repos, clusterColProj));
        if (hasIndexes) {
            clusterAppend.setOutputDesc(getUnclusteredInputDesc());
        }

        return clusterAppend;
    }

    FemLcsRowScanStreamDef newRowScan(
        LcsRowScanRelBase rel,
        Integer [] projectedColumns,
        Integer [] residualColumns)
    {
        FemLcsRowScanStreamDef scanStream = repos.newFemLcsRowScanStreamDef();

        defineScanStream(scanStream, rel, false);

        // setup the output projection relative to the ordered list of
        // clustered indexes
        Integer [] clusterProjection =
            computeProjectedColumns(projectedColumns);
        scanStream.setOutputProj(
            FennelRelUtil.createTupleProjection(repos, clusterProjection));
        scanStream.setFullScan(rel.isFullScan);
        scanStream.setHasExtraFilter(rel.hasResidualFilters());
        scanStream.setSamplingMode(TableSamplingModeEnum.SAMPLING_OFF);
        Integer [] clusterResidualColumns =
            computeProjectedColumns(residualColumns);

        scanStream.setResidualFilterColumns(
            FennelRelUtil.createTupleProjection(repos, clusterResidualColumns));
        return scanStream;
    }

    /**
     * Creates a set of streams for updating a bitmap index
     */
    LcsCompositeStreamDef newBitmapAppend(
        FennelRel rel,
        FemLocalIndex index,
        FemLocalIndex deletionIndex,
        FennelRelImplementor implementor,
        boolean createIndex,
        FennelRelParamId insertDynParamId,
        boolean createNewIndex)
    {
        // create the streams
        FemExecutionStreamDef generator =
            newGenerator(
                rel,
                index,
                createIndex,
                implementor.translateParamId(insertDynParamId).intValue());

        // do an early close in the sorter, in case there was an upstream
        // insert into the deletion index, which the splicer may need to read
        FemExecutionStreamDef sorter = newSorter(index, null, false, true);
        FemExecutionStreamDef splicer =
            newSplicer(
                rel,
                index,
                deletionIndex,
                implementor.translateParamId(insertDynParamId).intValue(),
                0,
                createNewIndex);

        // link them up
        implementor.addDataFlowFromProducerToConsumer(generator, sorter);
        implementor.addDataFlowFromProducerToConsumer(sorter, splicer);

        return new LcsCompositeStreamDef(generator, splicer);
    }

    private FemLbmGeneratorStreamDef newGenerator(
        FennelRel rel,
        FemLocalIndex index,
        boolean createIndex,
        int insertRowCountParamId)
    {
        FemLbmGeneratorStreamDef generator =
            repos.newFemLbmGeneratorStreamDef();

        //
        // Setup cluster scans. The generator scans are based on the new
        // clusters being written.
        //
        defineScanStream(generator, rel, true);

        //
        // Setup projection used by unclustered index
        //
        List<CwmIndexedFeature> indexFeatures = index.getIndexedFeature();
        Integer [] indexProjection = new Integer[indexFeatures.size()];
        int i = 0;
        for (CwmIndexedFeature feature : indexFeatures) {
            FemAbstractColumn column = (FemAbstractColumn) feature.getFeature();
            indexProjection[i++] = column.getOrdinal();
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
        generator.setInsertRowCountParamId(insertRowCountParamId);

        //
        // Setup Btree accessor parameters
        //
        defineIndexAccessor(generator, rel, index, false, true, false, 0);

        //
        // Set up FemExecutionStreamDef
        //        - setOutputDesc (same as tupleDesc)
        //
        generator.setOutputDesc(
            createUnclusteredBTreeTupleDesc(index));

        //
        // This to keeps the repository validator happy
        //
        Integer [] clusterResidualColumns = new Integer[] {};
        generator.setResidualFilterColumns(
            FennelRelUtil.createTupleProjection(repos, clusterResidualColumns));

        return generator;
    }

    /**
     * Creates a sort streamDef for sorting bitmap entries.
     *
     * @param index index corresponding to the bitmap entries that will be
     * sorted
     * @param estimatedNumRows estimated number of input rows into the sort
     * @param ridOnly true if this sort will only be used to sort single rid
     * values
     * @param earlyClose if true, setup the sorter to do an early close on its
     * producers; in that case, the sorter will explicitly close its producers
     * once it has read all its input; this is needed in the case where the
     * producers of the sort reference a table that's modified by the consumers
     * of the sort
     *
     * @return the created sort streamDef
     */
    FemSortingStreamDef newSorter(
        FemLocalIndex index,
        Double estimatedNumRows,
        boolean ridOnly,
        boolean earlyClose)
    {
        FemSortingStreamDef sortingStream = repos.newFemSortingStreamDef();

        //
        // Bitmap entry keys should be unique, but we save the effort
        // of enforcing uniqueness
        //
        sortingStream.setDistinctness(DistinctnessEnum.DUP_ALLOW);
        sortingStream.setKeyProj(createUnclusteredBTreeKeyProj(index));
        if (!ridOnly) {
            sortingStream.setOutputDesc(createUnclusteredBTreeTupleDesc(index));
        }

        // estimated number of rows in the sort input; if unknown, set to -1
        if (estimatedNumRows == null) {
            sortingStream.setEstimatedNumRows(-1);
        } else {
            sortingStream.setEstimatedNumRows(estimatedNumRows.longValue());
        }
        sortingStream.setEarlyClose(earlyClose);

        return sortingStream;
    }

    FemLbmSplicerStreamDef newSplicer(
        FennelRel rel,
        FemLocalIndex index,
        FemLocalIndex deletionIndex,
        int insertRowCountParamId,
        int writeRowCountParamId,
        boolean createNewIndex)
    {
        FemLbmSplicerStreamDef splicer = repos.newFemLbmSplicerStreamDef();

        // Setup the index that splicer will be writing to
        FemSplicerIndexAccessorDef indexAccessor =
            repos.newFemSplicerIndexAccessorDef();
        defineIndexAccessor(
            indexAccessor,
            rel,
            index,
            false,
            true,
            false,
            0);
        splicer.getIndexAccessor().add(indexAccessor);

        // Setup the deletion index if the splicer will be reading from it.
        // This deletion index scan needs to read data inserted upstream, so
        // it needs to be able to see uncommitted data.
        if (deletionIndex != null) {
            indexAccessor = repos.newFemSplicerIndexAccessorDef();
            defineIndexAccessor(
                indexAccessor,
                rel,
                deletionIndex,
                false,
                false,
                false,
                0);
            splicer.getIndexAccessor().add(indexAccessor);
        }

        //
        // The splicer is the terminal stream of a bitmap stream set.
        // It's output type is the same as the rel's: the standard
        // Dml output type.
        //
        splicer.setInsertRowCountParamId(insertRowCountParamId);
        splicer.setWriteRowCountParamId(writeRowCountParamId);
        splicer.setCreateNewIndex(createNewIndex);

        // NOTE zfong 11/30/06 - Splicer may also write out rid values.
        // As it turns out, the type of a rid is currently the same as a
        // rowcount.
        RelDataType rowType =
            typeFactory.createStructType(
                new RelDataType[] {
                    typeFactory.createSqlType(SqlTypeName.BIGINT)
                },
                new String[] { "ROWCOUNT" });
        splicer.setOutputDesc(
            FennelRelUtil.createTupleDescriptorFromRowType(
                repos,
                typeFactory,
                rowType));

        return splicer;
    }

    FemIndexScanDef newIndexScan(
        FennelRel rel,
        FemLocalIndex index,
        Integer [] projectedColumns)
    {
        FemIndexScanDef scanStream = repos.newFemIndexScanDef();
        defineIndexScan(scanStream, rel, index, projectedColumns);

        return scanStream;
    }

    FemLbmSearchStreamDef newIndexSearch(
        FennelRel rel,
        FemLocalIndex index,
        boolean isUniqueKey,
        boolean isOuter,
        Integer [] inputKeyProj,
        Integer [] inputJoinProj,
        Integer [] inputDirectiveProj,
        Integer [] outputProj,
        FennelDynamicParamId startRidParamId,
        FennelDynamicParamId rowLimitParamId)
    {
        FemLbmSearchStreamDef searchStream = repos.newFemLbmSearchStreamDef();
        defineIndexScan(searchStream, rel, index, outputProj);

        searchStream.setStartRidParamId(startRidParamId.intValue());
        searchStream.setRowLimitParamId(rowLimitParamId.intValue());

        searchStream.setUniqueKey(isUniqueKey);
        searchStream.setOuterJoin(isOuter);
        searchStream.setPrefetch(false);

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
        FemLocalIndex index,
        Integer [] outputProj)
    {
        defineIndexStream(scanStream, rel, index, false, false, 0);

        // set FemIndexScanDef
        if (outputProj == null) {
            // If projected fields are not specified, output projection
            // maps to [RID, bitmapfield1, bitmapfield2].
            scanStream.setOutputProj(
                createUnclusteredBTreeBitmapProj(index));
        } else {
            scanStream.setOutputProj(
                FennelRelUtil.createTupleProjection(
                    repos,
                    outputProj));
        }
    }

    private void defineIndexStream(
        FemIndexStreamDef indexStream,
        FennelRel rel,
        FemLocalIndex index,
        boolean clustered,
        boolean write,
        int rootPageIdParamId)
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

        defineIndexAccessor(
            indexStream,
            rel,
            index,
            clustered,
            write,
            !write,
            rootPageIdParamId);
    }

    private void defineIndexAccessor(
        FemIndexAccessorDef indexAccessor,
        FennelRel rel,
        FemLocalIndex index,
        boolean clustered,
        boolean write,
        boolean readOnlyCommittedData,
        int rootPageIdParamId)
    {
        final FarragoPreparingStmt stmt = FennelRelUtil.getPreparingStmt(rel);

        //
        // Set up FemIndexAccessorDef
        //        - setRootPageId
        //        - setSegmentId
        //        - setTupleDesc
        //        - setKeyProj
        //
        indexAccessor.setRootPageId(
            stmt.getIndexMap().getIndexRoot(index, write));
        indexAccessor.setRootPageIdParamId(rootPageIdParamId);

        indexAccessor.setSegmentId(
            LcsDataServer.getIndexSegmentId(index));

        long indexId = JmiObjUtil.getObjectId(index);

        if (!write) {
            failIfIndexInvalid(index);
        }

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
            //The key is simply the RID from the [RID, PageId] mapping stored in
            //this index.
            Integer [] keyProj = { 0 };
            femProj = FennelRelUtil.createTupleProjection(repos, keyProj);
        } else {
            femProj = createUnclusteredBTreeKeyProj(index);
        }
        indexAccessor.setKeyProj(femProj);
        indexAccessor.setReadOnlyCommittedData(readOnlyCommittedData);
    }

    /**
     * Fills in a stream definition for this scan.
     *
     * @param scanStream stream definition to fill in
     * @param rel the RelNode containing the cluster
     * @param write whether the cluster will be written
     */
    private void defineScanStream(
        FemLcsRowScanStreamDef scanStream,
        FennelRel rel,
        boolean write)
    {
        // setup each cluster scan def
        for (FemLocalIndex index : clusteredIndexes) {
            FemLcsClusterScanDef clusterScan = repos.newFemLcsClusterScanDef();
            Integer rootPageIdParamId =
                clusterToRootPageIdParamIdMap.get(index);
            defineClusterScan(
                index,
                rel,
                clusterScan,
                write,
                (rootPageIdParamId == null) ? 0 : rootPageIdParamId.intValue());
            scanStream.getClusterScan().add(clusterScan);
        }
    }

    private void failIfIndexInvalid(FemLocalIndex index)
    {
        // REVIEW jvs 10-Dec-2008:  The validator should prevent
        // us from ever actually hitting this, so perhaps it
        // should change to an assertion instead.
        if (index.isInvalid()) {
            throw FarragoResource.instance().QueryAccessNewColumn.ex(
                repos.getLocalizedObjectName(index));
        }
    }

    /**
     * Fills in a cluster scan def for this scan
     *
     * @param index clustered index corresponding to this can
     * @param rel the RelNode containing the cluster
     * @param clusterScan clustered scan to fill in
     * @param write true if the cluster will be written as well as read
     * @param rootPageIdParamId if > 0, the dynamic parameter that will supply
     * the value of the root pageId of the cluster
     */
    private void defineClusterScan(
        FemLocalIndex index,
        FennelRel rel,
        FemLcsClusterScanDef clusterScan,
        boolean write,
        int rootPageIdParamId)
    {
        final FarragoPreparingStmt stmt = FennelRelUtil.getPreparingStmt(rel);

        // setup cluster tuple descriptor and add to cluster map
        FemTupleDescriptor clusterDesc = getClusterTupleDesc(index);
        clusterScan.setClusterTupleDesc(clusterDesc);

        // setup index accessor def fields

        if (!write) {
            failIfIndexInvalid(index);
        }

        if (!FarragoCatalogUtil.isIndexTemporary(index)) {
            clusterScan.setRootPageId(
                stmt.getIndexMap().getIndexRoot(index, write));

            // If we're writing to the cluster, then we want to be able
            // to read that data.
            clusterScan.setReadOnlyCommittedData(!write);
        } else {
            // For a temporary index, each execution needs to bind to
            // a session-private root.  So don't burn anything into
            // the plan.
            clusterScan.setRootPageId(-1);
            clusterScan.setReadOnlyCommittedData(false);
        }
        clusterScan.setRootPageIdParamId(rootPageIdParamId);

        clusterScan.setSegmentId(LcsDataServer.getIndexSegmentId(index));
        clusterScan.setIndexId(JmiObjUtil.getObjectId(index));

        clusterScan.setTupleDesc(
            createClusteredBTreeTupleDesc());

        clusterScan.setKeyProj(
            createClusteredBTreeRidDesc());
    }

    /**
     * Creates input values for a bitmap index generator
     *
     * <p>The inputs are two columns: [rowCount startRid]
     */
    RexNode [] getUnclusteredInputs(RexBuilder builder)
    {
        // First obtain the row count and starting row id
        //
        // TODO: Make this work for the incremental case
        BigDecimal rowCount = BigDecimal.ZERO;
        BigDecimal startRid = BigDecimal.ZERO;

        // TODO: These types must match the fennel types for
        // RecordNum and LcsRowId
        RelDataType rowCountType =
            typeFactory.createSqlType(SqlTypeName.BIGINT);
        RelDataType ridType = rowCountType;

        return new RexNode[] {
                builder.makeExactLiteral(rowCount, rowCountType),
                builder.makeExactLiteral(startRid, ridType)
            };
    }

    RelDataType getUnclusteredInputType()
    {
        // TODO: These types must match the fennel types for
        // RecordNum and LcsRowId
        RelDataType rowCountType =
            typeFactory.createSqlType(SqlTypeName.BIGINT);
        RelDataType ridType = rowCountType;

        return typeFactory.createStructType(
            new RelDataType[] { rowCountType, ridType },
            new String[] { "ROWCOUNT", "SRID" });
    }

    FemTupleDescriptor getUnclusteredInputDesc()
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
        RelDataType ridType = typeFactory.createSqlType(SqlTypeName.BIGINT);
        RelDataType bitmapType =
            typeFactory.createSqlType(
                SqlTypeName.VARBINARY,
                LbmBitmapSegMaxSize);

        RelDataType segDescType =
            typeFactory.createTypeWithNullability(bitmapType, true);

        RelDataType segType =
            typeFactory.createTypeWithNullability(bitmapType, true);

        return typeFactory.createStructType(
            new RelDataType[] { ridType, segDescType, segType },
            new String[] { "SRID", "SegmentDesc", "Segment" });
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
     * Creates a key projection for bitmap data. The only key column is the
     * first column, SRID
     */
    public FemTupleProjection createUnclusteredBitmapKeyProj()
    {
        return FennelRelUtil.createTupleProjection(
            repos,
            FennelRelUtil.newIotaProjection(1));
    }

    FemSortingStreamDef newBitmapSorter()
    {
        FemSortingStreamDef sortingStream = repos.newFemSortingStreamDef();
        sortingStream.setDistinctness(DistinctnessEnum.DUP_ALLOW);
        sortingStream.setKeyProj(createUnclusteredBitmapKeyProj());
        sortingStream.setOutputDesc(createUnclusteredBitmapTupleDesc());
        sortingStream.setEarlyClose(false);

        // TODO zfong 8/16/06 - replace this with real stats when we can
        // call RelMetadataQuery.getRowCount on physical RelNodes
        sortingStream.setEstimatedNumRows(-1);
        return sortingStream;
    }

    private void setBitmapStreamParams(
        FemLbmBitOpStreamDef bitOpStream,
        FennelDynamicParamId startRidParamId,
        FennelDynamicParamId rowLimitParamId)
    {
        bitOpStream.setStartRidParamId(startRidParamId.intValue());
        bitOpStream.setRowLimitParamId(rowLimitParamId.intValue());

        bitOpStream.setOutputDesc(createUnclusteredBitmapTupleDesc());
    }

    FemLbmIntersectStreamDef newBitmapIntersect(
        FennelDynamicParamId startRidParamId,
        FennelDynamicParamId rowLimitParamId)
    {
        FemLbmIntersectStreamDef intersectStream =
            repos.newFemLbmIntersectStreamDef();

        setBitmapStreamParams(
            intersectStream,
            startRidParamId,
            rowLimitParamId);

        return intersectStream;
    }

    FemLbmMinusStreamDef newBitmapMinus(
        FennelDynamicParamId startRidParamId,
        FennelDynamicParamId rowLimitParamId,
        RelNode child)
    {
        FemLbmMinusStreamDef minusStream = repos.newFemLbmMinusStreamDef();

        setBitmapStreamParams(
            minusStream,
            startRidParamId,
            rowLimitParamId);

        // override the default output tuple
        minusStream.setOutputDesc(
            FennelRelUtil.createTupleDescriptorFromRowType(
                repos,
                typeFactory,
                child.getRowType()));

        return minusStream;
    }

    LcsIndexMinusRel createMinusOfDeletionIndex(
        FennelRel rel,
        LcsTable lcsTable,
        RelNode input)
    {
        FennelRelImplementor implementor = FennelRelUtil.getRelImplementor(rel);
        FennelRelParamId startRidParamId = implementor.allocateRelParamId();
        FennelRelParamId rowLimitParamId = implementor.allocateRelParamId();
        LcsIndexSearchRel delIndexScan =
            createDeletionIndexScan(
                rel,
                lcsTable,
                startRidParamId,
                rowLimitParamId,
                false);
        RelNode [] minusInputs = new RelNode[2];
        minusInputs[0] = input;
        minusInputs[1] = delIndexScan;
        LcsIndexMinusRel minus =
            new LcsIndexMinusRel(
                rel.getCluster(),
                minusInputs,
                lcsTable,
                startRidParamId,
                rowLimitParamId);
        return minus;
    }

    /**
     * Creates an index search on the deletion index corresponding to an lcs
     * table. A deletion index is searched by RID.
     *
     * @param rel the rel from which this scan is being generated
     * @param lcsTable the table whose deletion index is to be scanned
     * @param startRidParamId start rid parameter for the index search
     * @param rowLimitParamId row limit parameter for the index search
     * @param fullScan true if deletion index will be used with a full table
     * scan
     *
     * @return the created index search
     */
    LcsIndexSearchRel createDeletionIndexScan(
        FennelRel rel,
        LcsTable lcsTable,
        FennelRelParamId startRidParamId,
        FennelRelParamId rowLimitParamId,
        boolean fullScan)
    {
        FemLocalIndex delIndex =
            FarragoCatalogUtil.getDeletionIndex(
                repos,
                lcsTable.getCwmColumnSet());

        RelNode keyInput = createDelIndexScanInput(rel, fullScan);

        Integer [] inputDirectiveProj = { 0, 2 };
        Integer [] inputKeyProj = { 1, 3 };

        LcsIndexSearchRel indexSearch =
            new LcsIndexSearchRel(
                rel.getCluster(),
                keyInput,
                lcsTable,
                delIndex,
                false,
                null,
                true,
                false,
                false,
                inputKeyProj,
                null,
                inputDirectiveProj,
                startRidParamId,
                rowLimitParamId,
                null);

        return indexSearch;
    }

    /**
     * Creates the RelNode that corresponds to the key input into the deletion
     * index scan. The type of input depends on whether or not a full scan is
     * being done on the deletion index.
     *
     * @param rel the original rel from which this rel is being generated
     * @param fullScan true if a full scan will be done on the deletion index
     *
     * @return the created RelNode
     */
    private RelNode createDelIndexScanInput(FennelRel rel, boolean fullScan)
    {
        // Search on the index using the rid as the key
        FarragoPreparingStmt stmt = FennelRelUtil.getPreparingStmt(rel);
        FarragoTypeFactory typeFactory = stmt.getFarragoTypeFactory();
        RelDataType directiveType =
            typeFactory.createSqlType(
                SqlTypeName.CHAR,
                1);
        RelDataType keyType =
            typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.BIGINT),
                true);

        // Setup the directives.  In the case of a full scan, setup an
        // unbounded lower and upper search.  In the index scan case, setup
        // a >= search on the rid key.
        RelDataType keyRowType =
            typeFactory.createStructType(
                new RelDataType[] {
                    directiveType,
                    keyType,
                    directiveType,
                    keyType
                },
                new String[] {
                    "lowerBoundDirective",
                    "lowerBoundKey",
                    "upperBoundDirective",
                    "upperBoundKey"
                });

        List<List<RexLiteral>> inputTuples = new ArrayList<List<RexLiteral>>();
        List<RexLiteral> tuple = new ArrayList<RexLiteral>();
        RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
        SargFactory sargFactory = new SargFactory(rexBuilder);

        SargMutableEndpoint lowerEndpoint = sargFactory.newEndpoint(keyType);
        if (fullScan) {
            lowerEndpoint.setInfinity(-1);
        } else {
            // for now, just set the actual search key value to null; this
            // will get filled in at runtime with the value of the startRid
            // dynamic parameter
            lowerEndpoint.setFinite(
                SargBoundType.LOWER,
                SargStrictness.CLOSED,
                rexBuilder.constantNull());
        }
        RexLiteral lowerBoundDirective =
            FennelRelUtil.convertEndpoint(rexBuilder, lowerEndpoint);

        SargMutableEndpoint upperEndpoint = sargFactory.newEndpoint(keyType);
        upperEndpoint.setInfinity(1);

        RexLiteral upperBoundDirective =
            FennelRelUtil.convertEndpoint(rexBuilder, upperEndpoint);

        tuple.add(lowerBoundDirective);
        tuple.add(rexBuilder.constantNull());
        tuple.add(upperBoundDirective);
        tuple.add(rexBuilder.constantNull());
        inputTuples.add(tuple);

        RelNode keyRel =
            new FennelValuesRel(
                rel.getCluster(),
                keyRowType,
                inputTuples,
                false);

        RelNode keyInput =
            RelOptRule.mergeTraitsAndConvert(
                rel.getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION,
                keyRel);
        assert (keyInput != null);

        return keyInput;
    }

    private FemAbstractColumn getIndexColumn(
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
     * Determines the best index to be used to process a semijoin
     *
     * @param semiJoinKeys keys of the semijoin that we are trying to find an
     * index for.
     * @param bestKeyList appends the list of specific keys within the input
     * parameter that match an index if an appropriate index is available
     *
     * @return best index, if available; else null is returned
     */
    public FemLocalIndex findSemiJoinIndex(
        List<Integer> semiJoinKeys,
        List<Integer> bestKeyList)
    {
        // loop through the indexes and either find the one that has the
        // longest matching keys, or the first one that matches all the
        // semijoin keys
        Integer [] bestKeyOrder = {};
        FemLocalIndex bestIndex = null;
        int maxNkeys = 0;
        for (
            FemLocalIndex index
            : FarragoCatalogUtil.getUnclusteredIndexes(repos, table))
        {
            Integer [] keyOrder = new Integer[semiJoinKeys.size()];
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
     * @param keyOrder returns the positions of the matching RexInputRefs in the
     * order in which they match the index
     *
     * @return number of matching keys
     */
    public int matchIndexKeys(
        FemLocalIndex index,
        List<Integer> keys,
        Integer [] keyOrder)
    {
        int nMatches = 0;

        for (int i = 0; i < index.getIndexedFeature().size(); i++) {
            keyOrder[i] = -1;
            FemAbstractColumn idxCol = getIndexColumn(index, i);
            final List<FemAbstractColumn> columns =
                Util.cast(table.getFeature(), FemAbstractColumn.class);
            for (int j = 0; j < keys.size(); j++) {
                FemAbstractColumn keyCol = columns.get(keys.get(j));
                if (idxCol.equals(keyCol)) {
                    keyOrder[i] = j;
                    nMatches++;
                    break;
                }
            }

            // if no match was found for the index key or we've matched
            // every RexInputRef, stop searching
            if ((keyOrder[i] == -1) || (nMatches == keys.size())) {
                break;
            }
        }
        return nMatches;
    }

    /**
     * Retrieves a list of clusters corresponding to clusters containing
     * residual filtered columns.  The list of clusters is returned in the same
     * order as the residual filtered columns.  Note that the number of
     * clusters may be smaller than the number of residual columns, if a
     * cluster has more than one column.
     *
     * @param residualColumns array of flattened ordinals corresponding to the
     * columns that have residual filters
     *
     * @return the list of residual column clusters
     */
    public List<FemLocalIndex> createResidualClusterList(
        Integer[] residualColumns)
    {
        List<FemLocalIndex> residualClusterList =
            new ArrayList<FemLocalIndex>();

        // Add the clusters corresponding to residual columns
        for (int i = 0; i < residualColumns.length; i++) {
            FemLocalIndex cluster = ordinalToClusterMap.get(residualColumns[i]);
            if (!residualClusterList.contains(cluster)) {
                residualClusterList.add(cluster);
            }
        }

        return residualClusterList;
    }
}

// End LcsIndexGuide.java
