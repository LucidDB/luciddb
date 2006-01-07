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

import java.util.*;

import net.sf.farrago.catalog.FarragoCatalogUtil;
import net.sf.farrago.catalog.FarragoRepos;
import net.sf.farrago.cwm.keysindexes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fennel.tuple.FennelStandardTypeDescriptor;
import net.sf.farrago.fennel.tuple.FennelStoredTypeDescriptor;
import net.sf.farrago.query.FennelRelUtil;
import net.sf.farrago.type.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;

/**
 * LcsIndexGuide provides information about the mapping from catalog
 * definitions for LCS tables and their clusters to their Fennel representation.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class LcsIndexGuide
{
    private FarragoTypeFactory typeFactory;
    
    private FarragoRepos repos;
    
    private RelDataType unflattenedRowType;
    
    private RelDataType flattenedRowType;

    private int [] flatteningMap;
    
    private List<FemLocalIndex> clusteredIndexes;
    
    private List clusterMap;
    
    private int numFlattenedCols;
    
    private int numUnFlattenedCols;

    /**
     * Construct an IndexGuide using a specific list of indexes
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
        clusterMap = new ArrayList();
        
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
        int j = clusterMap.indexOf(i);
        assert(j != -1);
        return new Integer(j);
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
        attrDesc.setByteLength(4096);
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
     * [K1, K2, ..., RID, BITMAP], and the key is [K1, K2, ..., RID]
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
    public FemTupleProjection createUnclusteredBTreeKeyDesc(
        FemLocalIndex index)
    {
        // number of key fields = number of columns plus RID
        int n = index.getIndexedFeature().size() + 1;
        
        return FennelRelUtil.createTupleProjection(
            repos,
            FennelRelUtil.newIotaProjection(n));
    }
}

// End LcsIndexGuide.java
