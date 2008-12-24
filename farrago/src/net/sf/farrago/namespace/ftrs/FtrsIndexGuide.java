/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2004-2007 John V. Sichi
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
package net.sf.farrago.namespace.ftrs;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.keysindexes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.query.*;
import net.sf.farrago.type.*;

import org.eigenbase.jmi.JmiObjUtil;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * FtrsIndexGuide provides information about the mapping from catalog
 * definitions for tables and indexes to their Fennel representation.
 *
 * <p>Examples in the comments refer to the test tables EMPS and DEPTS defined
 * in <code>farrago/initsql/createSalesSchema.sql</code>. For an overview and
 * terminology, please see <a
 * href="http://farrago.sf.net/design/TableIndexing.html">the design docs</a>.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FtrsIndexGuide
{
    //~ Instance fields --------------------------------------------------------

    private FarragoTypeFactory typeFactory;

    private FarragoRepos repos;

    private CwmColumnSet table;

    private RelDataType unflattenedRowType;

    private RelDataType flattenedRowType;

    private int [] flatteningMap;

    //~ Constructors -----------------------------------------------------------

    FtrsIndexGuide(
        FarragoTypeFactory typeFactory,
        CwmColumnSet table)
    {
        this.typeFactory = typeFactory;
        this.table = table;
        repos = typeFactory.getRepos();

        unflattenedRowType = typeFactory.createStructTypeFromClassifier(table);

        int n = unflattenedRowType.getFieldList().size();
        flatteningMap = new int[n];
        flattenedRowType =
            SqlTypeUtil.flattenRecordType(
                typeFactory,
                unflattenedRowType,
                flatteningMap);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return the flattened row type for the indexed table
     */
    public RelDataType getFlattenedRowType()
    {
        return flattenedRowType;
    }

    /**
     * Gets a list of columns covered by an unclustered index.
     *
     * <p>Example: for index EMPS_UX, the result is [ NAME, DEPTNO, EMPNO ]
     *
     * @param index index for which to compute column list
     *
     * @return List (of CwmColumn) making up an unclustered index's tuple
     */
    List<FemAbstractColumn> getUnclusteredCoverageColList(
        FemLocalIndex index)
    {
        FemLocalIndex clusteredIndex =
            FarragoCatalogUtil.getClusteredIndex(
                repos,
                index.getSpannedClass());
        List<FemAbstractColumn> indexColumnList =
            new ArrayList<FemAbstractColumn>();
        appendDefinedKey(indexColumnList, index);
        appendClusteredDistinctKey(clusteredIndex, indexColumnList);
        return indexColumnList;
    }

    /**
     * Same as getUnclusteredCoverageColList, but returns flattened column
     * ordinals instead.
     *
     * <p>Example: for index EMPS_UX, the result is [ 1, 2, 0 ]
     *
     * @param index index for which to compute projection
     *
     * @return projection as array of 0-based flattened column ordinals
     */
    Integer [] getUnclusteredCoverageArray(
        FemLocalIndex index)
    {
        List<FemAbstractColumn> list = getUnclusteredCoverageColList(index);
        Integer [] projection = new Integer[list.size()];
        Iterator<FemAbstractColumn> iter = list.iterator();
        int i = 0;
        for (; iter.hasNext(); ++i) {
            FemAbstractColumn column = iter.next();
            projection[i] = flattenOrdinal(column.getOrdinal());
        }
        return projection;
    }

    /**
     * Gets the distinct key of a clustered index.
     *
     * <p>Example: for the clustered index on table EMPS, the result is [ 2, 0
     * ]. But if the clustered index were defined as non-unique on column city
     * instead, then the result would be [ 4, 2, 0 ]. For a non-unique clustered
     * index on empno, the result would be [ 0, 2 ].
     *
     * @param index the FemLocalIndex for which the key is to be projected
     *
     * @return array of 0-based flattened column ordinals
     */
    Integer [] getClusteredDistinctKeyArray(
        FemLocalIndex index)
    {
        assert (index.isClustered());
        List<FemAbstractColumn> indexColumnList =
            new ArrayList<FemAbstractColumn>();
        appendClusteredDistinctKey(index, indexColumnList);
        Integer [] array = new Integer[indexColumnList.size()];
        for (int i = 0; i < array.length; ++i) {
            FemAbstractColumn column = indexColumnList.get(i);
            array[i] = flattenOrdinal(column.getOrdinal());
        }
        return array;
    }

    List<? extends Object> getDistinctKeyColList(
        FemLocalIndex index)
    {
        List<FemAbstractColumn> indexColumnList =
            new ArrayList<FemAbstractColumn>();
        if (index.isClustered()) {
            appendClusteredDistinctKey(index, indexColumnList);
        } else {
            if (index.isUnique()) {
                appendDefinedKey(indexColumnList, index);
            } else {
                return getUnclusteredCoverageColList(index);
            }
        }
        return indexColumnList;
    }

    /**
     * Gets the collation key of an index.
     *
     * <p>Example: for index DEPTS_UNIQUE_NAME, the result is [ 1, 0 ]
     *
     * @param index index for which to compute projection
     *
     * @return projection as array of 0-based flattened column ordinals
     */
    Integer [] getCollationKeyArray(
        FemLocalIndex index)
    {
        if (index.isClustered()) {
            return getClusteredDistinctKeyArray(index);
        } else {
            return getUnclusteredCoverageArray(index);
        }
    }

    /**
     * Gets a FemTupleProjection which specifies how to extract the distinct key
     * from the result of getCoverageTupleDescriptor. The projected ordinals are
     * relative to the index coverage tuple, not the table.
     *
     * <p>Example: for the clustered index of table EMPS, the result is [ 2, 0
     * ]. For index DEPTS_UNIQUE_NAME, the result is [ 0 ]. For index EMPS_UX,
     * the result is [ 0, 1, 2 ].
     *
     * @param index the FemLocalIndex for which the key is to be projected
     *
     * @return new FemTupleProjection
     */
    FemTupleProjection getDistinctKeyProjection(
        FemLocalIndex index)
    {
        if (index.isClustered()) {
            List<FemAbstractColumn> indexColumnList =
                new ArrayList<FemAbstractColumn>();
            appendClusteredDistinctKey(index, indexColumnList);
            FemTupleProjection tupleProj =
                createTupleProjectionFromColumnList(indexColumnList);
            return tupleProj;
        }

        int n;
        if (index.isUnique()) {
            // For a unique unclustered index, we want just the key fields so
            // that we can detect duplicates.  TODO:  this doesn't work for
            // nullable keys; fix it.
            n = index.getIndexedFeature().size();
        } else {
            // For a non-unique unclustered index, we need to treat the whole
            // index tuple as a key to make it unique (the inclusion of the
            // clustering key guarantees this); that way we can perform
            // deletions and rollbacks without requiring linear search.
            n = getUnclusteredCoverageColList(index).size();
        }
        return FennelRelUtil.createTupleProjection(
            repos,
            FennelRelUtil.newIotaProjection(n));
    }

    /**
     * Creates a FemTupleDescriptor for the coverage tuple of an index.
     *
     * @param index the FemLocalIndex to be described
     *
     * @return new FemTupleDescriptor
     */
    FemTupleDescriptor getCoverageTupleDescriptor(
        FemLocalIndex index)
    {
        if (index.isClustered()) {
            return getClusteredCoverageTupleDescriptor();
        } else {
            return getUnclusteredCoverageTupleDescriptor(index);
        }
    }

    /**
     * Creates a FemTupleProjection which specifies how to extract the index
     * coverage tuple from a full table tuple.
     *
     * <p>Example: for the clustered index of table EMPS, the result is [ 0, 1,
     * 2, 3, 4, 5 ]. For index DEPTS_UNIQUE_NAME, the result is [ 1, 0 ].
     *
     * @param index the FemLocalIndex for which the tuple is to be projected
     *
     * @return new FemTupleProjection
     */
    FemTupleProjection getCoverageProjection(
        FemLocalIndex index)
    {
        if (index.isClustered()) {
            // clustered index tuple is full table tuple
            return FennelRelUtil.createTupleProjection(
                repos,
                FennelRelUtil.newIotaProjection(
                    flattenedRowType.getFieldList().size()));
        }

        List<FemAbstractColumn> indexColumnList =
            getUnclusteredCoverageColList(index);

        FemTupleProjection proj =
            createTupleProjectionFromColumnList(indexColumnList);
        return proj;
    }

    /**
     * Generates a FemTupleProjection from a list of CWM columns.
     *
     * @param indexColumnList list of columns
     *
     * @return generated FemTupleProjection
     */
    FemTupleProjection createTupleProjectionFromColumnList(
        List<FemAbstractColumn> indexColumnList)
    {
        FemTupleProjection tupleProj = repos.newFemTupleProjection();
        for (FemAbstractColumn column : indexColumnList) {
            FemTupleAttrProjection attrProj = repos.newFemTupleAttrProjection();
            tupleProj.getAttrProjection().add(attrProj);
            attrProj.setAttributeIndex(
                flattenOrdinal(
                    column.getOrdinal()));
        }
        return tupleProj;
    }

    /**
     * Creates a new physical writer definition for a particular index,
     * initializing any invariant information.
     *
     * @param rel relational expression modifying the index
     * @param index the index of interest
     *
     * @return new writer
     */
    public FemIndexWriterDef newIndexWriter(
        FennelRel rel,
        FemLocalIndex index)
    {
        FemIndexWriterDef indexWriter =
            repos.getFennelPackage().getFemIndexWriterDef()
            .createFemIndexWriterDef();
        if (!FarragoCatalogUtil.isIndexTemporary(index)) {
            final FarragoPreparingStmt stmt =
                FennelRelUtil.getPreparingStmt(rel);
            indexWriter.setRootPageId(
                stmt.getIndexMap().getIndexRoot(index, true));
        } else {
            indexWriter.setRootPageId(-1);
        }
        indexWriter.setReadOnlyCommittedData(false);
        indexWriter.setRootPageIdParamId(0);
        indexWriter.setSegmentId(FtrsDataServer.getIndexSegmentId(index));
        indexWriter.setIndexId(JmiObjUtil.getObjectId(index));
        indexWriter.setTupleDesc(
            getCoverageTupleDescriptor(index));
        indexWriter.setKeyProj(
            getDistinctKeyProjection(index));
        indexWriter.setDistinctness(
            index.isUnique() ? DistinctnessEnum.DUP_FAIL
            : DistinctnessEnum.DUP_ALLOW);
        return indexWriter;
    }

    private void appendConstraintColumns(
        List<FemAbstractColumn> list,
        FemAbstractUniqueConstraint constraint)
    {
        for (
            FemAbstractColumn column
            : Util.cast(constraint.getFeature(), FemAbstractColumn.class))
        {
            if (list.contains(column)) {
                continue;
            }
            list.add(column);
        }
    }

    private void appendDefinedKey(
        List<FemAbstractColumn> list,
        FemLocalIndex index)
    {
        for (CwmIndexedFeature indexedFeature : index.getIndexedFeature()) {
            FemAbstractColumn column =
                (FemAbstractColumn) indexedFeature.getFeature();
            if (list.contains(column)) {
                continue;
            }
            list.add(column);
        }
    }

    private void appendClusteredDistinctKey(
        FemLocalIndex clusteredIndex,
        List<FemAbstractColumn> indexColumnList)
    {
        assert (clusteredIndex.isClustered());
        appendDefinedKey(indexColumnList, clusteredIndex);
        if (!clusteredIndex.isUnique()) {
            appendConstraintColumns(
                indexColumnList,
                FarragoCatalogUtil.getPrimaryKey(
                    clusteredIndex.getSpannedClass()));
        }
    }

    private FemTupleDescriptor getClusteredCoverageTupleDescriptor()
    {
        FemTupleDescriptor tupleDesc = repos.newFemTupleDescriptor();

        for (RelDataTypeField field : flattenedRowType.getFieldList()) {
            FennelRelUtil.addTupleAttrDescriptor(
                repos,
                tupleDesc,
                field.getType());
        }
        return tupleDesc;
    }

    private FemTupleDescriptor getUnclusteredCoverageTupleDescriptor(
        FemLocalIndex index)
    {
        FemTupleDescriptor tupleDesc = repos.newFemTupleDescriptor();
        List<FemAbstractColumn> colList = getUnclusteredCoverageColList(index);
        for (FemAbstractColumn column : colList) {
            FennelRelUtil.addTupleAttrDescriptor(
                repos,
                tupleDesc,
                typeFactory.createCwmElementType(column));
        }
        return tupleDesc;
    }

    /**
     * Converts from unflattened 0-based logical column ordinal to 0-based
     * flattened tuple ordinal (as known by Fennel). These differ in the
     * presence of user-defined types. For example, consider DDL like
     *
     * <pre><code>
     *
     * create type pencil (
     *     outer_radius_mm double,
     *     lead_radius_mm double,
     *     length_mm double,
     *     has_eraser boolean
     * );
     *
     * create table pencil_case(
     *     id int not null primary key,
     *     p pencil not null,
     *     crayon_count int);
     *
     *</code></pre>
     *
     * The corresponding flattened ordinals would be 0 for <code>id</code>, 1
     * for <code>p</code>, 5 for <code>crayon_count</code>. The gap corresponds
     * to the fact that the <code>pencil</code> column has four fields, after
     * which comes <code>crayon_count</code>.
     *
     * @param columnOrdinal logical column ordinal; e.g. 0 for <code>id</code>,
     * 1 for <code>p</code>, 2 for <code>crayon_count</code>
     *
     * @return flattened column ordinal
     */
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
}

// End FtrsIndexGuide.java
