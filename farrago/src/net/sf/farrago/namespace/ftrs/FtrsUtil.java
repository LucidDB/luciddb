/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.farrago.namespace.ftrs;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.keysindexes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;
import net.sf.farrago.query.*;

import java.sql.*;

import java.util.*;

/**
 * Static utilities for FTRS.
 *
 * @author John V. Sichi
 * @version $Id$
 */
abstract class FtrsUtil
{
    /**
     * Get a list of columns covered by an unclustered index.
     *
     *<p>
     *
     * Example:  for index EMPS_UX, the result is
     * [ NAME, DEPTNO, EMPNO ]
     *
     * @param catalog catalog storing object definitions
     * @param index index for which to compute column list
     *
     * @return List (of CwmColumn) making up an unclustered index's tuple
     */
    static List getUnclusteredCoverageColList(
        FarragoCatalog catalog,
        CwmSqlindex index)
    {
        CwmSqlindex clusteredIndex =
            catalog.getClusteredIndex(index.getSpannedClass());
        List indexColumnList = new ArrayList();
        appendDefinedKey(indexColumnList,index);
        appendClusteredDistinctKey(catalog,clusteredIndex,indexColumnList);
        return indexColumnList;
    }

    /**
     * Same as getUnclusteredCoverageColList, but return table-relative column
     * ordinals instead.
     *
     *<p>
     *
     * Example:  for index EMPS_UX, the result is
     * [ 1, 2, 0 ]
     *
     * @param catalog catalog storing object definitions
     * @param index index for which to compute projection
     *
     * @return projection as array of 0-based table-relative column ordinals
     */
    static Integer [] getUnclusteredCoverageArray(
        FarragoCatalog catalog,
        CwmSqlindex index)
    {
        List list = getUnclusteredCoverageColList(catalog,index);
        Integer [] projection = new Integer[list.size()];
        Iterator iter = list.iterator();
        int i = 0;
        for (; iter.hasNext(); ++i) {
            FemAbstractColumn column = (FemAbstractColumn) iter.next();
            projection[i] = new Integer(column.getOrdinal());
        }
        return projection;
    }

    /**
     * Get the distinct key of a clustered index.
     *
     *<p>
     *
     * Example:  for the clustered index on table EMPS, the result is
     * [ 2, 0 ].  But if the clustered index were defined as non-unique on
     * column city instead, then the result would be [ 4, 2, 0 ].  For a
     * non-unique clustered index on empno, the result would be [ 0, 2 ].
     *
     * @param catalog catalog for storing transient objects
     * @param index the CwmSqlindex for which the key is to be projected
     *
     * @return array of 0-based column ordinals
     */
    static Integer [] getClusteredDistinctKeyArray(
        FarragoCatalog catalog,
        CwmSqlindex index)
    {
        assert (catalog.isClustered(index));
        List indexColumnList = new ArrayList();
        appendClusteredDistinctKey(catalog,index,indexColumnList);
        Integer [] array = new Integer[indexColumnList.size()];
        for (int i = 0; i < array.length; ++i) {
            FemAbstractColumn column = (FemAbstractColumn)
                indexColumnList.get(i);
            array[i] = new Integer(column.getOrdinal());
        }
        return array;
    }

    static List getDistinctKeyColList(
        FarragoCatalog catalog,
        CwmSqlindex index)
    {
        List indexColumnList = new ArrayList();
        if (catalog.isClustered(index)) {
            appendClusteredDistinctKey(catalog,index,indexColumnList);
        } else {
            if (index.isUnique()) {
                appendDefinedKey(indexColumnList,index);
            } else {
                return getUnclusteredCoverageColList(catalog,index);
            }
        }
        return indexColumnList;
    }

    /**
     * Get the collation key of an index.
     *
     *<p>
     *
     * Example:  for index DEPTS_UNIQUE_NAME, the result is
     * [ 1, 0 ]
     *
     * @param catalog catalog storing object definitions
     * @param index index for which to compute projection
     *
     * @return projection as array of 0-based table-relative column ordinals
     */
    static Integer [] getCollationKeyArray(
        FarragoCatalog catalog,
        CwmSqlindex index)
    {
        if (catalog.isClustered(index)) {
            return getClusteredDistinctKeyArray(catalog,index);
        } else {
            return getUnclusteredCoverageArray(catalog,index);
        }
    }
    
    /**
     * Get a FemTupleProjection which specifies how to extract the distinct
     * key from the result of getCoverageTupleDescriptor.  The projected
     * ordinals are relative to the index coverage tuple, not the table.
     *
     *<p>
     *
     * Example:  for the clustered index of table EMPS, the result is
     * [ 2, 0 ].  For index DEPTS_UNIQUE_NAME, the result is [ 0 ].
     * For index EMPS_UX, the result is [ 0, 1, 2 ].
     *
     * @param catalog catalog for storing transient objects
     * @param index the CwmSqlindex for which the key is to be projected
     *
     * @return new FemTupleProjection
     */
    static FemTupleProjection getDistinctKeyProjection(
        FarragoCatalog catalog,
        CwmSqlindex index)
    {
        if (catalog.isClustered(index)) {
            List indexColumnList = new ArrayList();
            appendClusteredDistinctKey(catalog,index,indexColumnList);
            return FennelRelUtil.createTupleProjectionFromColumnList(
                catalog,
                indexColumnList);
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
            n = getUnclusteredCoverageColList(catalog,index).size();
        }
        return FennelRelUtil.createTupleProjection(
            catalog,
            FennelRelUtil.newIotaProjection(n));
    }

    /**
     * Create a FemTupleDescriptor for the coverage tuple of an index.
     *
     * @param typeFactory factory for type analysis
     * @param index the CwmSqlindex to be described
     *
     * @return new FemTupleDescriptor
     */
    static FemTupleDescriptor getCoverageTupleDescriptor(
        FarragoTypeFactory typeFactory,
        CwmSqlindex index)
    {
        if (typeFactory.getCatalog().isClustered(index)) {
            return getClusteredCoverageTupleDescriptor(typeFactory,index);
        } else {
            return getUnclusteredCoverageTupleDescriptor(typeFactory,index);
        }
    }

    /**
     * Create a FemTupleProjection which specifies how to extract the
     * index coverage tuple from a full table tuple.
     *
     *<p>
     *
     * Example:  for the clustered index of table EMPS, the result is
     * [ 0, 1, 2, 3, 4, 5 ].  For index DEPTS_UNIQUE_NAME, the result
     * is [ 1, 0 ].
     *
     * @param catalog catalog for storing transient objects
     * @param index the CwmSqlindex for which the tuple is to be projected
     *
     * @return new FemTupleProjection
     */
    static FemTupleProjection getCoverageProjection(
        FarragoCatalog catalog,
        CwmSqlindex index)
    {
        if (catalog.isClustered(index)) {
            // clustered index tuple is full table tuple
            int n = index.getSpannedClass().getFeature().size();
            return FennelRelUtil.createTupleProjection(
                catalog,FennelRelUtil.newIotaProjection(n));
        }

        List indexColumnList = getUnclusteredCoverageColList(catalog,index);

        return FennelRelUtil.createTupleProjectionFromColumnList(
            catalog,indexColumnList);
    }

    private static void appendConstraintColumns(
        List list,
        CwmUniqueConstraint constraint)
    {
        Iterator iter = constraint.getFeature().iterator();
        while (iter.hasNext()) {
            Object column = iter.next();
            if (list.contains(column)) {
                continue;
            }
            list.add(column);
        }
    }

    private static void appendDefinedKey(List list,CwmSqlindex index)
    {
        Iterator iter = index.getIndexedFeature().iterator();
        while (iter.hasNext()) {
            CwmIndexedFeature indexedFeature = (CwmIndexedFeature) iter.next();
            Object column = indexedFeature.getFeature();
            if (list.contains(column)) {
                continue;
            }
            list.add(column);
        }
    }

    private static void appendClusteredDistinctKey(
        FarragoCatalog catalog,
        CwmSqlindex clusteredIndex,
        List indexColumnList)
    {
        assert(catalog.isClustered(clusteredIndex));
        appendDefinedKey(indexColumnList,clusteredIndex);
        if (!clusteredIndex.isUnique()) {
            appendConstraintColumns(
                indexColumnList,
                catalog.getPrimaryKey(clusteredIndex.getSpannedClass()));
        }
    }

    private static FemTupleDescriptor getClusteredCoverageTupleDescriptor(
        FarragoTypeFactory typeFactory,
        CwmSqlindex index)
    {
        FarragoCatalog catalog = typeFactory.getCatalog();
        FemTupleDescriptor tupleDesc =
            catalog.newFemTupleDescriptor();
        Iterator columnIter = index.getSpannedClass().getFeature().iterator();
        while (columnIter.hasNext()) {
            Object obj = columnIter.next();
            if (!(obj instanceof CwmColumn)) {
                continue;
            }
            CwmColumn column = (CwmColumn) obj;
            FennelRelUtil.addTupleAttrDescriptor(
                catalog,
                tupleDesc,
                typeFactory.createColumnType(column,true));
        }
        return tupleDesc;
    }

    private static FemTupleDescriptor getUnclusteredCoverageTupleDescriptor(
        FarragoTypeFactory typeFactory,
        CwmSqlindex index)
    {
        FarragoCatalog catalog = typeFactory.getCatalog();
        FemTupleDescriptor tupleDesc =
            catalog.newFemTupleDescriptor();
        List colList = getUnclusteredCoverageColList(catalog,index);
        Iterator columnIter = colList.iterator();
        while (columnIter.hasNext()) {
            CwmColumn column = (CwmColumn) columnIter.next();
            FennelRelUtil.addTupleAttrDescriptor(
                catalog,
                tupleDesc,
                typeFactory.createColumnType(column,true));
        }
        return tupleDesc;
    }
}

// End FtrsUtil.java
