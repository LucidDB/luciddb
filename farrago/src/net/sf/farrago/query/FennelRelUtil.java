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

package net.sf.farrago.query;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.keysindexes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import net.sf.saffron.core.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.util.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.rex.RexBuilder;

import java.nio.charset.*;

import java.sql.*;

import java.util.*;

/**
 * Static utilities for FennelRel implementations.  Examples in the comments
 * refer to the test tables EMPS and DEPTS defined in
 * {@link net.sf.farrago.test.PopulateTestData}.  For an overview and
 * terminology, please see
 * <a href="http://farrago.sf.net/design/TableIndexing.html">
 * the design docs</a>.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FennelRelUtil
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Generate a FemTupleAccessor from a FemTupleDescriptor.
     *
     * @param catalog catalog for storing transient objects
     * @param fennelDbHandle handle to Fennel database being accessed
     * @param tupleDesc source FemTupleDescriptor
     *
     * @return FemTupleAccessor for accessing tuples conforming to tupleDesc
     */
    public static FemTupleAccessor getAccessorForTupleDescriptor(
        FarragoCatalog catalog,
        FennelDbHandle fennelDbHandle,
        FemTupleDescriptor tupleDesc)
    {
        String tupleAccessorXmiString =
            fennelDbHandle.getAccessorXmiForTupleDescriptorTraced(tupleDesc);
        return catalog.parseTupleAccessor(tupleAccessorXmiString);
    }

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
    public static List getUnclusteredCoverageColList(
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
    public static Integer [] getUnclusteredCoverageArray(
        FarragoCatalog catalog,
        CwmSqlindex index)
    {
        List list = getUnclusteredCoverageColList(catalog,index);
        Integer [] projection = new Integer[list.size()];
        Iterator iter = list.iterator();
        int i = 0;
        for (; iter.hasNext(); ++i) {
            CwmColumn column = (CwmColumn) iter.next();
            projection[i] = new Integer(catalog.getColumnOrdinal(column));
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
    public static Integer [] getClusteredDistinctKeyArray(
        FarragoCatalog catalog,
        CwmSqlindex index)
    {
        assert (catalog.isClustered(index));
        List indexColumnList = new ArrayList();
        appendClusteredDistinctKey(catalog,index,indexColumnList);
        Integer [] array = new Integer[indexColumnList.size()];
        for (int i = 0; i < array.length; ++i) {
            array[i] =
                new Integer(
                    catalog.getColumnOrdinal(
                        (CwmColumn) indexColumnList.get(i)));
        }
        return array;
    }

    public static List getDistinctKeyColList(
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
    public static Integer [] getCollationKeyArray(
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
    public static FemTupleProjection getDistinctKeyProjection(
        FarragoCatalog catalog,
        CwmSqlindex index)
    {
        if (catalog.isClustered(index)) {
            List indexColumnList = new ArrayList();
            appendClusteredDistinctKey(catalog,index,indexColumnList);
            return createTupleProjectionFromColumnList(
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
        return createTupleProjection(catalog,newIotaProjection(n));
    }

    /**
     * Create a FemTupleDescriptor for the coverage tuple of an index.
     *
     * @param typeFactory factory for type analysis
     * @param index the CwmSqlindex to be described
     *
     * @return new FemTupleDescriptor
     */
    public static FemTupleDescriptor getCoverageTupleDescriptor(
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
    public static FemTupleProjection getCoverageProjection(
        FarragoCatalog catalog,
        CwmSqlindex index)
    {
        if (catalog.isClustered(index)) {
            // clustered index tuple is full table tuple
            int n = index.getSpannedClass().getFeature().size();
            return createTupleProjection(catalog,newIotaProjection(n));
        }

        List indexColumnList = getUnclusteredCoverageColList(catalog,index);

        return createTupleProjectionFromColumnList(catalog,indexColumnList);
    }

    /**
     * Create a FemTupleDescriptor for a SaffronType which is a row of
     * FarragoTypes.
     *
     * @param catalog catalog storing object definitions
     * @param rowType row of FarragoTypes
     *
     * @return generated tuple descriptor
     */
    public static FemTupleDescriptor createTupleDescriptorFromRowType(
        FarragoCatalog catalog,
        SaffronType rowType)
    {
        FemTupleDescriptor tupleDesc =
            catalog.newFemTupleDescriptor();
        SaffronField [] fields = rowType.getFields();
        for (int i = 0; i < fields.length; ++i) {
            assert (fields[i].getType() instanceof FarragoType);
            addTupleAttrDescriptor(
                catalog,
                tupleDesc,
                (FarragoType) fields[i].getType());
        }
        return tupleDesc;
    }

    /**
     * Generate a FemTupleProjection.
     *
     * @param catalog the catalog for storing transient objects
     * @param projection the projection to generate
     *
     * @return generated FemTupleProjection
     */
    public static FemTupleProjection createTupleProjection(
        FarragoCatalog catalog,
        Integer [] projection)
    {
        FemTupleProjection tupleProj =
            catalog.newFemTupleProjection();

        for (int i = 0; i < projection.length; ++i) {
            FemTupleAttrProjection attrProj =
                catalog.newFemTupleAttrProjection();
            tupleProj.getAttrProjection().add(attrProj);
            attrProj.setAttributeIndex(projection[i].intValue());
        }
        return tupleProj;
    }

    /**
     * Generate a projection of attribute indices in sequence from 0 to n-1.
     *
     * @param n length of array to generate
     *
     * @return generated array
     */
    public static Integer [] newIotaProjection(int n)
    {
        Integer [] array = new Integer[n];
        for (int i = 0; i < n; ++i) {
            array[i] = new Integer(i);
        }
        return array;
    }

    /**
     * Generate a projection of attribute indices in sequence from
     * (base) to (base + n-1).
     *
     * @param n length of array to generate
     *
     * @param base first value to generate
     *
     * @return generated array
     */
    public static Integer [] newBiasedIotaProjection(int n,int base)
    {
        Integer [] array = new Integer[n];
        for (int i = 0; i < n; ++i) {
            array[i] = new Integer(base + i);
        }
        return array;
    }

    private static int getByteLength(FarragoAtomicType type)
    {
        if (type instanceof FarragoPrimitiveType) {
            // for primitives, length is implied by datatype
            return 0;
        }
        assert (type instanceof FarragoPrecisionType);
        FarragoPrecisionType precisionType = (FarragoPrecisionType) type;

        // TODO:  numeric, date, etc.
        try {
            String charsetName = precisionType.getCharsetName();
            if (charsetName == null) {
                return precisionType.getPrecision();
            } else {
                Charset charset = Charset.forName(charsetName);
                return (int) charset.newEncoder().maxBytesPerChar()
                    * precisionType.getPrecision();
            }
                
        } catch (Exception ex) {
            throw Util.newInternal(
                ex,
                "Unsupported charset " + precisionType.getCharsetName());
        }
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

    private static void addTupleAttrDescriptor(
        FarragoCatalog catalog,
        FemTupleDescriptor tupleDesc,
        FarragoType type)
    {
        assert (type instanceof FarragoAtomicType);
        FarragoAtomicType atomicType = (FarragoAtomicType) type;
        FemTupleAttrDescriptor attrDesc =
            catalog.newFemTupleAttrDescriptor();
        tupleDesc.getAttrDescriptor().add(attrDesc);
        attrDesc.setTypeOrdinal(
            convertSqlTypeNumberToFennelTypeOrdinal(
                atomicType.getSimpleType().getTypeNumber().intValue()));
        attrDesc.setByteLength(getByteLength(atomicType));
        attrDesc.setNullable(atomicType.isNullable());
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

    private static int convertSqlTypeNumberToFennelTypeOrdinal(int sqlType)
    {
        // TODO:  return values correspond to enum
        // StandardTypeDescriptorOrdinal in Fennel; should be single-sourced
        // somehow
        // NOTE: Any changes must be copied into 
        // 1) enum StandardTypeDescriptorOrdinal
        // 2) net.sf.farrago.query.FennelRelUtil.convertSqlTypeNumberToFennelTypeOrdinal
        // 3) StandardTypeDescriptor class
        // 4) StoredTypeDescriptor standardTypes
        switch (sqlType) {
        case Types.BOOLEAN:
            return 9; // STANDARD_TYPE_BOOL
        case Types.TINYINT:
            return 1; // STANDARD_TYPE_INT_8
        case Types.SMALLINT:
            return 3; // STANDARD_TYPE_INT_16
        case Types.INTEGER:
            return 5; // STANDARD_TYPE_INT_32
        case Types.BIGINT:
            return 7; // STANDARD_TYPE_INT_64
        case Types.VARCHAR:
            return 13; // STANDARD_TYPE_VARCHAR
        case Types.VARBINARY:
            return 15; // STANDARD_TYPE_VARBINARY
        case Types.CHAR:
            return 12; // STANDARD_TYPE_CHAR
        case Types.BINARY:
            return 14; // STANDARD_TYPE_BINARY
        case Types.REAL:
            return 10; // STANDARD_TYPE_REAL
        case Types.DOUBLE:
            return 11; // STANDARD_TYPE_DOUBLE
        default:
            throw Util.newInternal("unimplemented SQL type number");
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
            addTupleAttrDescriptor(
                catalog,
                tupleDesc,
                typeFactory.createColumnType(column,true));
        }
        return tupleDesc;
    }

    public static FemTupleProjection createTupleProjectionFromColumnList(
        FarragoCatalog catalog,
        List indexColumnList)
    {
        FemTupleProjection tupleProj =
            catalog.newFemTupleProjection();
        Iterator indexColumnIter = indexColumnList.iterator();
        while (indexColumnIter.hasNext()) {
            Object column = indexColumnIter.next();
            FemTupleAttrProjection attrProj =
                catalog.newFemTupleAttrProjection();
            tupleProj.getAttrProjection().add(attrProj);
            attrProj.setAttributeIndex(
                catalog.getColumnOrdinal((CwmColumn) column));
        }
        return tupleProj;
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
            addTupleAttrDescriptor(
                catalog,
                tupleDesc,
                typeFactory.createColumnType(column,true));
        }
        return tupleDesc;
    }
}


// End FennelRelUtil.java
