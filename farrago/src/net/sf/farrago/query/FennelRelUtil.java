/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
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

import java.nio.charset.*;
import java.sql.*;
import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.*;


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
     * @param repos repos for storing transient objects
     * @param fennelDbHandle handle to Fennel database being accessed
     * @param tupleDesc source FemTupleDescriptor
     *
     * @return FemTupleAccessor for accessing tuples conforming to tupleDesc
     */
    public static FemTupleAccessor getAccessorForTupleDescriptor(
        FarragoRepos repos,
        FennelDbHandle fennelDbHandle,
        FemTupleDescriptor tupleDesc)
    {
        String tupleAccessorXmiString =
            fennelDbHandle.getAccessorXmiForTupleDescriptorTraced(tupleDesc);
        return repos.parseTupleAccessor(tupleAccessorXmiString);
    }

    /**
     * Create a FemTupleDescriptor for a RelDataType which is a row of
     * FarragoTypes.
     *
     * @param repos repos storing object definitions
     * @param rowType row of FarragoTypes
     *
     * @return generated tuple descriptor
     */
    public static FemTupleDescriptor createTupleDescriptorFromRowType(
        FarragoRepos repos,
        RelDataType rowType)
    {
        FemTupleDescriptor tupleDesc = repos.newFemTupleDescriptor();
        RelDataTypeField [] fields = rowType.getFields();
        for (int i = 0; i < fields.length; ++i) {
            assert (fields[i].getType() instanceof FarragoType);
            addTupleAttrDescriptor(repos, tupleDesc,
                (FarragoType) fields[i].getType());
        }
        return tupleDesc;
    }

    /**
     * Generate a FemTupleProjection.
     *
     * @param repos the repos for storing transient objects
     * @param projection the projection to generate
     *
     * @return generated FemTupleProjection
     */
    public static FemTupleProjection createTupleProjection(
        FarragoRepos repos,
        Integer [] projection)
    {
        FemTupleProjection tupleProj = repos.newFemTupleProjection();

        for (int i = 0; i < projection.length; ++i) {
            FemTupleAttrProjection attrProj =
                repos.newFemTupleAttrProjection();
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
    public static Integer [] newBiasedIotaProjection(
        int n,
        int base)
    {
        Integer [] array = new Integer[n];
        for (int i = 0; i < n; ++i) {
            array[i] = new Integer(base + i);
        }
        return array;
    }

    public static void addTupleAttrDescriptor(
        FarragoRepos repos,
        FemTupleDescriptor tupleDesc,
        FarragoType type)
    {
        assert (type instanceof FarragoAtomicType);
        FarragoAtomicType atomicType = (FarragoAtomicType) type;
        FemTupleAttrDescriptor attrDesc = repos.newFemTupleAttrDescriptor();
        tupleDesc.getAttrDescriptor().add(attrDesc);
        attrDesc.setTypeOrdinal(
            convertSqlTypeNumberToFennelTypeOrdinal(
                atomicType.getSimpleType().getTypeNumber().intValue()));
        attrDesc.setByteLength(getByteLength(atomicType));
        attrDesc.setNullable(atomicType.isNullable());
    }

    public static FemTupleProjection createTupleProjectionFromColumnList(
        FarragoRepos repos,
        List indexColumnList)
    {
        FemTupleProjection tupleProj = repos.newFemTupleProjection();
        Iterator indexColumnIter = indexColumnList.iterator();
        while (indexColumnIter.hasNext()) {
            FemAbstractColumn column =
                (FemAbstractColumn) indexColumnIter.next();
            FemTupleAttrProjection attrProj =
                repos.newFemTupleAttrProjection();
            tupleProj.getAttrProjection().add(attrProj);
            attrProj.setAttributeIndex(column.getOrdinal());
        }
        return tupleProj;
    }

    private static int getByteLength(FarragoAtomicType type)
    {
        if (type instanceof FarragoPrimitiveType
                || type instanceof FarragoDateTimeType) {
            // for primitives, length is implied by datatype
            return 0;
        }
        assert (type instanceof FarragoPrecisionType);
        FarragoPrecisionType precisionType = (FarragoPrecisionType) type;

        // TODO:  numeric, date, etc.
        try {
            if (!precisionType.isCharType()) {
                if (precisionType.getSqlTypeName().equals(SqlTypeName.Bit)) {
                    return (precisionType.getPrecision() + 7) / 8;
                }
                return precisionType.getPrecision();
            } else {
                assert (null != precisionType.getCharsetName());
                Charset charset = precisionType.getCharset();
                return (int) charset.newEncoder().maxBytesPerChar() * precisionType
                    .getPrecision();
            }
        } catch (Exception ex) {
            throw Util.newInternal(ex,
                "Unsupported charset " + precisionType.getCharsetName());
        }
    }

    private static int convertSqlTypeNumberToFennelTypeOrdinal(int sqlType)
    {
        // TODO:  return values correspond to enum
        // StandardTypeDescriptorOrdinal in Fennel; should be single-sourced
        // somehow
        // NOTE: Any changes must be copied into 
        // 1) enum StandardTypeDescriptorOrdinal
        // 2) this method
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
        case Types.DATE:
        case Types.TIME:
        case Types.TIMESTAMP:
        case Types.BIGINT:
            return 7; // STANDARD_TYPE_INT_64
        case Types.VARCHAR:
            return 13; // STANDARD_TYPE_VARCHAR
        case Types.BIT:
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
}


// End FennelRelUtil.java
