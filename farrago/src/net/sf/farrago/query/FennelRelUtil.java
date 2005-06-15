/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
package net.sf.farrago.query;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.util.*;
import net.sf.farrago.session.FarragoSessionPlanner;
import net.sf.farrago.FarragoMetadataFactory;
import net.sf.farrago.FarragoPackage;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * Static utilities for FennelRel implementations.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FennelRelUtil
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Generates a FemTupleAccessor from a FemTupleDescriptor.
     *
     * @param repos repos for storing transient objects
     * @param fennelDbHandle handle to Fennel database being accessed
     * @param tupleDesc source FemTupleDescriptor
     *
     * @return FemTupleAccessor for accessing tuples conforming to tupleDesc
     */
    public static FemTupleAccessor getAccessorForTupleDescriptor(
        FarragoMetadataFactory repos,
        FennelDbHandle fennelDbHandle,
        FemTupleDescriptor tupleDesc)
    {
        if (fennelDbHandle == null) {
            String tupleAccessorXmiString = "<xmiAccessor/>";
            return tupleDescriptorToAccessor(repos, tupleDesc);
        }
        String tupleAccessorXmiString =
            fennelDbHandle.getAccessorXmiForTupleDescriptorTraced(tupleDesc);
        // TODO: Move FarragoRepos.getTransientFarragoPackage up to the base
        //   class, FarragoMetadataFactory (which is generated, by the way).
        FarragoPackage transientFarragoPackage;
        if (repos instanceof FarragoRepos) {
            FarragoRepos farragoRepos = (FarragoRepos) repos;
            transientFarragoPackage = farragoRepos.getTransientFarragoPackage();
        } else {
            transientFarragoPackage = repos.getRootPackage();
        }
        Collection c =
            JmiUtil.importFromXmiString(
                transientFarragoPackage,
                tupleAccessorXmiString);
        assert (c.size() == 1);
        FemTupleAccessor accessor = (FemTupleAccessor) c.iterator().next();
        return accessor;
    }

    /**
     * Converts a {@link FemTupleDescriptor} into a {@link FemTupleAccessor}
     * without invoking native methods.
     */
    private static FemTupleAccessor tupleDescriptorToAccessor(
        FarragoMetadataFactory repos,
        FemTupleDescriptor tupleDesc)
    {
        FemTupleAccessor tupleAccessor = repos.newFemTupleAccessor();
        tupleAccessor.setMinByteLength(-1);
        tupleAccessor.setBitFieldOffset(-1);
        java.util.List attrDescriptors = tupleDesc.getAttrDescriptor();
        for (int i = 0; i < attrDescriptors.size(); i++) {
            FemTupleAttrDescriptor attrDescriptor =
                (FemTupleAttrDescriptor) attrDescriptors.get(i);
            FemTupleAttrAccessor attrAccessor =
                repos.newFemTupleAttrAccessor();
            attrAccessor.setNullBitIndex(-1);
            attrAccessor.setFixedOffset(-1);
            attrAccessor.setEndIndirectOffset(-1);
            attrAccessor.setBitValueIndex(-1);
            tupleAccessor.getAttrAccessor().add(attrAccessor);
        }
        return tupleAccessor;
    }

    /**
     * Creates a FemTupleDescriptor for a RelDataType which is a row.
     *
     * @param repos repos storing object definitions
     * @param rowType row type descriptor
     *
     * @return generated tuple descriptor
     */
    public static FemTupleDescriptor createTupleDescriptorFromRowType(
        FarragoMetadataFactory repos,
        RelDataTypeFactory typeFactory,
        RelDataType rowType)
    {
        rowType = SqlTypeUtil.flattenRecordType(
            typeFactory,
            rowType,
            null);
        FemTupleDescriptor tupleDesc = repos.newFemTupleDescriptor();
        RelDataTypeField [] fields = rowType.getFields();
        for (int i = 0; i < fields.length; ++i) {
            addTupleAttrDescriptor(repos, tupleDesc, fields[i].getType());
        }
        return tupleDesc;
    }

    /**
     * Generates a FemTupleProjection.
     *
     * @param repos the repos for storing transient objects
     * @param projection the projection to generate
     *
     * @return generated FemTupleProjection
     */
    public static FemTupleProjection createTupleProjection(
        FarragoMetadataFactory repos,
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
     * Generates a projection of attribute indices in sequence from 0 to n-1.
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
     * Generates a projection of attribute indices in sequence from
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
        FarragoMetadataFactory repos,
        FemTupleDescriptor tupleDesc,
        RelDataType type)
    {
        FemTupleAttrDescriptor attrDesc = repos.newFemTupleAttrDescriptor();
        tupleDesc.getAttrDescriptor().add(attrDesc);
        attrDesc.setTypeOrdinal(
            convertSqlTypeNameToFennelTypeOrdinal(
                type.getSqlTypeName()));
        int byteLength = SqlTypeUtil.getMaxByteSize(type);
        attrDesc.setByteLength(byteLength);
        attrDesc.setNullable(type.isNullable());
    }

    public static int convertSqlTypeNameToFennelTypeOrdinal(
        SqlTypeName sqlType)
    {
        // TODO:  return values correspond to enum
        // StandardTypeDescriptorOrdinal in Fennel; should be single-sourced
        // somehow
        // NOTE: Any changes must be copied into
        // 1) enum StandardTypeDescriptorOrdinal
        // 2) this method
        // 3) StandardTypeDescriptor class
        // 4) StoredTypeDescriptor standardTypes
        switch (sqlType.getOrdinal()) {
        case SqlTypeName.Boolean_ordinal:
            return 9; // STANDARD_TYPE_BOOL
        case SqlTypeName.Tinyint_ordinal:
            return 1; // STANDARD_TYPE_INT_8
        case SqlTypeName.Smallint_ordinal:
            return 3; // STANDARD_TYPE_INT_16
        case SqlTypeName.Integer_ordinal:
            return 5; // STANDARD_TYPE_INT_32
        case SqlTypeName.Date_ordinal:
        case SqlTypeName.Time_ordinal:
        case SqlTypeName.Timestamp_ordinal:
        case SqlTypeName.Bigint_ordinal:
            return 7; // STANDARD_TYPE_INT_64
        case SqlTypeName.Varchar_ordinal:
            return 13; // STANDARD_TYPE_VARCHAR
        case SqlTypeName.Varbinary_ordinal:
        case SqlTypeName.Multiset_ordinal:
            return 15; // STANDARD_TYPE_VARBINARY
        case SqlTypeName.Char_ordinal:
            return 12; // STANDARD_TYPE_CHAR
        case SqlTypeName.Binary_ordinal:
            return 14; // STANDARD_TYPE_BINARY
        case SqlTypeName.Real_ordinal:
            return 10; // STANDARD_TYPE_REAL
        case SqlTypeName.Float_ordinal:
        case SqlTypeName.Double_ordinal:
            return 11; // STANDARD_TYPE_DOUBLE
        default:
            throw Util.newInternal("unimplemented SQL type number");
        }
    }

    /**
     * Returns the repository that a relational expression belongs to.
     */
    public static FarragoPreparingStmt getPreparingStmt(FennelRel rel)
    {
        RelOptCluster cluster = rel.getCluster();
        RelOptPlanner planner = cluster.getPlanner();
        if (planner instanceof FarragoSessionPlanner) {
            FarragoSessionPlanner farragoPlanner =
                (FarragoSessionPlanner) planner;
            return (FarragoPreparingStmt)farragoPlanner.getPreparingStmt();
        } else {
            return null;
        }
    }

    /**
     * Returnss the repository that a relational expression belongs to.
     */
    public static FarragoRepos getRepos(FennelRel rel)
    {
        return getPreparingStmt(rel).getRepos();
    }
}


// End FennelRelUtil.java
