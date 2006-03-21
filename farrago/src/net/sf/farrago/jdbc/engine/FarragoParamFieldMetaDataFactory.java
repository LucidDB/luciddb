/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
// Portions Copyright (C) 2006-2006 John V. Sichi
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

package net.sf.farrago.jdbc.engine;

import java.sql.ParameterMetaData;
import java.util.List;

import org.eigenbase.sql.type.SqlTypeUtil;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import net.sf.farrago.jdbc.param.FarragoParamFieldMetaData;

/**
 * Factory class for creating the per-column metadata passed to the
 * client driver for its ParameterMetaData implementation.
 *
 * @author Angel Chang
 * @since March 3, 2006
 * @version $Id$
 */
public class FarragoParamFieldMetaDataFactory
{
    /** private constructor to prevent instantiation. */
    private FarragoParamFieldMetaDataFactory()
    {
    }

    public static FarragoParamFieldMetaData newParamFieldMetaData(
        RelDataType type, int mode)
    {
        FarragoParamFieldMetaData fieldMeta = new FarragoParamFieldMetaData();

        fieldMeta.nullable = type.isNullable()?
            ParameterMetaData.parameterNullable : ParameterMetaData.parameterNoNulls;
        fieldMeta.type = type.getSqlTypeName().getJdbcOrdinal();
        fieldMeta.typeName = type.getSqlTypeName().getName();
        // TODO: Get class name;
        fieldMeta.className = "";
        fieldMeta.precision = type.getPrecision();
        fieldMeta.scale = type.getSqlTypeName().allowsScale()? type.getScale(): 0;
        // TODO: treat all numerics as signed
        fieldMeta.signed = SqlTypeUtil.isNumeric(type);
        fieldMeta.mode = mode;
        fieldMeta.paramTypeStr = type.toString();

        return fieldMeta;
    }

    /**
     * Determines the parameter column meta data from the rowType
     * @param rowType
     * @return
     */
    public static FarragoParamFieldMetaData[] newParamMetaData(
        RelDataType rowType, int mode)
    {
        FarragoParamFieldMetaData[] metaData;

        List fieldTypes = rowType.getFieldList();
        int colCnt = fieldTypes.size();
        metaData = new FarragoParamFieldMetaData[colCnt];

        for (int i=0; i < colCnt; ++i) {
            RelDataTypeField f = (RelDataTypeField)fieldTypes.get(i);
            RelDataType relType = f.getType();

            FarragoParamFieldMetaData meta =
                newParamFieldMetaData(relType, mode);
            metaData[i] = meta;
        }
        return metaData;
    }


};

// End FarragoParamFieldMetaDataFactory.java
