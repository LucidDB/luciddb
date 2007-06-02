/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 Disruptive Tech
// Copyright (C) 2006-2007 LucidEra, Inc.
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
package net.sf.farrago.runtime;

import java.sql.*;

import net.sf.farrago.jdbc.param.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;


/**
 * Provides runtime support for implementing JDBC interfaces.
 *
 * @author Angel Chang
 * @version $Id$
 */
public abstract class FarragoRuntimeJdbcUtil
{
    //~ Methods ----------------------------------------------------------------

    public static FarragoParamFieldMetaData newParamFieldMetaData(
        RelDataType type,
        int mode)
    {
        FarragoParamFieldMetaData fieldMeta = new FarragoParamFieldMetaData();

        fieldMeta.nullable =
            type.isNullable() ? ParameterMetaData.parameterNullable
            : ParameterMetaData.parameterNoNulls;
        fieldMeta.type = type.getSqlTypeName().getJdbcOrdinal();
        fieldMeta.typeName = type.getSqlTypeName().name();

        // TODO: Get class name;
        fieldMeta.className = "";
        fieldMeta.precision = type.getPrecision();
        fieldMeta.scale =
            type.getSqlTypeName().allowsScale() ? type.getScale() : 0;

        // TODO: treat all numerics as signed
        fieldMeta.signed = SqlTypeUtil.isNumeric(type);
        fieldMeta.mode = mode;
        fieldMeta.paramTypeStr = type.toString();

        return fieldMeta;
    }
}

// End FarragoRuntimeJdbcUtil.java
