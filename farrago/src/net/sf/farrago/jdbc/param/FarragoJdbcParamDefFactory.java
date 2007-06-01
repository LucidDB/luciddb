/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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
package net.sf.farrago.jdbc.param;

import java.sql.*;


/**
 * FarragoJdbcEngineParamDefFactory create a FarragoJdbcParamDef (refactored
 * from FarragoJdbcEngineParamDefFactory) This class is JDK 1.4 compatible.
 *
 * @author Angel Chang
 * @version $Id$
 */
public class FarragoJdbcParamDefFactory
{
    //~ Methods ----------------------------------------------------------------

    public static FarragoJdbcParamDef newParamDef(
        String paramName,
        FarragoParamFieldMetaData paramMetaData,
        boolean useFennelTuple)
    {
        if (useFennelTuple) {
            return newFennelTupleParamDef(paramName, paramMetaData);
        } else {
            return newParamDef(paramName, paramMetaData);
        }
    }

    private static FarragoJdbcParamDef newFennelTupleParamDef(
        String paramName,
        FarragoParamFieldMetaData paramMetaData)
    {
        FarragoJdbcParamDef paramDef = newParamDef(paramName, paramMetaData);
        return new FarragoJdbcFennelTupleParamDef(
            paramName,
            paramMetaData,
            paramDef);
    }

    private static FarragoJdbcParamDef newParamDef(
        String paramName,
        FarragoParamFieldMetaData paramMetaData)
    {
        switch (paramMetaData.type) {
        case Types.BIT:
        case Types.BOOLEAN:
            return new FarragoJdbcBooleanParamDef(paramName, paramMetaData);
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.BIGINT:
            return new FarragoJdbcIntParamDef(paramName, paramMetaData);
        case Types.REAL:
        case Types.FLOAT:
        case Types.DOUBLE:
            return new FarragoJdbcApproxParamDef(paramName, paramMetaData);
        case Types.DECIMAL:
        case Types.NUMERIC:
            return new FarragoJdbcDecimalParamDef(paramName, paramMetaData);
        case Types.CHAR:
        case Types.VARCHAR:
            return new FarragoJdbcStringParamDef(paramName, paramMetaData);
        case Types.BINARY:
        case Types.VARBINARY:
            return new FarragoJdbcBinaryParamDef(paramName, paramMetaData);
        case Types.DATE:
            return new FarragoJdbcDateParamDef(paramName, paramMetaData);
        case Types.TIMESTAMP:
            return new FarragoJdbcTimestampParamDef(paramName, paramMetaData);
        case Types.TIME:
            return new FarragoJdbcTimeParamDef(paramName, paramMetaData);
        default:
            return new FarragoJdbcParamDef(paramName, paramMetaData);
        }
    }
}

// End FarragoJdbcParamDefFactory.java
