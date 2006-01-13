/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
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
package net.sf.farrago.jdbc.engine;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.type.SqlTypeName;

import net.sf.farrago.session.FarragoSessionStmtParamDef;
import net.sf.farrago.session.FarragoSessionStmtParamDefFactory;

/**
 * FarragoJdbcEngineParamDefFactory implements 
 * {@link FarragoSessionStmtParamDefFactory} for JDBC.
 * 
 * @author stephan
 * @version $Id$
 */
public class FarragoJdbcEngineParamDefFactory 
    implements FarragoSessionStmtParamDefFactory
{
    // Implement FarragoSessionStmtParamDefFactory
    public FarragoSessionStmtParamDef newParamDef(
        String paramName, RelDataType type)
    {
        final SqlTypeName sqlTypeName = type.getSqlTypeName();
        switch (sqlTypeName.getOrdinal()) {
        case SqlTypeName.Char_ordinal:
        case SqlTypeName.Varchar_ordinal:
            return new FarragoJdbcEngineStringParamDef(paramName, type);
        case SqlTypeName.Binary_ordinal:
        case SqlTypeName.Varbinary_ordinal:
            return new FarragoJdbcEngineBinaryParamDef(paramName, type);
        case SqlTypeName.Date_ordinal:
            return new FarragoJdbcEngineDateParamDef(paramName, type);
        case SqlTypeName.Timestamp_ordinal:
            return new FarragoJdbcEngineTimestampParamDef(paramName, type);
        case SqlTypeName.Time_ordinal:
            return new FarragoJdbcEngineTimeParamDef(paramName, type);
        case SqlTypeName.Decimal_ordinal:
            return new FarragoJdbcEngineDecimalParamDef(paramName, type);
        default:
            return new FarragoJdbcEngineParamDef(paramName, type);
        }
    }
}

