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
package net.sf.farrago.type;

import java.sql.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;


/**
 * Helper base class for implementing Jdbc metadata interfaces.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJdbcMetaDataImpl
{

    //~ Instance fields --------------------------------------------------------

    /**
     * Type info to return.
     */
    protected final RelDataType rowType;

    //~ Constructors -----------------------------------------------------------

    protected FarragoJdbcMetaDataImpl(RelDataType rowType)
    {
        this.rowType = rowType;
    }

    //~ Methods ----------------------------------------------------------------

    protected RelDataType getFieldNamedType(int fieldOrdinal)
    {
        return rowType.getFields()[fieldOrdinal - 1].getType();
    }

    protected RelDataType getFieldType(int fieldOrdinal)
    {
        RelDataType namedType = getFieldNamedType(fieldOrdinal);
        if (namedType.getSqlTypeName() == SqlTypeName.Distinct) {
            // for most metadata calls, report information about the
            // predefined type on which the distinct type is based
            return namedType.getFields()[0].getType();
        } else {
            return namedType;
        }
    }

    public String getFieldName(int fieldOrdinal)
    {
        return rowType.getFields()[fieldOrdinal - 1].getName();
    }

    protected int getFieldCount()
    {
        return rowType.getFieldList().size();
    }

    protected String getFieldClassName(int fieldOrdinal)
    {
        // TODO
        return "";
    }

    protected int getFieldJdbcType(int fieldOrdinal)
    {
        RelDataType type = getFieldNamedType(fieldOrdinal);
        SqlTypeName typeName = type.getSqlTypeName();
        if (typeName == null) {
            return Types.OTHER;
        }
        return typeName.getJdbcOrdinal();
    }

    protected String getFieldTypeName(int fieldOrdinal)
    {
        RelDataType type = getFieldNamedType(fieldOrdinal);
        SqlTypeName typeName = type.getSqlTypeName();
        if (typeName == null) {
            return type.toString();
        }
        switch (typeName.getOrdinal()) {
        case SqlTypeName.Structured_ordinal:
        case SqlTypeName.Distinct_ordinal:
            return type.getSqlIdentifier().toString();
        case SqlTypeName.IntervalDayTime_ordinal:
        case SqlTypeName.IntervalYearMonth_ordinal:
            return type.toString();
        }
        return typeName.getName();
    }

    protected int getFieldPrecision(int fieldOrdinal)
    {
        RelDataType type = getFieldType(fieldOrdinal);
        return type.getPrecision();
    }

    protected int getFieldScale(int fieldOrdinal)
    {
        RelDataType type = getFieldType(fieldOrdinal);
        SqlTypeName typeName = type.getSqlTypeName();
        if (typeName == null) {
            return 0;
        }
        if (typeName.allowsPrecScale(true, true)) {
            return type.getScale();
        } else {
            return 0;
        }
    }

    protected int getFieldDisplaySize(int column)
    {
        int precision = getFieldPrecision(column);
        int type = getFieldJdbcType(column);
        switch (type) {
        case Types.BOOLEAN:

            // 5 for max(strlen("true"),strlen("false"))
            return 5;
        case Types.DATE:

            // 10 for strlen("yyyy-mm-dd")
            return 10;
        case Types.TIME:
            if (precision == 0) {
                // 8 for strlen("hh:mm:ss")
                return 8;
            } else {
                // 1 extra for decimal point
                return 9 + precision;
            }
        case Types.TIMESTAMP:
            if (precision == 0) {
                // 19 for strlen("yyyy-mm-dd hh:mm:ss")
                return 19;
            } else {
                // 1 extra for decimal point
                return 20 + precision;
            }
        case Types.REAL:
        case Types.FLOAT:
            return 13;
        case Types.DOUBLE:
            return 22;
        default:

            // TODO:  adjust for numeric formatting, etc.
            return precision;
        }
    }

    protected String getFieldCatalogName(int fieldOrdinal)
    {
        // TODO
        return "";
    }

    protected String getFieldSchemaName(int fieldOrdinal)
    {
        // TODO
        return "";
    }

    protected String getFieldTableName(int fieldOrdinal)
    {
        // TODO
        return "";
    }

    protected int isFieldNullable(int fieldOrdinal)
    {
        RelDataType type = getFieldType(fieldOrdinal);
        return
            type.isNullable() ? ResultSetMetaData.columnNullable
            : ResultSetMetaData.columnNoNulls;
    }

    protected boolean isFieldAutoIncrement(int fieldOrdinal)
    {
        return false;
    }

    protected boolean isFieldCaseSensitive(int fieldOrdinal)
    {
        // TODO
        return false;
    }

    protected boolean isFieldSearchable(int fieldOrdinal)
    {
        return true;
    }

    protected boolean isFieldSigned(int fieldOrdinal)
    {
        // TODO
        RelDataType type = getFieldType(fieldOrdinal);
        if (SqlTypeUtil.isNumeric(type)) {
            return true;
        } else {
            return false;
        }
    }

    protected boolean isFieldCurrency(int fieldOrdinal)
    {
        return false;
    }

    protected boolean isFieldReadOnly(int fieldOrdinal)
    {
        return true;
    }

    protected boolean isFieldWritable(int fieldOrdinal)
    {
        return false;
    }

    protected boolean isFieldDefinitelyWritable(int fieldOrdinal)
    {
        return false;
    }
}

// End FarragoJdbcMetaDataImpl.java
