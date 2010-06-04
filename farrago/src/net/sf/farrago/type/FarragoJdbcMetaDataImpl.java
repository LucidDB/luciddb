/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
import java.util.List;

import org.eigenbase.jdbc4.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.Util;


/**
 * Helper base class for implementing Jdbc metadata interfaces.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJdbcMetaDataImpl
    extends Unwrappable
{
    //~ Instance fields --------------------------------------------------------

    protected final RelDataType rowType;
    private final List<List<String>> fieldOrigins;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FarragoJdbcMetaDataImpl.
     *
     * @param rowType Type info to return
     * @param fieldOrigins Origin of each field in column of catalog object
     */
    protected FarragoJdbcMetaDataImpl(
        RelDataType rowType,
        List<List<String>> fieldOrigins)
    {
        this.rowType = rowType;
        this.fieldOrigins = fieldOrigins;
        assert rowType != null;
        assert fieldOrigins != null;
        assert fieldOrigins.size() == rowType.getFieldCount()
            : "field origins " + fieldOrigins
            + " have different count than row type " + rowType;
    }

    //~ Methods ----------------------------------------------------------------

    public RelDataType getFieldNamedType(int fieldOrdinal)
    {
        return rowType.getFields()[fieldOrdinal - 1].getType();
    }

    public RelDataType getFieldType(int fieldOrdinal)
    {
        RelDataType namedType = getFieldNamedType(fieldOrdinal);
        if (namedType.getSqlTypeName() == SqlTypeName.DISTINCT) {
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

    public int getFieldCount()
    {
        return rowType.getFieldCount();
    }

    public String getFieldClassName(int fieldOrdinal)
    {
        int type = getFieldJdbcType(fieldOrdinal);
        switch (type) {
        case Types.ARRAY:
            return "java.sql.Array";
        case Types.BIGINT:
            return "java.lang.Long";
        case Types.BINARY:
            return "[B";
        case Types.BIT:
            return "java.lang.Boolean";
        case Types.BLOB:
            return "java.sql.Blob";
        case Types.BOOLEAN:
            return "java.lang.Boolean";
        case Types.CHAR:
            return "java.lang.String";
        case Types.CLOB:
            return "java.sql.Clob";
        case Types.DATALINK:
            return "";
        case Types.DATE:
            return "java.sql.Date";
        case Types.DECIMAL:
            return "java.math.BigDecimal";
        case Types.DISTINCT:
            // TODO
            return "";
        case Types.DOUBLE:
            return "java.lang.Double";
        case Types.FLOAT:
            return "java.lang.Double";
        case Types.INTEGER:
            return "java.lang.Integer";
        case Types.JAVA_OBJECT:
            return "java.lang.Object";
        case Types.LONGVARBINARY:
            return "[B";
        case Types.LONGVARCHAR:
            return "java.lang.String";
        case Types.NULL:
            // TODO
            return "";
        case Types.NUMERIC:
            return "java.math.BigDecimal";
        case Types.OTHER:
            return "java.lang.Object";
        case Types.REAL:
            return "java.lang.Float";
        case Types.REF:
            // TODO
            return "";
        case Types.SMALLINT:
            return "java.lang.Short";
        case Types.STRUCT:
            return "java.sql.Struct";
        case Types.TIME:
            return "java.sql.Time";
        case Types.TIMESTAMP:
            return "java.sql.Timestamp";
        case Types.TINYINT:
            return "java.lang.Byte";
        case Types.VARBINARY:
            return "[B";
        case Types.VARCHAR:
            return "java.lang.String";
        }
        return "";
    }

    public int getFieldJdbcType(int fieldOrdinal)
    {
        RelDataType type = getFieldNamedType(fieldOrdinal);
        SqlTypeName typeName = type.getSqlTypeName();
        if (typeName == null) {
            return Types.OTHER;
        }
        return typeName.getJdbcOrdinal();
    }

    public String getFieldTypeName(int fieldOrdinal)
    {
        RelDataType type = getFieldNamedType(fieldOrdinal);
        SqlTypeName typeName = type.getSqlTypeName();
        if (typeName == null) {
            return type.toString();
        }
        switch (typeName) {
        case STRUCTURED:
        case DISTINCT:
            return type.getSqlIdentifier().toString();
        case INTERVAL_DAY_TIME:
        case INTERVAL_YEAR_MONTH:
            return type.toString();
        }
        return typeName.name();
    }

    public int getFieldPrecision(int fieldOrdinal)
    {
        RelDataType type = getFieldType(fieldOrdinal);
        return type.getPrecision();
    }

    public int getFieldScale(int fieldOrdinal)
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

    public int getFieldDisplaySize(int column)
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

    public String getFieldCatalogName(int fieldOrdinal)
    {
        return getFieldOrigin(fieldOrdinal, 0);
    }

    public String getFieldSchemaName(int fieldOrdinal)
    {
        return getFieldOrigin(fieldOrdinal, 1);
    }

    public String getFieldTableName(int fieldOrdinal)
    {
        return getFieldOrigin(fieldOrdinal, 2);
    }

    public String getFieldColumnName(int fieldOrdinal)
    {
        return getFieldOrigin(fieldOrdinal, 3);
    }

    private String getFieldOrigin(int fieldOrdinal, int index)
    {
        final List<String> list = fieldOrigins.get(fieldOrdinal - 1);
        if (list == null) {
            return ""; // per JDBC spec: 'Return "" if not applicable'
        }
        if (list.size() < 4) {
            index -= (4 - list.size());
        }
        if (index < 0) {
            return "";
        }
        final String name = list.get(index);
        if (name == null) {
            return "";
        }
        return name;
    }

    public int isFieldNullable(int fieldOrdinal)
    {
        RelDataType type = getFieldType(fieldOrdinal);
        return type.isNullable() ? ResultSetMetaData.columnNullable
            : ResultSetMetaData.columnNoNulls;
    }

    public boolean isFieldAutoIncrement(int fieldOrdinal)
    {
        return false;
    }

    public boolean isFieldCaseSensitive(int fieldOrdinal)
    {
        // TODO
        return false;
    }

    public boolean isFieldSearchable(int fieldOrdinal)
    {
        return true;
    }

    public boolean isFieldSigned(int fieldOrdinal)
    {
        // TODO
        RelDataType type = getFieldType(fieldOrdinal);
        if (SqlTypeUtil.isNumeric(type)) {
            return true;
        } else {
            return false;
        }
    }

    public boolean isFieldCurrency(int fieldOrdinal)
    {
        return false;
    }

    public boolean isFieldReadOnly(int fieldOrdinal)
    {
        return true;
    }

    public boolean isFieldWritable(int fieldOrdinal)
    {
        return false;
    }

    public boolean isFieldDefinitelyWritable(int fieldOrdinal)
    {
        return false;
    }
}

// End FarragoJdbcMetaDataImpl.java
