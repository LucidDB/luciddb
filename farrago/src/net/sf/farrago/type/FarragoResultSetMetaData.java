/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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


/**
 * FarragoResultSetMetaData implements the ResultSetMetaData interface by
 * reading a Farrago type descriptor.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoResultSetMetaData extends FarragoJdbcMetaDataImpl
    implements ResultSetMetaData
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoResultSetMetaData object.
     *
     * @param rowType type info to return
     */
    public FarragoResultSetMetaData(RelDataType rowType)
    {
        super(rowType);
    }

    //~ Methods ---------------------------------------------------------------

    // implement ResultSetMetaData
    public boolean isAutoIncrement(int column)
        throws SQLException
    {
        return false;
    }

    // implement ResultSetMetaData
    public boolean isCaseSensitive(int column)
        throws SQLException
    {
        // TODO
        return false;
    }

    // implement ResultSetMetaData
    public String getCatalogName(int column)
        throws SQLException
    {
        // TODO
        return "";
    }

    // implement ResultSetMetaData
    public String getColumnClassName(int column)
        throws SQLException
    {
        return getFieldClassName(column);
    }

    // implement ResultSetMetaData
    public int getColumnCount()
        throws SQLException
    {
        return getFieldCount();
    }

    // implement ResultSetMetaData
    public int getColumnDisplaySize(int column)
        throws SQLException
    {
        int precision = getPrecision(column);
        int type = getColumnType(column);
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

    // implement ResultSetMetaData
    public String getColumnLabel(int column)
        throws SQLException
    {
        return getColumnName(column);
    }

    // implement ResultSetMetaData
    public String getColumnName(int column)
        throws SQLException
    {
        return getFieldName(column);
    }

    // implement ResultSetMetaData
    public int getColumnType(int column)
        throws SQLException
    {
        return getFieldJdbcType(column);
    }

    // implement ResultSetMetaData
    public String getColumnTypeName(int column)
        throws SQLException
    {
        return getFieldTypeName(column);
    }

    // implement ResultSetMetaData
    public boolean isCurrency(int column)
        throws SQLException
    {
        return false;
    }

    // implement ResultSetMetaData
    public boolean isDefinitelyWritable(int column)
        throws SQLException
    {
        return false;
    }

    // implement ResultSetMetaData
    public int isNullable(int column)
        throws SQLException
    {
        return isFieldNullable(column);
    }

    // implement ResultSetMetaData
    public int getPrecision(int column)
        throws SQLException
    {
        return getFieldPrecision(column);
    }

    // implement ResultSetMetaData
    public int getScale(int column)
        throws SQLException
    {
        return getFieldScale(column);
    }

    // implement ResultSetMetaData
    public boolean isReadOnly(int column)
        throws SQLException
    {
        return true;
    }

    // implement ResultSetMetaData
    public String getSchemaName(int column)
        throws SQLException
    {
        // TODO
        return "";
    }

    // implement ResultSetMetaData
    public boolean isSearchable(int column)
        throws SQLException
    {
        return true;
    }

    // implement ResultSetMetaData
    public boolean isSigned(int column)
        throws SQLException
    {
        return isFieldSigned(column);
    }

    // implement ResultSetMetaData
    public String getTableName(int column)
        throws SQLException
    {
        // TODO
        return "";
    }

    // implement ResultSetMetaData
    public boolean isWritable(int column)
        throws SQLException
    {
        return false;
    }
}


// End FarragoResultSetMetaData.java
