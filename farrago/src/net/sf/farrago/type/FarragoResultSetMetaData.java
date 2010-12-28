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

import org.eigenbase.reltype.*;


/**
 * FarragoResultSetMetaData implements the ResultSetMetaData interface by
 * reading a Farrago type descriptor.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoResultSetMetaData
    extends FarragoJdbcMetaDataImpl
    implements ResultSetMetaData
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoResultSetMetaData object.
     *
     * @param rowType type info to return
     * @param fieldOrigins Origin of each field in column of catalog object
     */
    public FarragoResultSetMetaData(
        RelDataType rowType,
        List<List<String>> fieldOrigins)
    {
        super(rowType, fieldOrigins);
    }

    //~ Methods ----------------------------------------------------------------

    // implement ResultSetMetaData
    public boolean isAutoIncrement(int column)
        throws SQLException
    {
        return isFieldAutoIncrement(column);
    }

    // implement ResultSetMetaData
    public boolean isCaseSensitive(int column)
        throws SQLException
    {
        return isFieldCaseSensitive(column);
    }

    // implement ResultSetMetaData
    public String getCatalogName(int column)
        throws SQLException
    {
        return getFieldCatalogName(column);
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
        return getFieldDisplaySize(column);
    }

    // implement ResultSetMetaData
    public String getColumnLabel(int column)
        throws SQLException
    {
        return getFieldName(column);
    }

    // implement ResultSetMetaData
    public String getColumnName(int column)
        throws SQLException
    {
        if (false) {
            // To adhere to the JDBC spec, this method should return the
            // name of the column this field is based on, or "" if it is based
            // on an expression. But client apps such as sqlline expect
            // otherwise.
            return getFieldColumnName(column);
        }
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
        return isFieldCurrency(column);
    }

    // implement ResultSetMetaData
    public boolean isDefinitelyWritable(int column)
        throws SQLException
    {
        return isFieldDefinitelyWritable(column);
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
        return isFieldReadOnly(column);
    }

    // implement ResultSetMetaData
    public String getSchemaName(int column)
        throws SQLException
    {
        return getFieldSchemaName(column);
    }

    // implement ResultSetMetaData
    public boolean isSearchable(int column)
        throws SQLException
    {
        return isFieldSearchable(column);
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
        return getFieldTableName(column);
    }

    // implement ResultSetMetaData
    public boolean isWritable(int column)
        throws SQLException
    {
        return isFieldWritable(column);
    }
}

// End FarragoResultSetMetaData.java
