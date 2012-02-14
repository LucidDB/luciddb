/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
