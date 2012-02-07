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
package net.sf.farrago.jdbc.engine;

import java.io.*;

import java.math.*;

import java.net.*;

import java.sql.*;
import java.sql.Date;

import java.util.*;

import net.sf.farrago.session.*;

import org.eigenbase.jdbc4.*;


/**
 * FarragoJdbcEnginePreparedStatement is an abstract base for Farrago
 * implementations of {@link java.sql.PreparedStatement}. Subclasses define
 * details of preparation for DDL, DML, and queries.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoJdbcEnginePreparedStatement
    extends FarragoJdbcEngineStatement
    implements PreparedStatement
{
    //~ Static fields/initializers ---------------------------------------------

    protected static final String ERRMSG_ALREADY_PREPARED =
        "Statement already prepared";

    //~ Instance fields --------------------------------------------------------

    protected String sql;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoJdbcEnginePreparedStatement object.
     *
     * @param connection the connection creating this statement
     * @param stmtContext underlying FarragoSessionStmtContext
     * @param sql the text of the SQL statement
     */
    protected FarragoJdbcEnginePreparedStatement(
        FarragoJdbcEngineConnection connection,
        FarragoSessionStmtContext stmtContext,
        String sql)
    {
        super(connection, stmtContext);
        this.sql = sql;
    }

    //~ Methods ----------------------------------------------------------------

    // implement PreparedStatement
    public ResultSetMetaData getMetaData()
        throws SQLException
    {
        throw new SQLException(ERRMSG_NOT_A_QUERY + sql);
    }

    // implement PreparedStatement
    public ParameterMetaData getParameterMetaData()
        throws SQLException
    {
        throw new SQLException(ERRMSG_NOT_A_QUERY + sql);
    }

    // implement PreparedStatement
    public void clearParameters()
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement
    public void setNull(
        int parameterIndex,
        int sqlType)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement
    public void setBoolean(
        int parameterIndex,
        boolean x)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement
    public void setByte(
        int parameterIndex,
        byte x)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement
    public void setShort(
        int parameterIndex,
        short x)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement
    public void setInt(
        int parameterIndex,
        int x)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement
    public void setLong(
        int parameterIndex,
        long x)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement
    public void setFloat(
        int parameterIndex,
        float x)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement
    public void setDouble(
        int parameterIndex,
        double x)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement
    public void setBigDecimal(
        int parameterIndex,
        BigDecimal x)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement
    public void setString(
        int parameterIndex,
        String x)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement
    public void setBytes(
        int parameterIndex,
        byte [] x)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement
    public void setDate(
        int parameterIndex,
        Date x)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement
    public void setTime(
        int parameterIndex,
        Time x)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement
    public void setTimestamp(
        int parameterIndex,
        Timestamp x)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement
    public void setAsciiStream(
        int parameterIndex,
        InputStream x,
        int length)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement
    public void setUnicodeStream(
        int parameterIndex,
        InputStream x,
        int length)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement
    public void setBinaryStream(
        int parameterIndex,
        InputStream x,
        int length)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement
    public void setObject(
        int parameterIndex,
        Object x,
        int targetSqlType,
        int scale)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement
    public void setObject(
        int parameterIndex,
        Object x,
        int targetSqlType)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement
    public void setObject(
        int parameterIndex,
        Object x)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement
    public void setCharacterStream(
        int parameterIndex,
        Reader reader,
        int length)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement
    public void setRef(
        int i,
        Ref x)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement
    public void setBlob(
        int i,
        Blob x)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement
    public void setClob(
        int i,
        Clob x)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement
    public void setArray(
        int i,
        Array x)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement
    public void setDate(
        int parameterIndex,
        Date x,
        Calendar cal)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement
    public void setTime(
        int parameterIndex,
        Time x,
        Calendar cal)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement
    public void setTimestamp(
        int parameterIndex,
        Timestamp x,
        Calendar cal)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement
    public void setNull(
        int paramIndex,
        int sqlType,
        String typeName)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement PreparedStatement
    public void setURL(
        int parameterIndex,
        URL x)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement Statement:  disallow for PreparedStatements
    public void addBatch()
        throws SQLException
    {
        throw new SQLException(ERRMSG_ALREADY_PREPARED);
    }

    // implement Statement:  disallow for PreparedStatements
    public void clearBatch()
        throws SQLException
    {
        throw new SQLException(ERRMSG_ALREADY_PREPARED);
    }

    // implement Statement:  disallow for PreparedStatements
    public void setEscapeProcessing(boolean enable)
        throws SQLException
    {
        throw new SQLException(ERRMSG_ALREADY_PREPARED);
    }

    // implement Statement:  disallow for PreparedStatements
    public void addBatch(String sql)
        throws SQLException
    {
        throw new SQLException(ERRMSG_ALREADY_PREPARED);
    }

    // implement Statement:  disallow for PreparedStatements
    public int [] executeBatch()
        throws SQLException
    {
        throw new SQLException(ERRMSG_ALREADY_PREPARED);
    }

    // implement Statement:  disallow for PreparedStatements
    public ResultSet executeQuery(String sql)
        throws SQLException
    {
        throw new SQLException(ERRMSG_ALREADY_PREPARED);
    }

    // implement Statement:  disallow for PreparedStatements
    public boolean execute(String sql)
        throws SQLException
    {
        throw new SQLException(ERRMSG_ALREADY_PREPARED);
    }

    // implement Statement:  disallow for PreparedStatements
    public boolean execute(
        String sql,
        int autoGeneratedKeys)
        throws SQLException
    {
        throw new SQLException(ERRMSG_ALREADY_PREPARED);
    }

    // implement Statement:  disallow for PreparedStatements
    public boolean execute(
        String sql,
        int [] columnIndexes)
        throws SQLException
    {
        throw new SQLException(ERRMSG_ALREADY_PREPARED);
    }

    // implement Statement:  disallow for PreparedStatements
    public boolean execute(
        String sql,
        String [] columnNames)
        throws SQLException
    {
        throw new SQLException(ERRMSG_ALREADY_PREPARED);
    }

    // implement Statement:  disallow for PreparedStatements
    public int executeUpdate(String sql)
        throws SQLException
    {
        throw new SQLException(ERRMSG_ALREADY_PREPARED);
    }

    // implement Statement:  disallow for PreparedStatements
    public int executeUpdate(
        String sql,
        int autoGeneratedKeys)
        throws SQLException
    {
        throw new SQLException(ERRMSG_ALREADY_PREPARED);
    }

    // implement Statement:  disallow for PreparedStatements
    public int executeUpdate(
        String sql,
        int [] columnIndexes)
        throws SQLException
    {
        throw new SQLException(ERRMSG_ALREADY_PREPARED);
    }

    // implement Statement:  disallow for PreparedStatements
    public int executeUpdate(
        String sql,
        String [] columnNames)
        throws SQLException
    {
        throw new SQLException(ERRMSG_ALREADY_PREPARED);
    }

    //
    // begin JDBC 4 methods
    //

    // implement PreparedStatement
    public void setCharacterStream(int i, Reader reader)
        throws SQLException
    {
        throw new UnsupportedOperationException("setCharacterStream");
    }

    // implement PreparedStatement
    public void setCharacterStream(int i, Reader reader, long len)
        throws SQLException
    {
        throw new UnsupportedOperationException("setCharacterStream");
    }

    // implement PreparedStatement
    public void setNCharacterStream(int i, Reader reader)
        throws SQLException
    {
        throw new UnsupportedOperationException("setNCharacterStream");
    }

    // implement PreparedStatement
    public void setNCharacterStream(int i, Reader reader, long len)
        throws SQLException
    {
        throw new UnsupportedOperationException("setNCharacterStream");
    }

    // implement PreparedStatement
    public void setClob(int i, Reader reader)
        throws SQLException
    {
        throw new UnsupportedOperationException("setClob");
    }

    // implement PreparedStatement
    public void setClob(int i, Reader reader, long len)
        throws SQLException
    {
        throw new UnsupportedOperationException("setClob");
    }

    // implement PreparedStatement
    public void setNClob(int i, Reader reader)
        throws SQLException
    {
        throw new UnsupportedOperationException("setNClob");
    }

    // implement PreparedStatement
    public void setNClob(int i, NClob nclob)
        throws SQLException
    {
        throw new UnsupportedOperationException("setNClob");
    }

    // implement PreparedStatement
    public void setNClob(int i, Reader reader, long len)
        throws SQLException
    {
        throw new UnsupportedOperationException("setNClob");
    }

    // implement PreparedStatement
    public void setBlob(int i, InputStream inputStream)
        throws SQLException
    {
        throw new UnsupportedOperationException("setBlob");
    }

    // implement PreparedStatement
    public void setBlob(int i, InputStream inputStream, long len)
        throws SQLException
    {
        throw new UnsupportedOperationException("setBlob");
    }

    // implement PreparedStatement
    public void setBinaryStream(int i, InputStream inputStream)
        throws SQLException
    {
        throw new UnsupportedOperationException("setBinaryStream");
    }

    // implement PreparedStatement
    public void setBinaryStream(int i, InputStream inputStream, long len)
        throws SQLException
    {
        throw new UnsupportedOperationException("setBinaryStream");
    }

    // implement PreparedStatement
    public void setAsciiStream(int i, InputStream inputStream)
        throws SQLException
    {
        throw new UnsupportedOperationException("setAsciiStream");
    }

    // implement PreparedStatement
    public void setAsciiStream(int i, InputStream inputStream, long len)
        throws SQLException
    {
        throw new UnsupportedOperationException("setAsciiStream");
    }

    // implement PreparedStatement
    public void setSQLXML(int i, SQLXML sqlxml)
        throws SQLException
    {
        throw new UnsupportedOperationException("setSQLXML");
    }

    // implement PreparedStatement
    public void setNString(int i, String nstring)
        throws SQLException
    {
        throw new UnsupportedOperationException("setNString");
    }

    // implement PreparedStatement
    public void setRowId(int i, RowId rowid)
        throws SQLException
    {
        throw new UnsupportedOperationException("setRowId");
    }

    //
    // end JDBC 4 methods
    //
}

// End FarragoJdbcEnginePreparedStatement.java
