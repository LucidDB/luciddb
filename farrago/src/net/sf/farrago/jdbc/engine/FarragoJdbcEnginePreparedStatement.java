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
package net.sf.farrago.jdbc.engine;

import java.io.*;
import java.math.*;
import java.net.*;
import java.sql.*;
import java.sql.Date;
import java.util.*;

import net.sf.farrago.session.*;


/**
 * FarragoJdbcEnginePreparedStatement is an abstract base for Farrago
 * implementations of {@link java.sql.PreparedStatement}.  Subclasses define
 * details of preparation for DDL, DML, and queries.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoJdbcEnginePreparedStatement
    extends FarragoJdbcEngineStatement implements PreparedStatement
{
    //~ Static fields/initializers --------------------------------------------

    protected static final String ERRMSG_ALREADY_PREPARED =
        "Statement already prepared";

    //~ Instance fields -------------------------------------------------------

    protected String sql;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoJdbcEnginePreparedStatement object.
     *
     * @param connection the connection creating this statement
     *
     * @param stmtContext underlying FarragoSessionStmtContext
     *
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

    //~ Methods ---------------------------------------------------------------

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
}


// End FarragoJdbcEnginePreparedStatement.java
