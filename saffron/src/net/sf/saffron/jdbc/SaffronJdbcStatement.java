/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.saffron.jdbc;

import net.sf.saffron.oj.stmt.*;

import java.sql.*;


/**
 * A <code>SaffronJdbcStatement</code> is a JDBC {@link Statement} on a
 * Saffron database.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since Mar 19, 2003
 */
public class SaffronJdbcStatement implements Statement
{
    //~ Instance fields -------------------------------------------------------

    protected ResultSet resultSet;
    private final SaffronJdbcConnection connection;

    //~ Constructors ----------------------------------------------------------

    protected SaffronJdbcStatement(SaffronJdbcConnection connection)
    {
        this.connection = connection;
    }

    //~ Methods ---------------------------------------------------------------

    public Connection getConnection() throws SQLException
    {
        return connection;
    }

    public void setCursorName(String name) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void setEscapeProcessing(boolean enable) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void setFetchDirection(int direction) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public int getFetchDirection() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void setFetchSize(int rows) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public int getFetchSize() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public ResultSet getGeneratedKeys() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void setMaxFieldSize(int max) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public int getMaxFieldSize() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void setMaxRows(int max) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public int getMaxRows() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public boolean getMoreResults() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public boolean getMoreResults(int current) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void setQueryTimeout(int seconds) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public int getQueryTimeout() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public ResultSet getResultSet() throws SQLException
    {
        return resultSet;
    }

    public int getResultSetConcurrency() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public int getResultSetHoldability() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public int getResultSetType() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public int getUpdateCount() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public SQLWarning getWarnings() throws SQLException
    {
        return null;
    }

    public void addBatch(String sql) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void cancel() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void clearBatch() throws SQLException
    {
    }

    public void clearWarnings() throws SQLException
    {
    }

    public void close() throws SQLException
    {
        if (resultSet != null) {
            resultSet.close();
            resultSet = null;
        }
    }

    public boolean execute(String sql) throws SQLException
    {
        OJStatement statement = new OJStatement(connection.saffronConnection);
        resultSet = statement.executeSql(sql);
        return true;
    }

    public boolean execute(String sql,int autoGeneratedKeys)
        throws SQLException
    {
        if (autoGeneratedKeys != NO_GENERATED_KEYS) {
            throw new UnsupportedOperationException();
        }
        return execute(sql);
    }

    public boolean execute(String sql,int [] columnIndexes)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public boolean execute(String sql,String [] columnNames)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public int [] executeBatch() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public ResultSet executeQuery(String sql) throws SQLException
    {
        if (execute(sql)) {
            return resultSet;
        } else {
            // TODO:  fail before execution for this case
            throw new SQLException("Not a SELECT statement:  " + sql);
        }
    }

    public int executeUpdate(String sql) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public int executeUpdate(String sql,int autoGeneratedKeys)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public int executeUpdate(String sql,int [] columnIndexes)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public int executeUpdate(String sql,String [] columnNames)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }
}


// End SaffronJdbcStatement.java
