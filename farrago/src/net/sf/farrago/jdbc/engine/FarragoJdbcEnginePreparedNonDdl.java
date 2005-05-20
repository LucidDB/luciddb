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

import java.math.*;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.logging.*;

import net.sf.farrago.query.*;
import net.sf.farrago.session.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;


/**
 * FarragoJdbcEnginePreparedNonDdl implements {@link
 * FarragoJdbcEnginePreparedStatement} when the statement is a query or DML.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJdbcEnginePreparedNonDdl
    extends FarragoJdbcEnginePreparedStatement
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoJdbcEnginePreparedNonDdl object.
     *
     * @param connection the connection creating this statement
     *
     * @param stmtContext the underyling prepared FarragoSessionStmtContext
     *
     * @param sql the text of the SQL statement
     */
    FarragoJdbcEnginePreparedNonDdl(
        FarragoJdbcEngineConnection connection,
        FarragoSessionStmtContext stmtContext,
        String sql)
    {
        super(connection, stmtContext, sql);
    }

    //~ Methods ---------------------------------------------------------------

    // implement PreparedStatement
    public boolean execute()
        throws SQLException
    {
        stmtContext.execute();
        return (stmtContext.getResultSet() != null);
    }

    // implement PreparedStatement
    public ResultSet executeQuery()
        throws SQLException
    {
        if (stmtContext.isPreparedDml()) {
            throw new SQLException(ERRMSG_NOT_A_QUERY + sql);
        }
        stmtContext.execute();
        assert (stmtContext.getResultSet() != null);
        return stmtContext.getResultSet();
    }

    // implement PreparedStatement
    public int executeUpdate()
        throws SQLException
    {
        if (!stmtContext.isPreparedDml()) {
            throw new SQLException(ERRMSG_IS_A_QUERY + sql);
        }
        stmtContext.execute();
        assert (stmtContext.getResultSet() == null);
        int count = getUpdateCount();
        if (count == -1) {
            count = 0;
        }
        return count;
    }

    // implement PreparedStatement
    public ResultSetMetaData getMetaData()
        throws SQLException
    {
        if (stmtContext.isPreparedDml()) {
            throw new SQLException(ERRMSG_NOT_A_QUERY + sql);
        }
        try {
            return new FarragoResultSetMetaData(
                stmtContext.getPreparedRowType());
        } catch (Throwable ex) {
            throw FarragoJdbcEngineDriver.newSqlException(ex);
        }
    }

    // implement PreparedStatement
    public ParameterMetaData getParameterMetaData()
        throws SQLException
    {
        try {
            return new FarragoParameterMetaData(
                stmtContext.getPreparedParamType());
        } catch (Throwable ex) {
            throw FarragoJdbcEngineDriver.newSqlException(ex);
        }
    }

    // implement PreparedStatement
    public void clearParameters()
        throws SQLException
    {
        try {
            stmtContext.clearParameters();
        } catch (Throwable ex) {
            throw FarragoJdbcEngineDriver.newSqlException(ex);
        }
    }

    private void setDynamicParam(
        int parameterIndex,
        Object obj)
        throws SQLException
    {
        try {
            stmtContext.setDynamicParam(parameterIndex - 1, obj);
        } catch (Throwable ex) {
            throw FarragoJdbcEngineDriver.newSqlException(ex);
        }
    }

    // implement PreparedStatement
    public void setNull(
        int parameterIndex,
        int sqlType)
        throws SQLException
    {
        // REVIEW:  use sqlType?
        setDynamicParam(parameterIndex, null);
    }

    // implement PreparedStatement
    public void setBoolean(
        int parameterIndex,
        boolean x)
        throws SQLException
    {
        setDynamicParam(
            parameterIndex,
            Boolean.valueOf(x));
    }

    // implement PreparedStatement
    public void setByte(
        int parameterIndex,
        byte x)
        throws SQLException
    {
        setDynamicParam(
            parameterIndex,
            new Byte(x));
    }

    // implement PreparedStatement
    public void setShort(
        int parameterIndex,
        short x)
        throws SQLException
    {
        setDynamicParam(
            parameterIndex,
            new Short(x));
    }

    // implement PreparedStatement
    public void setInt(
        int parameterIndex,
        int x)
        throws SQLException
    {
        setDynamicParam(
            parameterIndex,
            new Integer(x));
    }

    // implement PreparedStatement
    public void setLong(
        int parameterIndex,
        long x)
        throws SQLException
    {
        setDynamicParam(
            parameterIndex,
            new Long(x));
    }

    // implement PreparedStatement
    public void setFloat(
        int parameterIndex,
        float x)
        throws SQLException
    {
        setDynamicParam(
            parameterIndex,
            new Float(x));
    }

    // implement PreparedStatement
    public void setDouble(
        int parameterIndex,
        double x)
        throws SQLException
    {
        setDynamicParam(
            parameterIndex,
            new Double(x));
    }

    // implement PreparedStatement
    public void setBigDecimal(
        int parameterIndex,
        BigDecimal x)
        throws SQLException
    {
        setDynamicParam(parameterIndex, x);
    }

    // implement PreparedStatement
    public void setString(
        int parameterIndex,
        String x)
        throws SQLException
    {
        setDynamicParam(parameterIndex, x);
    }

    // implement PreparedStatement
    public void setBytes(
        int parameterIndex,
        byte [] x)
        throws SQLException
    {
        setDynamicParam(parameterIndex, x);
    }

    // implement PreparedStatement
    public void setDate(
        int parameterIndex,
        Date x)
        throws SQLException
    {
        setDynamicParam(parameterIndex, x);
    }

    // implement PreparedStatement
    public void setTime(
        int parameterIndex,
        Time x)
        throws SQLException
    {
        setDynamicParam(parameterIndex, x);
    }

    // implement PreparedStatement
    public void setTimestamp(
        int parameterIndex,
        Timestamp x)
        throws SQLException
    {
        setDynamicParam(parameterIndex, x);
    }

    // implement PreparedStatement
    public void setObject(
        int parameterIndex,
        Object x)
        throws SQLException
    {
        setDynamicParam(parameterIndex, x);
    }
}


// End FarragoJdbcEnginePreparedNonDdl.java
