/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
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

package net.sf.farrago.jdbc;

import net.sf.farrago.query.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import java.sql.*;
import java.math.*;
import java.util.*;
import java.util.logging.*;

import java.sql.Date;

/**
 * FarragoJdbcPreparedNonDdl implements FarragoJdbcPreparedStatement when the
 * statement is a query or DML.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJdbcPreparedNonDdl extends FarragoJdbcPreparedStatement
{
    private static Logger tracer =
        TraceUtil.getClassTrace(FarragoJdbcPreparedNonDdl.class);

    private FarragoExecutableStmt executableStmt;

    private FarragoCompoundAllocation allocations;
    
    /**
     * Creates a new FarragoJdbcPreparedNonDdl object.
     *
     * @param connection the connection creating this statement
     *
     * @param sql the text of the SQL statement
     *
     * @param executableStmt the prepared FarragoExecutableStmt
     */
    FarragoJdbcPreparedNonDdl(
        FarragoJdbcConnection connection,
        String sql,
        FarragoExecutableStmt executableStmt,
        FarragoCompoundAllocation allocations)
    {
        super(connection,sql);
        this.executableStmt = executableStmt;
        this.allocations = allocations;

        dynamicParamValues = new Object[
            executableStmt.getDynamicParamRowType().getFieldCount()];
    }

    private void traceExecute()
    {
        if (!tracer.isLoggable(Level.FINE)) {
            return;
        }
        tracer.fine(sql);
        if (!tracer.isLoggable(Level.FINER)) {
            return;
        }
        for (int i = 0; i < dynamicParamValues.length; ++i) {
            tracer.finer("?" + (i + 1) + " = [" + dynamicParamValues[i] + "]");
        }
    }
    
    // implement PreparedStatement
    public boolean execute() throws SQLException
    {
        traceExecute();
        executeImpl(executableStmt,null);
        return (resultSet != null);
    }

    // implement PreparedStatement
    public ResultSet executeQuery() throws SQLException
    {
        if (executableStmt.isDml()) {
            throw new SQLException(ERRMSG_NOT_A_QUERY + sql);
        }
        traceExecute();
        executeImpl(executableStmt,null);
        assert(resultSet != null);
        return resultSet;
    }

    // implement PreparedStatement
    public int executeUpdate() throws SQLException
    {
        if (!executableStmt.isDml()) {
            throw new SQLException(ERRMSG_IS_A_QUERY + sql);
        }
        traceExecute();
        executeImpl(executableStmt,null);
        assert(resultSet == null);
        int count = getUpdateCount();
        if (count == -1) {
            count = 0;
        }
        if (tracer.isLoggable(Level.FINE)) {
            tracer.fine("Update count = " + count);
        }
        return count;
    }

    // implement Statement
    public void close() throws SQLException
    {
        allocations.closeAllocation();
        executableStmt = null;
        super.close();
    }
    
    // implement PreparedStatement
    public ResultSetMetaData getMetaData() throws SQLException
    {
        if (executableStmt.isDml()) {
            throw new SQLException(ERRMSG_NOT_A_QUERY + sql);
        }
        return new FarragoResultSetMetaData(executableStmt.getRowType());
    }

    // implement PreparedStatement
    public ParameterMetaData getParameterMetaData() throws SQLException
    {
        return new FarragoParameterMetaData(
            executableStmt.getDynamicParamRowType());
    }

    // implement PreparedStatement
    public void clearParameters() throws SQLException
    {
        Arrays.fill(dynamicParamValues,null);
    }

    private void setDynamicParam(int parameterIndex,Object x)
    {
        // TODO:  type/null checking
        dynamicParamValues[parameterIndex - 1] = x;
    }

    // implement PreparedStatement
    public void setNull(int parameterIndex, int sqlType) throws SQLException
    {
        // REVIEW:  use sqlType?
        setDynamicParam(parameterIndex,null);
    }

    // implement PreparedStatement
    public void setBoolean(int parameterIndex,boolean x) throws SQLException
    {
        setDynamicParam(parameterIndex,Boolean.valueOf(x));
    }

    // implement PreparedStatement
    public void setByte(int parameterIndex, byte x) throws SQLException
    {
        setDynamicParam(parameterIndex,new Byte(x));
    }

    // implement PreparedStatement
    public void setShort(int parameterIndex, short x) throws SQLException
    {
        setDynamicParam(parameterIndex,new Short(x));
    }

    // implement PreparedStatement
    public void setInt(int parameterIndex, int x) throws SQLException
    {
        setDynamicParam(parameterIndex,new Integer(x));
    }

    // implement PreparedStatement
    public void setLong(int parameterIndex, long x) throws SQLException
    {
        setDynamicParam(parameterIndex,new Long(x));
    }

    // implement PreparedStatement
    public void setFloat(int parameterIndex, float x) throws SQLException
    {
        setDynamicParam(parameterIndex,new Float(x));
    }

    // implement PreparedStatement
    public void setDouble(int parameterIndex, double x) throws SQLException
    {
        setDynamicParam(parameterIndex,new Double(x));
    }

    // implement PreparedStatement
    public void setBigDecimal(
        int parameterIndex,BigDecimal x) throws SQLException
    {
        setDynamicParam(parameterIndex,x);
    }

    // implement PreparedStatement
    public void setString(int parameterIndex, String x) throws SQLException
    {
        setDynamicParam(parameterIndex,x);
    }

    // implement PreparedStatement
    public void setBytes(int parameterIndex, byte [] x) throws SQLException
    {
        setDynamicParam(parameterIndex,x);
    }

    // implement PreparedStatement
    public void setDate(int parameterIndex,Date x) throws SQLException
    {
        setDynamicParam(parameterIndex,x);
    }

    // implement PreparedStatement
    public void setTime(int parameterIndex,Time x) throws SQLException
    {
        setDynamicParam(parameterIndex,x);
    }

    // implement PreparedStatement
    public void setTimestamp(
        int parameterIndex,Timestamp x) throws SQLException
    {
        setDynamicParam(parameterIndex,x);
    }

    // implement PreparedStatement
    public void setObject(int parameterIndex, Object x) throws SQLException
    {
        setDynamicParam(parameterIndex,x);
    }
}

// End FarragoJdbcPreparedNonDdl.java
