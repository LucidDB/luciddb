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

import java.sql.*;

import net.sf.farrago.session.*;


/**
 * FarragoJdbcEnginePreparedDdl implements {@link
 * FarragoJdbcEnginePreparedStatement} when the statement is DDL.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJdbcEnginePreparedDdl
    extends FarragoJdbcEnginePreparedStatement
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoJdbcEnginePreparedDdl object.
     *
     * @param connection the connection creating this statement
     * @param stmtContext the underyling FarragoSessionStmtContext (unprepared)
     * @param sql the text of the DDL statement
     */
    public FarragoJdbcEnginePreparedDdl(
        FarragoJdbcEngineConnection connection,
        FarragoSessionStmtContext stmtContext,
        String sql)
    {
        super(connection, stmtContext, sql);
    }

    //~ Methods ----------------------------------------------------------------

    // implement PreparedStatement
    public boolean execute()
        throws SQLException
    {
        executeDdl();
        return false;
    }

    // implement PreparedStatement
    public ResultSet executeQuery()
        throws SQLException
    {
        throw new SQLException(ERRMSG_NOT_A_QUERY + sql);
    }

    // implement PreparedStatement
    public int executeUpdate()
        throws SQLException
    {
        executeDdl();
        return 0;
    }

    private void executeDdl()
        throws SQLException
    {
        // REVIEW:  need to reset any context (like current schema) as it was
        // at the time of prepare?
        // NOTE:  We fib and say this is direct execution.
        try {
            stmtContext.prepare(sql, true);
            assert (!stmtContext.isPrepared());
        } catch (Throwable ex) {
            throw FarragoJdbcEngineDriver.newSqlException(ex);
        }
    }
}

// End FarragoJdbcEnginePreparedDdl.java
