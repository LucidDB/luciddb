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
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoJdbcEnginePreparedDdl object.
     *
     * @param connection the connection creating this statement
     *
     * @param stmtContext the underyling FarragoSessionStmtContext (unprepared)
     *
     * @param sql the text of the DDL statement
     */
    FarragoJdbcEnginePreparedDdl(
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
