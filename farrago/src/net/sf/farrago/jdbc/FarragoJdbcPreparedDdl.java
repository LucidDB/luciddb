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

import java.sql.*;

/**
 * FarragoJdbcPreparedDdl implements FarragoJdbcPreparedStatement when the
 * statement is DDL.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJdbcPreparedDdl extends FarragoJdbcPreparedStatement
{
    /**
     * Creates a new FarragoJdbcPreparedDdl object.
     *
     * @param connection the connection creating this statement
     *
     * @param sql the text of the DDL statement
     */
    FarragoJdbcPreparedDdl(FarragoJdbcConnection connection,String sql)
    {
        super(connection,sql);
    }
    
    // implement PreparedStatement
    public boolean execute() throws SQLException
    {
        executeDdl();
        return false;
    }

    // implement PreparedStatement
    public ResultSet executeQuery() throws SQLException
    {
        throw new SQLException(ERRMSG_NOT_A_QUERY + sql);
    }

    // implement PreparedStatement
    public int executeUpdate() throws SQLException
    {
        executeDdl();
        return 0;
    }

    private void executeDdl() throws SQLException
    {
        // REVIEW:  need to reset any context (like current schema) as it was
        // at the time of prepare?

        // NOTE:  We fib and say this is direct execution.
        FarragoExecutableStmt stmt =
            farragoConnection.prepareImpl(sql,null,true,null);
        assert(stmt == null);
    }
}

// End FarragoJdbcPreparedDdl.java
