/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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
package net.sf.farrago.util;

import java.sql.*;

import org.eigenbase.runtime.*;

/**
 * FarragoStatementAllocation takes care of closing a JDBC Statement (and its
 * associated ResultSet if any).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoStatementAllocation
    implements FarragoAllocation, ResultSetProvider
{
    //~ Instance fields --------------------------------------------------------

    private Connection conn;
    private Statement stmt;
    private ResultSet resultSet;
    private String sql;

    //~ Constructors -----------------------------------------------------------

    public FarragoStatementAllocation(Statement stmt)
    {
        this.stmt = stmt;
    }

    public FarragoStatementAllocation(Connection conn, Statement stmt)
    {
        this.conn = conn;
        this.stmt = stmt;
    }

    //~ Methods ----------------------------------------------------------------

    public void setResultSet(ResultSet resultSet)
    {
        this.resultSet = resultSet;
    }

    public void setSql(String sql)
    {
        this.sql = sql;
    }

    // implement FarragoAllocation
    public void closeAllocation()
    {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            stmt.close();
        } catch (SQLException ex) {
            // REVIEW:  is it OK to suppress?  Should at least trace.
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    // REVIEW:  is it OK to suppress?  Should at least trace.
                }
            }
        }
    }

    public Statement getStatement()
    {
        return stmt;
    }

    // implement ResultSetProvider
    public ResultSet getResultSet()
        throws SQLException
    {
        if (resultSet == null) {
            if (sql != null) {
                resultSet = stmt.executeQuery(sql);
            }
        }
        return resultSet;
    }
}

// End FarragoStatementAllocation.java
