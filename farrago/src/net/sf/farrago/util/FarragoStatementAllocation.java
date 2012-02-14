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
