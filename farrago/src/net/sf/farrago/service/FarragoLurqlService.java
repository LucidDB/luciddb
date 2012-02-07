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
package net.sf.farrago.service;

import java.sql.*;
import java.util.*;
import java.util.logging.*;

import javax.jmi.reflect.*;
import javax.sql.*;

import org.eigenbase.jmi.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.util.*;

/**
 * Service to enable clients to get information about repository objects via
 * LURQL queries withou needing any of the underlying server classes.
 * @author chard
 */
public class FarragoLurqlService
{
    protected DataSource dataSource;
    protected Logger tracer;

    /**
     * Creates an instance of the FarragoLurqlService for a given server
     * (represented by a DataSource used to create connections) and a Logger for
     * tracing.
     * @param dataSource DataSource to be used for connecting to the server
     * you want to query
     * @param tracer Logger for trace messages
     */
    public FarragoLurqlService(
        DataSource dataSource,
        Logger tracer)
    {
        this.dataSource = dataSource;
        this.tracer = tracer;
    }

    /**
     * Executes a specified LURQL query against the server's repository and
     * instantiates the results in the specified target package.
     * @param lurqlQuery String containing a LURQL query
     * @param target RefPackage where the resulting objects will reside
     * @return Collection of RefBaseObject objects representing the result of
     * the LURQL query
     */
    public Collection<RefBaseObject> executeLurql(
        String lurqlQuery,
        RefPackage target)
    {
        Connection c = null;
        Statement stmt = null;
        ResultSet rs = null;
        Collection<RefBaseObject> result = null;
        try {
            c = dataSource.getConnection();
            stmt = c.createStatement();
            rs = stmt.executeQuery(constructQuery(lurqlQuery));
            String xmiString = StringChunker.readChunks(rs, 2);
            result = JmiObjUtil.importFromXmiString(
                target,
                xmiString);
            rs.close();
            rs = null;
            stmt.close();
            stmt = null;
            c.close();
            c = null;
        } catch (Throwable se) {
            tracer.warning("Error executing LURQL query '" + lurqlQuery + "'");
            tracer.warning("Stack trace:\n" + Util.getStackTrace(se));
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                rs = null;
                if (stmt != null) {
                    stmt.close();
                }
                if (c != null) {
                    c.close();
                }
            } catch (SQLException se) {
            } finally {
                stmt = null;
                c = null;
            }
        }
        return result;
    }

    /**
     * Wraps a LURQL query string into a call to the LURQL-XMI UDX. Note that
     * we currently swallow all newline characters because of bug FRG-418
     * (newlines get encoded as Unicode literals, processed improperly).
     * @param lurqlQuery String containing LURQL query
     * @return String containing the appropriate UDX call wrapping the LURQL
     */
    private String constructQuery(String lurqlQuery)
    {
        final SqlBuilder sqlBuilder = new SqlBuilder(SqlDialect.EIGENBASE);
        sqlBuilder.append("SELECT * FROM TABLE(SYS_BOOT.MGMT.GET_LURQL_XMI(")
            .literal(lurqlQuery.replaceAll("\n", " "))
            .append("))");
        return sqlBuilder.getSql();
    }
}

// End FarragoLurqlService.java
