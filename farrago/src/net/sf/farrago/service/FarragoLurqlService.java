/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2011 The Eigenbase Project
// Copyright (C) 2011 SQLstream, Inc.
// Copyright (C) 2011 Dynamo BI Corporation
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
