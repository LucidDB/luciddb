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
 * LURQL queries without needing any of the underlying server classes.
 * @author chard
 */
public class FarragoLurqlService extends FarragoService
{
    private JmiJsonUtil jmiJsonUtil = null;

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
        this(dataSource, tracer, null);
    }

    public FarragoLurqlService(
        DataSource dataSource,
        Logger tracer,
        boolean reusingConnection)
    {
        this(dataSource, tracer, reusingConnection, null);
    }

    public FarragoLurqlService(
        DataSource dataSource,
        Logger tracer,
        JmiJsonUtil jmiJsonUtil)
    {
        this(dataSource, tracer, false, jmiJsonUtil);
    }

    public FarragoLurqlService(
        DataSource dataSource,
        Logger tracer,
        boolean reusingConnection,
        JmiJsonUtil jmiJsonUtil)
    {
        super(dataSource, tracer, reusingConnection);
        this.jmiJsonUtil = jmiJsonUtil;
    }

    public JmiJsonUtil getJmiJsonUtil()
    {
        return jmiJsonUtil;
    }

    protected Collection<RefBaseObject> parseInterchange(
        RefPackage target,
        String interchangeString,
        InterchangeFormat format)
    {
        Collection<RefBaseObject> result = null;
        switch (format) {
        case XMI:
            result =
                JmiObjUtil.importFromXmiString(target, interchangeString);
            break;
        case JSON:
            result = getJmiJsonUtil().importFromJsonString(
                target,
                interchangeString);
            break;
        default:
            result = null;
        }
        return result;
    }

    protected Collection<RefBaseObject> getRemoteObjects(
        String wrappedLurqlQuery,
        RefPackage target,
        InterchangeFormat format)
    {
        Connection c = null;
        Statement stmt = null;
        ResultSet rs = null;
        Collection<RefBaseObject> result = null;
        try {
            c = getConnection();
            stmt = getStatement(c);
            rs = stmt.executeQuery(wrappedLurqlQuery);
            String interchangeString = StringChunker.readChunks(rs, 2);
            tracer.fine("Interchange data is:\n" + interchangeString);
            result = parseInterchange(target, interchangeString, format);
            rs.close();
            rs = null;
            releaseStatement(stmt);
            releaseConnection(c);
        } catch (SQLException e) {
            tracer.warning("Error executing query '" + wrappedLurqlQuery + "'");
            tracer.warning("Stack trace:\n" + Util.getStackTrace(e));
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                    rs = null;
                }
                releaseStatement(stmt);
                releaseConnection(c);
            } catch (SQLException se) {
            } finally {
                stmt = null;
                c = null;
            }
        }
        return result;
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
        return executeLurql(lurqlQuery, target, InterchangeFormat.XMI);
    }

    public Collection<RefBaseObject> executeLurql(
        String lurqlQuery,
        RefPackage target,
        InterchangeFormat format)
    {
        // if no JSON parse helper provided, force XMI
        if (format.equals(InterchangeFormat.JSON)
            && (getJmiJsonUtil() == null))
        {
            format = InterchangeFormat.XMI;
        }
        String wrappedQuery = constructQuery(lurqlQuery, format);
        Connection c = null;
        Statement stmt = null;
        ResultSet rs = null;
        Collection<RefBaseObject> result = null;
        try {
            c = getConnection();
            stmt = getStatement(c);
            rs = stmt.executeQuery(wrappedQuery);
            String xmiString = StringChunker.readChunks(rs, 2);
            tracer.finer("Interchange data is:\n" + xmiString);
            result = parseInterchange(target, xmiString, format);
            tracer.fine("Remotely imported " + result.size() + " objects");
            rs.close();
            rs = null;
            releaseStatement(stmt);
            releaseConnection(c);
        } catch (Throwable se) {
            tracer.warning("Error executing LURQL query '" + lurqlQuery + "'");
            tracer.warning("Stack trace:\n" + Util.getStackTrace(se));
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                    rs = null;
                }
                releaseStatement(stmt);
                releaseConnection(c);
            } catch (SQLException se) {
            } finally {
                stmt = null;
                c = null;
            }
        }
        return result;
    }

    /**
     * Wraps a LURQL query into a call to the LURQL-XMI UDX. Note that
     * we currently swallow all newline characters because of bug FRG-418
     * (newlines get encoded as Unicode literals, processed improperly).
     *
     * @param lurqlQuery String containing LURQL query
     * @param format InterchangeFormat option to indicate the format
     * @return String containing wrapped LURQL query
     */
    private String constructQuery(String lurqlQuery, InterchangeFormat format)
    {
        final SqlBuilder sqlBuilder = new SqlBuilder(SqlDialect.EIGENBASE);
        sqlBuilder.append("SELECT * FROM TABLE(SYS_BOOT.MGMT.GET_LURQL_");
        sqlBuilder.append(format.toString()).append("(")
            .literal(lurqlQuery.replace('\n', ' '))
            .append("))");
        return sqlBuilder.getSql();
    }

    public void getMetamodel(RefPackage extent)
    {
        final SqlBuilder query = new SqlBuilder(SqlDialect.EIGENBASE);
        query.append("SELECT * FROM TABLE(SYS_BOOT.MGMT.GET_METAMODEL_XMI(")
            .literal("FarragoMetamodel")
            .append("))");
        getRemoteObjects(query.getSql(), extent, InterchangeFormat.XMI);
    }

    /**
     * Enumeration for the formats supported for exchanging object descriptions
     * from the server to a client.
     * @author chard
     *
     */
    public enum InterchangeFormat {
        XMI, JSON
    }

}
// End FarragoLurqlService.java
