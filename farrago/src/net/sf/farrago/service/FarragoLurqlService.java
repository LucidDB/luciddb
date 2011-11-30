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
    private Object lockHandle = new Object();

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

    /**
     * Get a list of repository object names, representing the names of the
     * objects that result from the specified LURQL query. This is meant for
     * quick verification of object lists without the overhead of actually
     * transmitting the objects from the server to the client.
     *
     * @param wrappedLurqlQuery String containing the LURQL query wrapped in a
     * SQL UDR call
     * @return List of object names (if any) that matched the LURQL query
     */
    protected List<String> getRemoteNames(String wrappedLurqlQuery)
    {
        List<String> result = new LinkedList<String>();
        Connection connection = null;
        Statement statement = null;
        ResultSet rs = null;
        synchronized (lockHandle) {
            try {
                connection = getConnection();
                statement = getStatement(connection);
                rs = statement.executeQuery(wrappedLurqlQuery);
                while (rs.next()) {
                    result.add(rs.getString(1));
                }
                rs.close();
                rs = null;
                releaseStatement(statement);
                releaseConnection(connection);
            } catch (SQLException e) {
                tracer.warning(
                    "Error executing query '" + wrappedLurqlQuery + "'");
                tracer.warning("Stack trace:\n" + Util.getStackTrace(e));
            } finally {
                try {
                    if (rs != null) {
                        rs.close();
                        rs = null;
                    }
                    releaseStatement(statement);
                    releaseConnection(connection);
                } catch (SQLException se) {
                } finally {
                    statement = null;
                    connection = null;
                }
            }
        }
        return result;
    }

    protected Collection<RefBaseObject> getRemoteObjects(
        String wrappedLurqlQuery,
        RefPackage target,
        InterchangeFormat format)
    {
        Connection connection = null;
        Statement statement = null;
        ResultSet rs = null;
        Collection<RefBaseObject> result = null;
        synchronized (lockHandle) {
            try {
                connection = getConnection();
                statement = getStatement(connection);
                rs = statement.executeQuery(wrappedLurqlQuery);
                String interchangeString = StringChunker.readChunks(rs, 2);
                tracer.finer("Interchange data is:\n" + interchangeString);
                result = parseInterchange(target, interchangeString, format);
                tracer.fine("Remotely imported " + result.size() + " objects");
                rs.close();
                rs = null;
                releaseStatement(statement);
                releaseConnection(connection);
            } catch (SQLException e) {
                tracer.warning(
                    "Error executing query '" + wrappedLurqlQuery + "'");
                tracer.warning("Stack trace:\n" + Util.getStackTrace(e));
            } finally {
                try {
                    if (rs != null) {
                        rs.close();
                        rs = null;
                    }
                    releaseStatement(statement);
                    releaseConnection(connection);
                } catch (SQLException se) {
                } finally {
                    statement = null;
                    connection = null;
                }
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
        return getRemoteObjects(wrappedQuery, target, format);
    }

    /**
     * Returns just the names of the objects that match the specified LURQL
     * query. This is meant to be a lightweight alternative to transferring the
     * set of objects and iterating it on the client for cases when you just
     * want the names of the objects.
     *
     * @param lurqlQuery String containing LURQL query
     * @return List of String objects containing only the names of the objects
     * matching the LURQL query
     */
    public List<String> getNameList(String lurqlQuery)
    {
        String wrappedQuery = wrapNameQuery(lurqlQuery);
        return getRemoteNames(wrappedQuery);
    }

    /**
     * Return the SQL declaration (DDL) for the specified object. This can be
     * used by clients to tell whether the object definition has changed.
     *
     * @param catalogName String containing the name of the catalog containing
     * the object (e.g., &quot;LOCALDB&quot;)
     * @param schemaName String containing the name of the schema containing
     * the object
     * @param objectName String containing the name of the object
     * @return String containing DDL representing the object or null if the
     * object does not exist
     */
    public String getObjectDdl(
        String catalogName,
        String schemaName,
        String objectName)
    {
        String result = null;
        final SqlBuilder sqlBuilder = new SqlBuilder(SqlDialect.EIGENBASE);
        sqlBuilder.append("SELECT * FROM TABLE(SYS_BOOT.MGMT.GET_OBJECT_DDL(");
        sqlBuilder.literal(catalogName).append(",")
            .literal(schemaName).append(",")
            .literal(objectName).append("))");
        final String query = sqlBuilder.getSql();
        Connection connection = null;
        Statement statement = null;
        ResultSet rs = null;
        synchronized (lockHandle) {
            try {
                connection = getConnection();
                statement = getStatement(connection);
                rs = statement.executeQuery(query);
                result = StringChunker.readChunks(rs, 2);
                rs.close();
                rs = null;
                releaseStatement(statement);
                releaseConnection(connection);
            } catch (SQLException e) {
                tracer.warning("Error executing query '" + query + "'");
                tracer.warning("Stack trace:\n" + Util.getStackTrace(e));
            } finally {
                try {
                    if (rs != null) {
                        rs.close();
                        rs = null;
                    }
                    releaseStatement(statement);
                    releaseConnection(connection);
                } catch (SQLException se) {
                } finally {
                    statement = null;
                    connection = null;
                }
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

    private String wrapNameQuery(String lurqlQuery)
    {
        final SqlBuilder sqlBuilder = new SqlBuilder(SqlDialect.EIGENBASE);
        sqlBuilder.append("SELECT * FROM TABLE(SYS_BOOT.MGMT.GET_LURQL_NAMES(");
        sqlBuilder.literal(lurqlQuery.replace('\n', ' ')).append("))");
        return sqlBuilder.getSql();
    }

    /**
     * Loads the Farrago metamodel into he specified package.
     * @param extent RefPackage to load the metamodel into (most likely the
     * farrago package)
     */
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
     */
    public enum InterchangeFormat {
        XMI, JSON
    }

}
// End FarragoLurqlService.java
