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
import java.util.logging.*;

import javax.sql.*;

/**
 * Base class for all Farrago services. Superficially provides for a DataSource
 * and tracing as well as abstracting getting and releasing connections and
 * statements, enabling reuse in non-pooled applications.
 *
 * @author chard
 */
public abstract class FarragoService
{
    protected DataSource dataSource;
    protected Logger tracer;
    protected boolean reusingConnection;
    private Connection cachedConnection = null;
    private Statement cachedStatement = null;

    /**
     * Constructs a base service, tracking its data source and tracer.
     * @param dataSource DataSource to provide connection(s) to the server
     * @param tracer Logger for any errors or debugging info
     */
    protected FarragoService(DataSource dataSource, Logger tracer)
    {
        this(dataSource, tracer, false);
    }

    /**
     * Constructs a base service with data source and tracing, allowing for
     * self-caching of connections and statements, rather than relying on a
     * connection pool. Most applications probably do not need to enable this
     * connection reuse, but in cases where pooling is either unavailable or
     * excessively expnsive, this internal caching may be useful.
     *
     * @param dataSource DataSource to provide connection(s) to the server
     * @param tracer Logger for ay errors or debugging info
     * @param reusingConnection Boolean indicator whether to internally cache
     * connections and statements.
     */
    protected FarragoService(
        DataSource dataSource,
        Logger tracer,
        boolean reusingConnection)
    {
        this.dataSource = dataSource;
        this.tracer = tracer;
        this.reusingConnection = reusingConnection;
    }

    /**
     * Gets a connection from the service's data source. If the service reuses
     * connections, the connection is cached. Connections obtained from this
     * method should not be closed directly, but instead released by calling
     * {@link #releaseConnection(Connection)}. That allows either pooled or
     * unpooled connections to operate correctly.
     * @return Connection to server
     * @throws SQLException if connection fails
     */
    protected Connection getConnection() throws SQLException
    {
        if ((cachedConnection == null) || !reusingConnection) {
            cachedConnection = dataSource.getConnection();
        }
        return cachedConnection;
    }

    /**
     * Gets a statement on the indicated connection. Reuses an existing, cached
     * statement if one is available. Otherwise, creates a new statement.
     * Statements obtained from this method should not be closed directly, but
     * instead released by calling {@link #releaseStatement(Statement)}.
     * @param connection Connection to the server
     * @return Statement from local cache if available; otherwise, newl-created
     * statement
     * @throws SQLException if statement creation fails
     */
    protected Statement getStatement(Connection connection)
    throws SQLException
    {
        if ((cachedStatement == null) || (connection != cachedConnection)) {
            cachedStatement = connection.createStatement();
            cachedConnection = connection;
        }
        return cachedStatement;
    }

    /**
     * Releases the indicated connection. If the service is reusing connections,
     * this is a NOOP, otherwise it closes the connection. You chould call this
     * method instead of closing the connection as you normally would. This is
     * the complement to {@link #getConnection()}.
     * @param connection Connection to the server
     */
    protected void releaseConnection(Connection connection)
    {
        if (reusingConnection || (connection == null)) {
            return;
        } else {
            try {
                connection.close();
                connection = null;
            } catch (SQLException se) {
            }
        }
    }

    /**
     * Releases the indicated statement. If the service is reusing connections
     * (and statements), this is a NOOP; otherwise it closes the statement. You
     * should call this method instead of closing the statement as you would
     * normally. This is the complement to {@link #getStatement(Connection)}.
     * @param statement Statement to release
     */
    protected void releaseStatement(Statement statement)
    {
        if (reusingConnection || (statement == null)) {
            return;
        } else {
            try {
                statement.close();
                statement = null;
            } catch (SQLException se) {
            }
        }
    }

    /**
     * Resets local connection/statement cache by essentially forgetting the
     * previous values. This is used when disconnecting the client from the
     * server so that stale connections are not reused.
     */
    public void reset()
    {
        cachedConnection = null;
        cachedStatement = null;
    }
}
// End FarragoService.java
