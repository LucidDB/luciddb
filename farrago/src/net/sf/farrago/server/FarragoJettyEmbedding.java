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
package net.sf.farrago.server;

import java.io.*;
import java.sql.*;
import java.util.*;

import org.eigenbase.util.*;

import org.eclipse.jetty.server.*;
import org.eclipse.jetty.servlet.*;

import de.simplicit.vjdbc.server.config.*;
import de.simplicit.vjdbc.server.servlet.*;
import de.simplicit.vjdbc.util.*;

import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.util.*;

/**
 * FarragoJettyEmbedding is responsible for loading up an embedded instance of
 * Jetty acting as a container for the VJDBC HTTP servlet.  We keep the
 * Jetty dependencies in their own class to avoid dragging them in
 * when not necessary.
 *
 * @author ngoodman
 * @version $Id$
 */
class FarragoJettyEmbedding
{
    private static final String VJDBC_URI = "/vjdbc";

    private Server httpServer;

    void startServlet(VJdbcConfiguration vjdbcConfig, int httpPort)
        throws Exception
    {
        httpServer = new Server(httpPort);

        ServletContextHandler context = new ServletContextHandler(
            ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        httpServer.setHandler(context);

        FarragoServletCommandSink servlet = new FarragoServletCommandSink();
        servlet.setVjdbcConfig(vjdbcConfig);
        servlet.init(null);

        ServletHolder sh = new ServletHolder(servlet);
        context.addServlet(sh, VJDBC_URI);

        httpServer.start();
    }

    void stopServlet()
    {
        try {
            httpServer.stop();
        } catch (Throwable ex) {
            throw Util.newInternal(ex);
        } finally {
            httpServer = null;
        }
    }
}

// End FarragoJettyEmbedding.java
