/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2009 The Eigenbase Project
// Copyright (C) 2009 SQLstream, Inc.
// Copyright (C) 2009 Dynamo BI Corporation
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
