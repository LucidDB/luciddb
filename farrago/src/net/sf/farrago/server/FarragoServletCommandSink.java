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

import javax.servlet.*;
import javax.servlet.http.*;

import org.apache.commons.logging.*;

import de.simplicit.vjdbc.command.*;
import de.simplicit.vjdbc.serial.*;
import de.simplicit.vjdbc.server.command.*;
import de.simplicit.vjdbc.server.config.*;
import de.simplicit.vjdbc.servlet.*;
import de.simplicit.vjdbc.util.*;

/**
 * FarragoServletCommandSink was cloned from VJDBC ServletCommandSink.  We
 * could have foregone this entire class and simpy extended ServletCommandSink,
 * except _processor was private.  We just needed to add an additional member
 * (vjdbcConfig) and overload the init().
 *
 * @author ngoodman
 */
public class FarragoServletCommandSink extends HttpServlet
{
    private static Log logger =
        LogFactory.getLog(FarragoServletCommandSink.class);

    private CommandProcessor processor;
    private VJdbcConfiguration vjdbcConfig;

    public FarragoServletCommandSink()
    {
    }

    public VJdbcConfiguration getVjdbcConfig()
    {
        return vjdbcConfig;
    }

    public void setVjdbcConfig(VJdbcConfiguration vjdbcConfig)
    {
        this.vjdbcConfig = vjdbcConfig;
    }

    public void init(ServletConfig servletConfig) throws ServletException
    {
        VJdbcConfiguration.init(vjdbcConfig);
        processor = CommandProcessor.getInstance();
    }

    public void destroy()
    {
    }

    protected void doGet(
        HttpServletRequest httpServletRequest,
        HttpServletResponse httpServletResponse) throws ServletException
    {
        handleRequest(httpServletRequest, httpServletResponse);
    }

    protected void doPost(
        HttpServletRequest httpServletRequest,
        HttpServletResponse httpServletResponse) throws ServletException
    {
        handleRequest(httpServletRequest, httpServletResponse);
    }

    private void handleRequest(
        HttpServletRequest httpServletRequest,
        HttpServletResponse httpServletResponse) throws ServletException
    {
        ObjectInputStream ois = null;
        ObjectOutputStream oos = null;

        try {
            // Get the method to execute
            String method = httpServletRequest.getHeader(
                ServletCommandSinkIdentifier.METHOD_IDENTIFIER);

            if (method != null) {
                ois = new ObjectInputStream(
                    httpServletRequest.getInputStream());
                // And initialize the output
                OutputStream os = httpServletResponse.getOutputStream();
                oos = new ObjectOutputStream(os);
                Object objectToReturn = null;

                try {
                    // Some command to process ?
                    if (method.equals(
                           ServletCommandSinkIdentifier.PROCESS_COMMAND))
                    {
                        // Read parameter objects
                        Long connuid = (Long) ois.readObject();
                        Long uid = (Long) ois.readObject();
                        Command cmd = (Command) ois.readObject();
                        CallingContext ctx = (CallingContext) ois.readObject();
                        // Delegate execution to the CommandProcessor
                        objectToReturn = processor.process(
                            connuid, uid, cmd, ctx);
                    } else if (method.equals(
                                  ServletCommandSinkIdentifier.CONNECT_COMMAND))
                    {
                        String url = ois.readUTF();
                        Properties props = (Properties) ois.readObject();
                        Properties clientInfo = (Properties) ois.readObject();
                        CallingContext ctx = (CallingContext) ois.readObject();

                        ConnectionConfiguration connectionConfiguration =
                            VJdbcConfiguration.singleton().getConnection(url);

                        if (connectionConfiguration != null) {
                            Connection conn = connectionConfiguration.create(
                                props);
                            objectToReturn = processor.registerConnection(
                                conn, connectionConfiguration, clientInfo, ctx);
                        } else {
                            objectToReturn =
                                new SQLException(
                                    "VJDBC-Connection " + url + " not found");
                        }
                    }
                } catch (Throwable t) {
                    // Wrap any exception so that it can be transported back to
                    // the client
                    objectToReturn = SQLExceptionHelper.wrap(t);
                }

                // Write the result in the response buffer
                oos.writeObject(objectToReturn);
                oos.flush();

                httpServletResponse.flushBuffer();
            } else {
                // No VJDBC-Method ? Then we redirect the stupid browser user to
                // some information page :-)
                httpServletResponse.sendRedirect("index.html");
            }
        } catch (Exception e) {
            logger.error("Unexpected Exception", e);
            throw new ServletException(e);
        } finally {
            StreamCloser.close(ois);
            StreamCloser.close(oos);
        }
    }
}

// End FarragoServletCommandSink.java
