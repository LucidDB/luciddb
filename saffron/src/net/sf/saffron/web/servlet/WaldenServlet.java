/*
// Saffron preprocessor and data engine.
// Copyright (C) 2002-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

package net.sf.saffron.web.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Enumeration;
import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletException;
import javax.servlet.http.*;

import net.sf.saffron.walden.Handler;
import net.sf.saffron.walden.Interpreter;
import net.sf.saffron.walden.PrintHandler;

import openjava.tools.parser.Parser;

import org.eigenbase.util.SaffronProperties;
import org.eigenbase.util.Util;


/**
 * <code>WaldenServlet</code> todo:
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 26 May, 2002
 */
public class WaldenServlet extends HttpServlet
{
    public String getServletInfo()
    {
        return "Execute a Java/Saffron statement";
    }

    public void destroy()
    {
    }

    public void init(ServletConfig config)
        throws ServletException
    {
        super.init(config);
    }

    protected void doGet(
        HttpServletRequest request,
        HttpServletResponse response)
        throws ServletException, IOException
    {
        processRequest(request, response);
    }

    protected void doPost(
        HttpServletRequest request,
        HttpServletResponse response)
        throws ServletException, java.io.IOException
    {
        processRequest(request, response);
    }

    /**
     * Processes requests for both HTTP <code>GET</code> and <code>POST</code>
     * methods.
     *
     * @param request servlet request
     * @param response servlet response
     */
    protected void processRequest(
        HttpServletRequest request,
        HttpServletResponse response)
        throws ServletException, IOException
    {
        String command = request.getParameter("commandString");
        request.setAttribute("commandString", command);
        if (command != null) {
            Session session = Session.getInstance(request.getSession());
            StringWriter sw = new StringWriter();
            Handler handler =
                new PrintHandler(session.interpreter,
                    new PrintWriter(sw, true), false);
            Parser parser = new Parser(new StringReader(command));
            while (session.interpreter.runOne(parser, handler)) {
            }
            String result = sw.toString();
            request.setAttribute("result", result);
        }
        getServletContext().getRequestDispatcher("/index.jsp").include(request,
            response);
    }

    /**
     * Holds the context required by {@link WaldenServlet}.
     */
    static class Context
    {
        private static final String ATTRNAME =
            "saffron.web.servlet.WaldenServlet$Context";
        private ServletContext context;

        /**
         * Creates a <code>Context</code>. Only {@link WaldenListener} calls
         * this; you should probably call {@link #getInstance}.
         */
        Context()
        {
        }

        /**
         * Retrieves the one and only instance of <code>Context</code> in this
         * servlet's httpSession.
         */
        public static Context getInstance(ServletContext context)
        {
            return (Context) context.getAttribute(ATTRNAME);
        }

        public void destroy(ServletContextEvent event)
        {
        }

        public void init(ServletContextEvent event)
        {
            this.context = event.getServletContext();
            context.setAttribute(ATTRNAME, this);

            Properties properties = SaffronProperties.instance();
            for (Enumeration enumeration =
                    event.getServletContext().getInitParameterNames();
                    enumeration.hasMoreElements();) {
                String name = (String) enumeration.nextElement();
                if (name.startsWith("saffron")) {
                    String value =
                        event.getServletContext().getInitParameter(name);
                    properties.setProperty(name, value);
                }
            }
        }
    }

    /**
     * Holds the httpSession required by {@link WaldenServlet}.
     */
    static class Session
    {
        private static final String ATTRNAME =
            "saffron.web.servlet.WaldenServlet$Session";
        Interpreter interpreter;
        private HttpSession httpSession;

        /**
         * Creates a <code>Context</code>. Only {@link WaldenListener} calls
         * this; you should probably call {@link #getInstance}.
         */
        Session()
        {
        }

        /**
         * Retrieves the one and only instance of <code>Context</code> in this
         * servlet's httpSession.
         */
        public static Session getInstance(HttpSession session)
        {
            return (Session) session.getAttribute(ATTRNAME);
        }

        public void destroy(HttpSessionEvent event)
        {
        }

        public void init(HttpSessionEvent event)
        {
            this.httpSession = event.getSession();
            httpSession.setAttribute(ATTRNAME, this);

            Properties properties = SaffronProperties.instance();
            for (Enumeration enumeration = httpSession.getAttributeNames();
                    enumeration.hasMoreElements();) {
                String name = (String) enumeration.nextElement();
                if (name.startsWith("saffron")) {
                    Object value = httpSession.getAttribute(name);
                    if (value instanceof String) {
                        properties.setProperty(name, (String) value);
                    }
                }
            }

            interpreter = new Interpreter();
        }
    }
}


// End WaldenServlet.java
