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

/*
   // $Id$
   // This software is subject to the terms of the Common Public License
   // Agreement, available at the following URL:
   // http://www.opensource.org/licenses/cpl.html.
   // (C) Copyright 2002-2003 Kana Software, Inc. and others.
   // All Rights Reserved.
   // You must accept the terms of that agreement to use this software.
   //
   // Julian Hyde, 28 March, 2002
 */
package net.sf.saffron.web.servlet;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;


/**
 * <code>WaldenListener</code> responds to servlet events on behalf of a
 * {@link WaldenServlet}.
 *
 * <p>
 * NOTE: This class must not depend upon any non-standard packages (such as
 * <code>javax.transform</code>) because it is loaded when Tomcat starts, not
 * when the servlet is loaded. (This might be a bug in Tomcat 4.0.3, because
 * it worked in 4.0.1. But anyway.)
 * </p>
 */
public class WaldenListener implements ServletContextListener,
    HttpSessionListener
{
    public WaldenListener()
    {
    }

    public void contextDestroyed(ServletContextEvent event)
    {
        WaldenServlet.Context context =
            WaldenServlet.Context.getInstance(event.getServletContext());
        context.destroy(event);
    }

    public void contextInitialized(ServletContextEvent event)
    {
        WaldenServlet.Context context = new WaldenServlet.Context();
        context.init(event);
    }

    public void sessionCreated(HttpSessionEvent event)
    {
        WaldenServlet.Session session = new WaldenServlet.Session();
        session.init(event);
    }

    public void sessionDestroyed(HttpSessionEvent event)
    {
        WaldenServlet.Session session =
            WaldenServlet.Session.getInstance(event.getSession());
        session.destroy(event);
    }
}


// End WaldenListener.java
