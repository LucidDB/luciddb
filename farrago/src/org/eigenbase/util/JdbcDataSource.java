/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
package org.eigenbase.util;

import java.io.*;

import java.sql.*;

import javax.sql.*;


/**
 * Adapter to make a JDBC connection into a {@link javax.sql.DataSource}.
 *
 * @author jhyde
 * @version $Id$
 * @since Sep 7, 2003
 */
public class JdbcDataSource
    implements DataSource
{

    //~ Instance fields --------------------------------------------------------

    private final String url;
    private PrintWriter logWriter;
    private int loginTimeout;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a JDBC data source.
     *
     * @param url URL of JDBC connection (must not be null)
     *
     * @pre url != null
     */
    public JdbcDataSource(String url)
    {
        assert (url != null);
        this.url = url;
    }

    //~ Methods ----------------------------------------------------------------

    public Connection getConnection()
        throws SQLException
    {
        if (url.startsWith("jdbc:hsqldb:")) {
            // Hsqldb requires a username, but doesn't support username as part
            // of the URL, durn it. Assume that the username is "sa".
            return DriverManager.getConnection(url, "sa", "");
        } else {
            return DriverManager.getConnection(url);
        }
    }

    public String getUrl()
    {
        return url;
    }

    public Connection getConnection(
        String username,
        String password)
        throws SQLException
    {
        return DriverManager.getConnection(url, username, password);
    }

    public void setLogWriter(PrintWriter out)
        throws SQLException
    {
        logWriter = out;
    }

    public PrintWriter getLogWriter()
        throws SQLException
    {
        return logWriter;
    }

    public void setLoginTimeout(int seconds)
        throws SQLException
    {
        loginTimeout = seconds;
    }

    public int getLoginTimeout()
        throws SQLException
    {
        return loginTimeout;
    }
}

// End JdbcDataSource.java
