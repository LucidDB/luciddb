/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package org.eigenbase.util;

import java.io.PrintWriter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.sql.DataSource;


/**
 * Adapter to make a JDBC connection into a {@link javax.sql.DataSource}.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since Sep 7, 2003
 */
public class JdbcDataSource implements DataSource
{
    //~ Instance fields -------------------------------------------------------

    public final String _url;
    private PrintWriter _logWriter;
    private int _loginTimeout;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a JDBC data source.
     *
     * @param url URL of JDBC connection (must not be null)
     *
     * @pre url != null
     */
    public JdbcDataSource(String url)
    {
        assert(url != null);
        this._url = url;
    }

    //~ Methods ---------------------------------------------------------------

    public Connection getConnection() throws SQLException
    {
        if (_url.startsWith("jdbc:hsqldb:")) {
            // Hsqldb requires a username, but doesn't support username as part
            // of the URL, durn it. Assume that the username is "sa".
            return DriverManager.getConnection(_url, "sa", "");
        } else {
            return DriverManager.getConnection(_url);
        }
    }

    public Connection getConnection(String username,String password)
        throws SQLException
    {
        return DriverManager.getConnection(_url,username,password);
    }

    public void setLogWriter(PrintWriter out) throws SQLException
    {
        _logWriter = out;
    }

    public PrintWriter getLogWriter() throws SQLException
    {
        return _logWriter;
    }

    public void setLoginTimeout(int seconds) throws SQLException
    {
        _loginTimeout = seconds;
    }

    public int getLoginTimeout() throws SQLException
    {
        return _loginTimeout;
    }
}


// End JdbcDataSource.java
