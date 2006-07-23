/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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
package net.sf.farrago.namespace.jdbc;

import java.sql.*;


/**
 * MedJdbcUDR defines some user-defined routines related to MedJdbc. They are
 * used for testing since MedJdbc is the only code which is always built as a
 * jar, even in development environments.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class MedJdbcUDR
{

    //~ Methods ----------------------------------------------------------------

    /**
     * Uses parameters to test whether a given JDBC connection can be
     * successfully established.
     *
     * @param driverClassName fully-qualified name of the driver class to load
     * (must be pre-installed on Farrago's classpath)
     * @param url JDBC url to use for connection
     * @param userName name of user to connect as
     * @param password password to connect with
     */
    public static void testConnection(
        String driverClassName,
        String url,
        String userName,
        String password)
        throws SQLException, ClassNotFoundException
    {
        Connection connection = null;
        try {
            Class.forName(driverClassName);
            connection = DriverManager.getConnection(
                    url,
                    userName,
                    password);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * Attempts to determine the driver class name for a given JDBC URL.
     *
     * @param url JDBC url
     *
     * @return fully-qualified class name of driver to use, or null if none
     * found
     */
    public static String getDriverForUrl(String url)
    {
        try {
            Driver driver = DriverManager.getDriver(url);
            return driver.getClass().getName();
        } catch (SQLException ex) {
            return null;
        }
    }
}

// End MedJdbcUDR.java
