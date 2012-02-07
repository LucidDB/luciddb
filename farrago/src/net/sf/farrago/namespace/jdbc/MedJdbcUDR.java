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
            connection =
                DriverManager.getConnection(
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
