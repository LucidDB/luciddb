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
package org.h2.util;

import java.sql.SQLException;

/**
 * This class tries to automatically load the right JDBC driver for a given
 * database URL.
 */
public class JdbcDriverUtils {

    private static final String[] DRIVERS = {
        "jdbc:luciddb", "com.lucidera.jdbc.LucidDbRmiDriver", 
        "jdbc:h2:", "org.h2.Driver",
        "jdbc:Cache:", "com.intersys.jdbc.CacheDriver",
        "jdbc:daffodilDB://", "in.co.daffodil.db.rmi.RmiDaffodilDBDriver",
        "jdbc:daffodil", "in.co.daffodil.db.jdbc.DaffodilDBDriver",
        "jdbc:db2:", "COM.ibm.db2.jdbc.net.DB2Driver",
        "jdbc:derby:net:", "org.apache.derby.jdbc.ClientDriver",
        "jdbc:derby://", "org.apache.derby.jdbc.ClientDriver",
        "jdbc:derby:", "org.apache.derby.jdbc.EmbeddedDriver",
        "jdbc:FrontBase:", "com.frontbase.jdbc.FBJDriver",
        "jdbc:firebirdsql:", "org.firebirdsql.jdbc.FBDriver",
        "jdbc:hsqldb:", "org.hsqldb.jdbcDriver",
        "jdbc:informix-sqli:", "com.informix.jdbc.IfxDriver",
        "jdbc:jtds:", "net.sourceforge.jtds.jdbc.Driver",
        "jdbc:microsoft:", "com.microsoft.jdbc.sqlserver.SQLServerDriver",
        "jdbc:mimer:", "com.mimer.jdbc.Driver",
        "jdbc:mysql:", "com.mysql.jdbc.Driver",
        "jdbc:odbc:", "sun.jdbc.odbc.JdbcOdbcDriver",
        "jdbc:oracle:", "oracle.jdbc.driver.OracleDriver",
        "jdbc:pervasive:", "com.pervasive.jdbc.v2.Driver",
        "jdbc:pointbase:micro:", "com.pointbase.me.jdbc.jdbcDriver",
        "jdbc:pointbase:", "com.pointbase.jdbc.jdbcUniversalDriver",
        "jdbc:postgresql:", "org.postgresql.Driver",
        "jdbc:sybase:", "com.sybase.jdbc3.jdbc.SybDriver",
        "jdbc:sqlserver:", "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "jdbc:teradata:", "com.ncr.teradata.TeraDriver",
    };

    private JdbcDriverUtils() {
        // utility class
    }

    /**
     * Get the driver class name for the given URL, or null if the URL is
     * unknown.
     *
     * @param url the database URL
     * @return the driver class name
     */
    public static String getDriver(String url) {
        for (int i = 0; i < DRIVERS.length; i += 2) {
            String prefix = DRIVERS[i];
            if (url.startsWith(prefix)) {
                return DRIVERS[i + 1];
            }
        }
        return null;
    }

    /**
     * Load the driver class for the given URL, if the database URL is known.
     *
     * @param url the database URL
     */
    public static void load(String url) throws SQLException {
        String driver = getDriver(url);
        if (driver != null) {
            ClassUtils.loadUserClass(driver);
        }
    }
}
