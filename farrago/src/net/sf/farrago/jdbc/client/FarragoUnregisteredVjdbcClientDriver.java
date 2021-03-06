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
package net.sf.farrago.jdbc.client;

import de.simplicit.vjdbc.*;

import java.sql.*;

import java.util.*;

import net.sf.farrago.jdbc.*;
import net.sf.farrago.release.*;


/**
 * FarragoUnregisteredJdbcClientDriver implements the Farrago client side of the
 * {@link java.sql.Driver} interface via the VJDBC proxy. It does not register
 * itself; for that, use {@link FarragoVjdbcClientDriver}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoUnregisteredVjdbcClientDriver
    extends FarragoAbstractJdbcDriver
{
    //~ Methods ----------------------------------------------------------------

    /**
     * @return the prefix for JDBC URL's understood by this driver; subclassing
     * drivers can override this to customize the URL scheme
     */
    public String getUrlPrefix()
    {
        return getBaseUrl() + "rmi://";
    }

    // implement Driver
    public Connection connect(
        String url,
        Properties info)
        throws SQLException
    {
        if (!acceptsURL(url)) {
            return null;
        }

        // connection property precedence:
        // connect string (URI), info props, connection defaults

        // don't modify user's properties:
        //  copy input props backed by connection defaults,
        //  move any params from the URI to the properties
        Properties driverProps = applyDefaultConnectionProps(info);
        String driverUrl = parseConnectionParams(url, driverProps);

        Driver rmiDriver;
        try {
            rmiDriver = new VirtualDriver();
        } catch (Exception ex) {
            // TODO: use FarragoJdbcUtil.newSqlException, see Jira FRG-122
            throw new SQLException(ex.getMessage());
        }

        // transform the URL into a form understood by VJDBC
        String urlRmi = driverUrl.substring(getUrlPrefix().length());
        String [] split = urlRmi.split(":");
        if (split.length == 1) {
            // no port number, so append default
            FarragoReleaseProperties props =
                FarragoReleaseProperties.instance();
            urlRmi = urlRmi + ":" + props.jdbcUrlPortDefault.get();
        }
        urlRmi = "jdbc:vjdbc:rmi://" + urlRmi + "/VJdbc,FarragoDBMS";

        // NOTE:  can't call DriverManager.connect here, because that
        // would deadlock in the case where client and server are
        // running in the same VM
        return rmiDriver.connect(urlRmi, driverProps);
    }
}

// End FarragoUnregisteredVjdbcClientDriver.java
