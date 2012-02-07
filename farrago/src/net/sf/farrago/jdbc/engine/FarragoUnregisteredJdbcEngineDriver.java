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
package net.sf.farrago.jdbc.engine;

import java.sql.*;

import java.util.*;
import java.util.logging.*;

import net.sf.farrago.db.*;
import net.sf.farrago.jdbc.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.trace.*;


/**
 * FarragoJdbcEngineDriver implements the Farrago engine/server side of the
 * {@link java.sql.Driver} interface. It does not register itself; for that, use
 * {@link FarragoJdbcEngineDriver}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoUnregisteredJdbcEngineDriver
    extends FarragoAbstractJdbcDriver
    implements FarragoJdbcServerDriver
{
    //~ Static fields/initializers ---------------------------------------------

    private static final Logger tracer =
        FarragoTrace.getFarragoJdbcEngineDriverTracer();

    static {
        // NOTE jvs 29-July-2006:  Even though we don't register ourselves,
        // we do register the loopback driver, because that always needs
        // to be registered regardless of which driver is being used to
        // access the engine from outside.
        new FarragoJdbcRoutineDriver().register();
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoAbstractJdbcDriver
    public String getUrlPrefix()
    {
        return getBaseUrl();
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

        try {
            if (driverUrl.equals(getBaseUrl())
                || driverUrl.equals(getClientUrl()))
            {
                return new FarragoJdbcEngineConnection(
                    driverUrl,
                    driverProps,
                    newSessionFactory());
            } else {
                throw FarragoResource.instance().JdbcInvalidUrl.ex(driverUrl);
            }
        } catch (Throwable ex) {
            throw newSqlException(ex);
        }
    }

    // implement FarragoJdbcServerDriver
    public FarragoSessionFactory newSessionFactory()
    {
        return FarragoDatabase.newSessionFactory();
    }

    /**
     * Converter from any Throwable to SQLException.
     *
     * @param ex Throwable to be converted
     *
     * @return ex as a SQLException
     */
    static SQLException newSqlException(Throwable ex)
    {
        return FarragoJdbcUtil.newSqlException(ex, tracer);
    }

    /**
     * Creates a new SQLException from the message.
     *
     * @param message detail message, the reason for this exception
     *
     * @return message as a SQLException
     */
    static SQLException newSqlException(String message)
    {
        return FarragoJdbcUtil.newSqlException(message, tracer);
    }
}

// End FarragoUnregisteredJdbcEngineDriver.java
