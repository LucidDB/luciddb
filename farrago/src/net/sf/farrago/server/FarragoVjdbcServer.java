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
package net.sf.farrago.server;

import de.simplicit.vjdbc.server.config.*;
import de.simplicit.vjdbc.server.rmi.*;
import de.simplicit.vjdbc.util.*;

import java.io.*;

import java.rmi.*;

import java.sql.*;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.jdbc.engine.*;


/**
 * FarragoVjdbcServer is a wrapper which configures a VJDBC server to listen for
 * connections on behalf of a Farrago DBMS engine.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoVjdbcServer
    extends FarragoAbstractServer
{
    //~ Instance fields --------------------------------------------------------

    private FarragoJdbcServerDriver jdbcDriver;
    private FarragoJettyEmbedding jettyEmbedding;

    //~ Constructors -----------------------------------------------------------

    public FarragoVjdbcServer()
    {
    }

    public FarragoVjdbcServer(PrintWriter pw)
    {
        super(pw);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Defines the main entry point for the Farrago server. Customized servers
     * can provide their own which call start() with an extended implementation
     * of {@link net.sf.farrago.jdbc.engine.FarragoJdbcServerDriver}.
     *
     * @param args ignored
     */
    public static void main(String [] args)
        throws Exception
    {
        FarragoVjdbcServer server = new FarragoVjdbcServer();
        server.start(new FarragoJdbcEngineDriver());
        server.runConsole();
    }

    protected int startNetwork(FarragoJdbcServerDriver jdbcDriver)
        throws Exception
    {
        // REVIEW jvs 7-Sept-2006: seems like we should null this out in
        // stopNetwork, but that causes problems in FarragoVjdbcServerTest.
        this.jdbcDriver = jdbcDriver;

        VJdbcConfiguration vjdbcConfig = new VJdbcConfiguration();
        ConnectionConfiguration configFarrago =
            new FarragoConnectionConfiguration();
        configFarrago.setDriver(jdbcDriver.getClass().getName());
        configFarrago.setId("FarragoDBMS");
        configFarrago.setUrl(jdbcDriver.getBaseUrl());
        configFarrago.setConnectionPooling(false);
        configFarrago.setPrefetchResultSetMetaData(true);
        vjdbcConfig.addConnection(configFarrago);

        if (protocol == ListeningProtocol.HTTP) {
            configureConnectionTimeout(vjdbcConfig);
            jettyEmbedding = new FarragoJettyEmbedding();
            jettyEmbedding.startServlet(vjdbcConfig, httpPort);
            return httpPort;
        }

        // NOTE:  This odd sequence is required because of the
        // way the VJdbcConfiguration singleton works.
        VJdbcConfiguration.init(vjdbcConfig);
        vjdbcConfig = VJdbcConfiguration.singleton();
        configureConnectionTimeout(vjdbcConfig);

        RmiConfiguration rmiConfig = new RmiConfiguration();
        vjdbcConfig.setRmiConfiguration(rmiConfig);
        rmiConfig.setPort(rmiRegistryPort);
        if (rmiRegistry != null) {
            // A server instance was previously in existence, so don't
            // try to recreate the RMI registry.
            rmiConfig.setCreateRegistry(false);
        } else {
            rmiConfig.setCreateRegistry(true);
        }

        ConnectionServer server = new ConnectionServer();
        server.serve();
        locateRmiRegistry();

        return rmiRegistryPort;
    }

    private void configureConnectionTimeout(VJdbcConfiguration vjdbcConfig)
    {
        if (connectionTimeoutMillis == -1) {
            // -1 means never timeout, so set OCCT checking period to 0
            vjdbcConfig.getOcctConfiguration().setTimeoutInMillis(
                FarragoCatalogInit.DEFAULT_CONNECTION_TIMEOUT_MILLIS);
            vjdbcConfig.getOcctConfiguration().setCheckingPeriodInMillis(0);
        } else {
            vjdbcConfig.getOcctConfiguration().setTimeoutInMillis(
                connectionTimeoutMillis);
        }
    }

    protected void stopNetwork()
    {
        if (protocol == ListeningProtocol.HTTP) {
            if (jettyEmbedding != null) {
                jettyEmbedding.stopServlet();
                jettyEmbedding = null;
            }
        } else {
            super.stopNetwork();
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    // NOTE jvs 7-Sept-2006:  This is to avoid calling DriverManager,
    // which can deadlock when client and server are in same process.
    private class FarragoConnectionConfiguration
        extends ConnectionConfiguration
    {
        public Connection create(Properties props)
            throws SQLException
        {
            try {
                // set remoteProtocol to RMI to tell engine driver that
                // this is a remote connection

                // NOTE: if basing authentication on whether a connection is
                // remote or not, it is VITAL that the remoteProtocol is
                // overwritten here. Otherwise this is a potential security
                // hole!
                props.setProperty("remoteProtocol", protocol.toString());
                return jdbcDriver.connect(jdbcDriver.getBaseUrl(), props);
            } catch (Throwable t) {
                throw SQLExceptionHelper.wrap(t);
            }
        }
    }
}

// End FarragoVjdbcServer.java
