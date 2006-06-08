/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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
package net.sf.farrago.server;

import net.sf.farrago.jdbc.engine.*;

import de.simplicit.vjdbc.server.rmi.*;
import de.simplicit.vjdbc.server.config.*;

import java.io.*;

/**
 * FarragoVjdbcServer is a wrapper which configures a VJDBC server 
 * to listen for connections on behalf of a Farrago DBMS engine.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoVjdbcServer extends FarragoAbstractServer
{
    /**
     * Defines the main entry point for the Farrago server.  Customized servers
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

    public FarragoVjdbcServer()
    {
    }

    public FarragoVjdbcServer(PrintWriter pw)
    {
        super(pw);
    }
    
    protected int startNetwork(FarragoJdbcServerDriver jdbcDriver)
        throws Exception
    {
        VJdbcConfiguration vjdbcConfig = new VJdbcConfiguration();
        ConnectionConfiguration configFarrago = new ConnectionConfiguration();
        configFarrago.setDriver(jdbcDriver.getClass().getName());
        configFarrago.setId("FarragoDBMS");
        configFarrago.setUrl(jdbcDriver.getBaseUrl());
        configFarrago.setConnectionPooling(false);
        vjdbcConfig.addConnection(configFarrago);
        
        // NOTE:  This odd sequence is required because of the
        // way the VJdbcConfiguration singleton works.
        VJdbcConfiguration.init(vjdbcConfig);
        vjdbcConfig = VJdbcConfiguration.singleton();

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
}

// End FarragoVjdbcServer.java
