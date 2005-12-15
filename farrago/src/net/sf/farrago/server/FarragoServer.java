/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
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
package net.sf.farrago.server;

import java.io.*;
import java.rmi.*;
import java.rmi.registry.*;
import java.util.*;

import net.sf.farrago.db.*;
import net.sf.farrago.fem.config.*;
import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.release.*;
import net.sf.farrago.util.*;

/**
 * FarragoServer is a wrapper for an RmiJdbc server.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoServer
{
    //~ Static fields/initializers --------------------------------------------

    protected static Registry rmiRegistry;

    //~ Methods ---------------------------------------------------------------

    /**
     * Defines the main entry point for the Farrago server.  Customized servers
     * can provide their own which call run() with an extended implementation
     * of {@link net.sf.farrago.jdbc.engine.FarragoJdbcServerDriver}.
     *
     * @param args ignored
     */
    public static void main(String [] args)
    {
        FarragoServer server = new FarragoServer();
        server.start(new FarragoJdbcEngineDriver());
        server.runConsole();
    }

    /**
     * Starts the server.
     *
     * @param jdbcDriver the JDBC driver which will be served
     * to remote clients
     */
    public void start(FarragoJdbcServerDriver jdbcDriver)
    {
        FarragoResource res = FarragoResource.instance();
        FarragoReleaseProperties releaseProps =
            FarragoReleaseProperties.instance();
        System.out.println(
            res.ServerProductName.str(
                releaseProps.productName.get()));
        System.out.println(res.ServerLoadingDatabase.str());

        // Load the session factory
        FarragoSessionFactory sessionFactory = jdbcDriver.newSessionFactory();

        // Load the database instance
        FarragoDatabase db = FarragoDbSingleton.pinReference(sessionFactory);

        FemFarragoConfig config = db.getSystemRepos().getCurrentConfig();

        int rmiRegistryPort = config.getServerRmiRegistryPort();

        int singleListenerPort = config.getServerSingleListenerPort();

        if (rmiRegistryPort == -1) {
            rmiRegistryPort = releaseProps.jdbcUrlPortDefault.get();
        }

        System.out.println(res.ServerStartingNetwork.str());

        List argList = new ArrayList();

        if (rmiRegistry != null) {
            // A server instance was previously in existence, so don't
            // try to recreate the RMI registry.
            argList.add("-noreg");
        }

        argList.add("-port");
        argList.add(Integer.toString(rmiRegistryPort));

        if (singleListenerPort != -1) {
            argList.add("-lp");
            argList.add(Integer.toString(singleListenerPort));
        }

        FarragoRJJdbcServer.main((String []) argList.toArray(new String[0]));

        if (rmiRegistry == null) {
            // This is the first server instance in this JVM, so
            // look up the RMI registry which was just created by
            // RJJdbcServer.main.
            try {
                rmiRegistry = LocateRegistry.getRegistry(rmiRegistryPort);
            } catch (RemoteException ex) {
                // TODO:  handle this better
            }
        }

        System.out.println(
            res.ServerListening.str(new Integer(rmiRegistryPort)));
    }

    /**
     * Stops the server if there are no sessions.
     *
     * @return whether server was stopped
     */
    public boolean stopSoft()
    {
        FarragoResource res = FarragoResource.instance();
        System.out.println(res.ServerShuttingDown.str());

        // NOTE:  use groundReferences=1 in shutdownConditional
        // to account for our baseline reference
        if (FarragoDbSingleton.shutdownConditional(getGroundReferences())) {
            System.out.println(res.ServerShutdownComplete.str());

            // TODO: should find a way to prevent new messages BEFORE shutdown
            unbindRegistry();
            return true;
        } else {
            System.out.println(res.ServerSessionsExist.str());
            return false;
        }
    }

    /**
     * Returns the number of ground references for this server.  Ground
     * references are references pinned at startup time.  For the base
     * implementation of FarragoServer this is always 1.  Farrago extensions,
     * especially those that initialize resources via
     * {@link FarragoSessionFactory#specializedInitialization(
     *     FarragoAllocationOwner)}, may need to alter this value.
     *
     * @return the number of ground references for this server
     */
    protected int getGroundReferences()
    {
        return 1;
    }

    /**
     * Stops the server, killing any sessions.
     */
    public void stopHard()
    {
        unbindRegistry();
        FarragoResource res = FarragoResource.instance();
        System.out.println(res.ServerShuttingDown.str());
        FarragoDbSingleton.shutdown();
        System.out.println(res.ServerShutdownComplete.str());
    }

    /** Unbinds all items remaining in RMI registry. */
    protected void unbindRegistry()
    {
        if (rmiRegistry == null) {
            return;
        }

        try {
            String [] names = rmiRegistry.list();
            for (int i = 0; i < names.length; ++i) {
                rmiRegistry.unbind(names[i]);
            }
        } catch (Exception ex) {
            // TODO:  handle this better
            ex.printStackTrace();
        }
    }

    /**
     * Implements console interaction from stdin after the server
     * has successfully started.
     */
    public void runConsole()
    {
        FarragoResource res = FarragoResource.instance();

        // TODO:  install signal handlers also
        InputStreamReader inReader = new InputStreamReader(System.in);
        LineNumberReader lineReader = new LineNumberReader(inReader);
        for (;;) {
            String cmd;
            try {
                cmd = lineReader.readLine();
            } catch (IOException ex) {
                break;
            }
            if (cmd == null) {
                // interpret end-of-stream as meaning we are supposed to
                // run forever as a daemon
                return;
            }
            if (cmd.equals("!quit")) {
                if (stopSoft()) {
                    break;
                }
            } else if (cmd.equals("!kill")) {
                stopHard();
                break;
            } else {
                System.out.println(res.ServerBadCommand.str(cmd));
            }
        }
        System.exit(0);
    }

}


// End FarragoServer.java
