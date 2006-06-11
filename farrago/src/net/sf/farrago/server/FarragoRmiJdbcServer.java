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

import java.io.*;
import java.util.*;

import net.sf.farrago.jdbc.engine.*;

/**
 * FarragoRmiJdbcServer is a wrapper which configures an RmiJdbc server
 * to listen for connections on behalf of a Farrago DBMS engine.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoRmiJdbcServer extends FarragoAbstractServer
{
    //~ Methods ---------------------------------------------------------------

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
        FarragoRmiJdbcServer server = new FarragoRmiJdbcServer();
        server.start(new FarragoJdbcEngineDriver());
        server.runConsole();
    }

    /**
     * Creates a new FarragoRmiJdbcServer instance, with console output to
     * System.out.  This constructor can be used to embed a FarragoRmiJdbcServer
     * inside of another container such as a J2EE app server.
     */
    public FarragoRmiJdbcServer()
    {
        super();
    }

    /**
     * Creates a new FarragoRmiJdbcServer instance, with redirected console
     * output.  This constructor can be used to embed a FarragoServer inside of
     * another container such as a J2EE app server.
     *
     * @param pw receives console output
     */
    public FarragoRmiJdbcServer(PrintWriter pw)
        throws Exception
    {
        super(pw);
    }

    // implement FarragoAbstractServer
    protected int startNetwork(FarragoJdbcServerDriver jdbcDriver)
        throws Exception
    {
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
        locateRmiRegistry();

        return rmiRegistryPort;
    }
}

// End FarragoRmiJdbcServer.java
