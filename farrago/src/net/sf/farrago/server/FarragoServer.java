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

import java.util.*;

import net.sf.farrago.jdbc.engine.*;


/**
 * @author John V. Sichi
 * @version $Id$
 * @deprecated use FarragoRmiJdbcServer instead; this is hanging around for a
 * while to avoid breaking dependencies
 */
public class FarragoServer
    extends FarragoRmiJdbcServer
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoServer instance, with console output to System.out.
     * This constructor can be used to embed a FarragoServer inside of another
     * container such as a J2EE app server.
     */
    public FarragoServer()
    {
        super();
    }

    /**
     * Creates a new FarragoServer instance, with redirected console output.
     * This constructor can be used to embed a FarragoServer inside of another
     * container such as a J2EE app server.
     *
     * @param pw receives console output
     */
    public FarragoServer(PrintWriter pw)
        throws Exception
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
        FarragoServer server = new FarragoServer();
        server.start(new FarragoJdbcEngineDriver());
        server.runConsole();
    }
}

// End FarragoServer.java
