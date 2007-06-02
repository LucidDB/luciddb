/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 LucidEra, Inc.
// Copyright (C) 2006-2007 The Eigenbase Project
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
package com.lucidera.luciddb.test;

import net.sf.farrago.server.*;
import net.sf.farrago.util.*;

import org.eigenbase.util.property.*;

import java.io.*;

/**
 * LucidDbDebugServer's only purpose is to provide an entry point
 * from which the LucidDB server can be debugged via an IDE such as Eclipse.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LucidDbDebugServer
{
    /**
     * Provides an entry point for debugging the LucidDB server.
     *
     * @param args unused
     */
    public static void main(String [] args)
        throws Exception
    {
        initProperties();

        // Start the server.
        FarragoVjdbcServer.main(new String[0]);
    }

    static void initProperties()
    {
        // Set required system properties based on environment variables.
        StringProperty homeDir =
            FarragoProperties.instance().homeDir;
        StringProperty traceConfigFile =
            FarragoProperties.instance().traceConfigFile;
        String eigenHome = System.getenv("EIGEN_HOME");
        if (eigenHome == null) {
            throw new IllegalStateException(
                "environment variable EIGEN_HOME not set");
        }
        homeDir.set(new File(eigenHome, "luciddb").getAbsolutePath());
        traceConfigFile.set(
            new File(
                new File(
                    homeDir.get(),
                    "trace"),
                "LucidDbTrace.properties").getAbsolutePath());
        
        System.setProperty(
            FarragoProperties.instance().defaultSessionFactoryLibraryName
                .getPath(), "class:com.lucidera.farrago.LucidDbSessionFactory");
    }
}

// End LucidDbDebugServer.java
