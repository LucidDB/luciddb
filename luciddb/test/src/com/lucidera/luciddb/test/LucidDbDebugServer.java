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
                .getPath(), "class:org.luciddb.session.LucidDbSessionFactory");
    }
}

// End LucidDbDebugServer.java
