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
package org.luciddb.session;

import org.luciddb.jdbc.*;

import java.io.*;

import net.sf.farrago.server.*;


/**
 * LucidDbServer is a wrapper to insulate LucidDB scripts from direct
 * dependencies on Farrago classes.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LucidDbServer
    extends FarragoVjdbcServer
{
    //~ Constructors -----------------------------------------------------------

    public LucidDbServer()
        throws Exception
    {
        super();
    }

    public LucidDbServer(PrintWriter pw)
        throws Exception
    {
        super(pw);
    }

    //~ Methods ----------------------------------------------------------------

    // override FarragoVjdbcServer
    public static void main(String [] args)
        throws Exception
    {
        LucidDbServer server = new LucidDbServer();
        server.start(new LucidDbLocalDriver());
        server.runConsole();
    }
}

// End LucidDbServer.java
