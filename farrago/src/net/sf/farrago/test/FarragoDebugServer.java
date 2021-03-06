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
package net.sf.farrago.test;

import net.sf.farrago.server.*;


/**
 * FarragoDebugServer's only purpose is to provide an entry point from which
 * FarragoServer can be debugged via an IDE such as Eclipse.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoDebugServer
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Provides an entry point for debugging FarragoServer.
     *
     * @param args unused
     */
    public static void main(String [] args)
        throws Exception
    {
        // Trick to invoke FarragoTestCase's static initializer to get default
        // settings for environment variables.
        FarragoQueryTest unused = new FarragoQueryTest("unused");

        FarragoVjdbcServer.main(new String[0]);
    }
}

// End FarragoDebugServer.java
