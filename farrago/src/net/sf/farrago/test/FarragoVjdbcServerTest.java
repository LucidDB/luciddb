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

import net.sf.farrago.jdbc.*;
import net.sf.farrago.jdbc.client.*;
import net.sf.farrago.server.*;


/**
 * FarragoServerTest tests Farrago client/server connections via VJDBC's
 * RMI implementation.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoVjdbcServerTest
    extends FarragoServerTest
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Initializes a new FarragoVjdbcServerTest.
     *
     * @param testCaseName JUnit test case name
     */
    public FarragoVjdbcServerTest(String testCaseName)
        throws Exception
    {
        super(testCaseName);
    }

    //~ Methods ----------------------------------------------------------------

    protected FarragoAbstractServer newServer()
    {
        FarragoVjdbcServer server = new FarragoVjdbcServer();
        server.setDefaultProtocol(FarragoVjdbcServer.ListeningProtocol.RMI);
        return server;
    }

    protected FarragoAbstractJdbcDriver newClientDriver()
    {
        return new FarragoVjdbcClientDriver();
    }

    public void testExceptionContents()
        throws Throwable
    {
        // JDF 02/06/08 This override is due to a limititation imposed by Vjdbc.
        // FarragoSqlException inherits SQLException.  Vjdbc will only pass
        // generic SQLExceptions.  If it is an exception that extends
        // SQLException a new SQLException is allocated and populated from the
        // original exception. See vjdbc.util.SqlExceptionHelper.wrap().
        // Bascially this is a bad method name since either it returns the
        // original exception or creates a whole new one and discards the old.
    }
}

// End FarragoVjdbcServerTest.java
