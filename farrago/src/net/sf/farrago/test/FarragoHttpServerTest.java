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
 * HTTP implementation.
 *
 *<p>
 *
 * TODO jvs 30-Sept-2009:  validate and re-enable tests with JRockit since
 * HTTP should be OK there (no RMI distributed gc to cause bumps).
 *
 * @author John Sichi
 * @version $Id$
 */
public class FarragoHttpServerTest extends FarragoVjdbcServerTest
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Initializes a new FarragoHttpServerTest.
     *
     * @param testCaseName JUnit test case name
     */
    public FarragoHttpServerTest(String testCaseName)
        throws Exception
    {
        super(testCaseName);
    }

    //~ Methods ----------------------------------------------------------------

    protected FarragoAbstractServer newServer()
    {
        FarragoVjdbcServer server = new FarragoVjdbcServer();
        // default protocol is HTTP
        return server;
    }

    protected FarragoAbstractJdbcDriver newClientDriver()
    {
        return new FarragoVjdbcHttpClientDriver();
    }
}

// End FarragoHttpServerTest.java
