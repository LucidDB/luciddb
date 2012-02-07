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
package org.luciddb.jdbc;

import org.luciddb.session.*;

import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.session.*;


/**
 * LucidDbLocalDriver is a JDBC driver for the LucidDB engine for use by callers
 * running in the same JVM with the DBMS.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LucidDbLocalDriver
    extends FarragoUnregisteredJdbcEngineDriver
{
    //~ Static fields/initializers ---------------------------------------------

    static {
        new LucidDbLocalDriver().register();
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoJdbcServerDriver
    public FarragoSessionFactory newSessionFactory()
    {
        return new LucidDbSessionFactory();
    }
}

// End LucidDbLocalDriver.java
