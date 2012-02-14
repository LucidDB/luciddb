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
package net.sf.farrago.jdbc.engine;

import java.sql.*;

import net.sf.farrago.session.*;


/**
 * FarragoJdbcServerDriver defines the interface which must be implemented by
 * JDBC drivers which can be used to implement {@link
 * net.sf.farrago.server.FarragoVjdbcServer}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoJdbcServerDriver
    extends Driver
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Creates a new FarragoSessionFactory which will govern the behavior of
     * connections established through this driver.
     *
     * @return new factory
     */
    public FarragoSessionFactory newSessionFactory();

    /**
     * @return the base JDBC URL for this driver; subclassing drivers can
     * override this to customize the URL scheme
     */
    public String getBaseUrl();
}

// End FarragoJdbcServerDriver.java
