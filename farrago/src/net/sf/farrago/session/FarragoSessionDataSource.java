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
package net.sf.farrago.session;

import java.sql.*;

import javax.sql.*;

import org.eigenbase.util.*;


/**
 * FarragoSessionDataSource implements {@link DataSource} by providing a
 * loopback {@link Connection} into a session; this can be used for execution of
 * reentrant SQL.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoSessionDataSource
    extends JdbcDataSource
{
    //~ Instance fields --------------------------------------------------------

    private final FarragoSessionConnectionSource connectionSource;

    //~ Constructors -----------------------------------------------------------

    public FarragoSessionDataSource(FarragoSession session)
    {
        super("jdbc:farrago:");
        connectionSource = session.getConnectionSource();
    }

    //~ Methods ----------------------------------------------------------------

    public Connection getConnection()
        throws SQLException
    {
        return connectionSource.newConnection();
    }
}

// End FarragoSessionDataSource.java
