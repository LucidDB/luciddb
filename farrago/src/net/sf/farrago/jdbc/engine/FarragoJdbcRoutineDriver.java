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

import java.util.*;

import net.sf.farrago.jdbc.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.session.*;


/**
 * FarragoJdbcRoutineDriver implements the JDBC driver used for default
 * connections from user-defined routines.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJdbcRoutineDriver
    extends FarragoAbstractJdbcDriver
    implements Driver
{
    // NOTE jvs 19-Jan-2005:  let FarragoJdbcEngineDriver register us,
    // since no one else should be referencing us directly

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoJdbcRoutineDriver object.
     */
    public FarragoJdbcRoutineDriver()
    {
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoAbstractJdbcDriver
    public String getBaseUrl()
    {
        return "jdbc:default:connection";
    }

    // implement FarragoAbstractJdbcDriver
    public String getUrlPrefix()
    {
        return getBaseUrl();
    }

    // implement Driver
    public Connection connect(
        String url,
        Properties info)
        throws SQLException
    {
        if (!acceptsURL(url)) {
            return null;
        }
        try {
            if (!url.equals(getBaseUrl())) {
                throw FarragoResource.instance().JdbcInvalidUrl.ex(url);
            }
            return FarragoRuntimeContext.newConnection();
        } catch (Throwable ex) {
            throw FarragoJdbcEngineDriver.newSqlException(ex);
        }
    }

    /**
     * Converts a connection returned via URL "jdbc:default:connection" to a
     * FarragoSession. This can be used by user-defined routines to gain
     * internal access to Farrago. Use with caution.
     *
     * @param conn connection
     *
     * @return session
     */
    public static FarragoSession getSessionForConnection(Connection conn)
        throws SQLException
    {
        try {
            return ((FarragoJdbcEngineConnection) conn).getSession();
        } catch (ClassCastException ex) {
            throw FarragoJdbcEngineDriver.newSqlException(ex);
        }
    }
}

// End FarragoJdbcRoutineDriver.java
