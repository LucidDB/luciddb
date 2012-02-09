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
package net.sf.firewater;

import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.jdbc.*;

import java.sql.*;
import java.util.*;

/**
 * FirewaterDataWrapper implements the
 * {@link net.sf.farrago.namespace.FarragoMedDataWrapper}
 * interface for Firewater distributed tables.
 *
 * @author John Sichi
 * @version $Id$
 */
public class FirewaterDataWrapper extends MedJdbcForeignDataWrapper
{
    // implement FarragoMedDataWrapper
    public String getSuggestedName()
    {
        return "FIREWATER_DATA_WRAPPER";
    }

    // implement FarragoMedDataWrapper
    public String getDescription(Locale locale)
    {
        // TODO: localize
        return "Local data wrapper for Firewater distributed tables";
    }

    // override MedJdbcForeignDataWrapper
    public boolean isForeign()
    {
        return false;
    }

    // override MedJdbcForeignDataWrapper
    public FarragoMedDataServer newServer(
        String serverMofId,
        Properties props)
        throws SQLException
    {
        String driverClassName = props.getProperty(PROP_DRIVER_CLASS_NAME);
        if (driverClassName != null) {
            loadDriverClass(driverClassName);
        }
        Properties chainedProps = new Properties(getProperties());
        chainedProps.putAll(props);
        FirewaterDataServer server =
            new FirewaterDataServer(serverMofId, chainedProps);
        boolean success = false;
        try {
            server.initialize();
            success = true;
            return server;
        } finally {
            if (!success) {
                server.closeAllocation();
            }
        }
    }
}

// End FirewaterDataWrapper.java
