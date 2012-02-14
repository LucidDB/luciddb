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
package net.sf.farrago.namespace.sfdc;

import java.sql.*;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.namespace.sfdc.resource.*;


/**
 * SfdcDataWrapper provides an implementation of the {@link
 * FarragoMedDataWrapper} interface.
 *
 * @author Sunny Choi
 * @version $Id$
 */
public class SfdcDataWrapper
    extends MedAbstractDataWrapper
{
    //~ Constructors -----------------------------------------------------------

    // ~ Static fields/initializers --------------------------------------------

    // ~ Constructors ----------------------------------------------------------

    /**
     * Creates a new data wrapper instance.
     */
    public SfdcDataWrapper()
    {
    }

    //~ Methods ----------------------------------------------------------------

    // ~ Methods ---------------------------------------------------------------

    // implement FarragoMedDataWrapper
    public String getSuggestedName()
    {
        return "SFDC_DATA_WRAPPER";
    }

    // implement FarragoMedDataWrapper
    public String getDescription(Locale locale)
    {
        // TODO: localize
        return "Foreign data wrapper for SFDC data";
    }

    // implement FarragoMedDataWrapper
    public DriverPropertyInfo [] getServerPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps)
    {
        // TODO: use locale

        MedPropertyInfoMap infoMap =
            new MedPropertyInfoMap(
                SfdcResource.instance(),
                "MedSfdc",
                serverProps);
        infoMap.addPropInfo(SfdcDataServer.PROP_USER_NAME, true);
        infoMap.addPropInfo(SfdcDataServer.PROP_PASSWORD, true);
        infoMap.addPropInfo(
            SfdcDataServer.PROP_EXTRA_VARCHAR_PRECISION,
            true,
            new String[] {
                Integer.toString(SfdcDataServer.DEFAULT_EXTRA_VARCHAR_PRECISION)
            });
        infoMap.addPropInfo(SfdcDataServer.PROP_ENDPOINT_URL, false);
        return infoMap.toArray();
    }

    // implement FarragoMedDataWrapper
    public void initialize(FarragoRepos repos, Properties props)
        throws SQLException
    {
        super.initialize(repos, props);
    }

    // implement FarragoMedDataWrapper
    public FarragoMedDataServer newServer(String serverMofId, Properties props)
        throws SQLException
    {
        SfdcDataServer server = new SfdcDataServer(serverMofId, props);
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

// End SfdcDataWrapper.java
