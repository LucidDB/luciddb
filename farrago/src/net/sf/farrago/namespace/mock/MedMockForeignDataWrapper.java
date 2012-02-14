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
package net.sf.farrago.namespace.mock;

import java.sql.*;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.resource.*;


/**
 * MedMockForeignDataWrapper provides a mock implementation of the {@link
 * FarragoMedDataWrapper} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class MedMockForeignDataWrapper
    extends MedAbstractDataWrapper
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new data wrapper instance.
     */
    public MedMockForeignDataWrapper()
    {
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoMedDataWrapper
    public String getSuggestedName()
    {
        return "MOCK_FOREIGN_DATA_WRAPPER";
    }

    // implement FarragoMedDataWrapper
    public String getDescription(Locale locale)
    {
        // NOTE jvs 18-June-2006: Since this is just a mock we can ignore
        // locale.
        return "Foreign data wrapper for mock tables";
    }

    @Override
    public DriverPropertyInfo[] getPluginPropertyInfo(
        Locale locale,
        Properties props)
    {
        MedPropertyInfoMap infoMap =
            new MedPropertyInfoMap(
                FarragoResource.instance(),
                "MedMock",
                props);
        infoMap.addPropInfo(
            MedMockDataServer.PROP_EXECUTOR_IMPL,
            false,
            new String[] {
                MedMockDataServer.PROPVAL_JAVA,
                MedMockDataServer.PROPVAL_FENNEL
            });
        infoMap.addPropInfo(
            MedMockDataServer.PROP_FOO,
            false);
        return infoMap.toArray();
    }

    // implement FarragoMedDataWrapper
    public DriverPropertyInfo [] getServerPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps)
    {
        MedPropertyInfoMap infoMap =
            new MedPropertyInfoMap(
                FarragoResource.instance(),
                "MedMock",
                serverProps);
        infoMap.addPropInfo(
            MedMockDataServer.PROP_SCHEMA_NAME);
        infoMap.addPropInfo(
            MedMockDataServer.PROP_TABLE_NAME);
        infoMap.addPropInfo(
            MedMockDataServer.PROP_EXECUTOR_IMPL,
            true,
            new String[] {
                MedMockDataServer.PROPVAL_JAVA,
                MedMockDataServer.PROPVAL_FENNEL
            });
        return infoMap.toArray();
    }

    @Override
    public DriverPropertyInfo[] getColumnSetPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps,
        Properties tableProps)
    {
        MedPropertyInfoMap infoMap =
            new MedPropertyInfoMap(
                FarragoResource.instance(),
                "MedMock",
                serverProps);
        infoMap.addPropInfo(
            "Prop1");
        infoMap.addPropInfo(
            "Prop2", true);
        infoMap.addPropInfo(
            "Prop3", false, new String[] { "x", "y"});
        return infoMap.toArray();
    }

    @Override
    public DriverPropertyInfo[] getColumnPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps,
        Properties tableProps,
        Properties columnProps)
    {
        MedPropertyInfoMap infoMap =
            new MedPropertyInfoMap(
                FarragoResource.instance(),
                "MedMock",
                serverProps);
        infoMap.addPropInfo(
            "ColProp1");
        infoMap.addPropInfo(
            "ColProp2", true);
        infoMap.addPropInfo(
            "ColProp3", false, new String[] { "x", null, "y"});
        return infoMap.toArray();
    }

    // TODO:  DriverPropertyInfo calls
    // implement FarragoMedDataWrapper
    public void initialize(
        FarragoRepos repos,
        Properties props)
        throws SQLException
    {
        super.initialize(repos, props);
    }

    // implement FarragoMedDataWrapper
    public FarragoMedDataServer newServer(
        String serverMofId,
        Properties props)
        throws SQLException
    {
        MedMockDataServer server =
            new MedMockDataServer(
                this,
                serverMofId,
                props);
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

// End MedMockForeignDataWrapper.java
