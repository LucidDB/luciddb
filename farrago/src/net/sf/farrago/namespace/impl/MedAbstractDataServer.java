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
package net.sf.farrago.namespace.impl;

import java.sql.*;

import java.util.*;

import javax.sql.*;

import net.sf.farrago.namespace.*;

import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;


/**
 * MedAbstractDataServer is an abstract base class for implementations of the
 * {@link FarragoMedDataServer} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class MedAbstractDataServer
    extends MedAbstractBase
    implements FarragoMedDataServer
{
    //~ Instance fields --------------------------------------------------------

    private String serverMofId;
    private Properties props;
    private DataSource loopbackDataSource;

    //~ Constructors -----------------------------------------------------------

    protected MedAbstractDataServer(
        String serverMofId,
        Properties props)
    {
        this.serverMofId = serverMofId;
        this.props = props;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return the MofId of the catalog definition for this server
     */
    public String getServerMofId()
    {
        return serverMofId;
    }

    /**
     * @return the options specified by CREATE SERVER
     */
    public Properties getProperties()
    {
        return props;
    }

    /**
     * @return current loopback data source
     */
    public DataSource getLoopbackDataSource()
    {
        return loopbackDataSource;
    }

    // implement FarragoMedDataServer
    public void setLoopbackDataSource(DataSource loopbackDataSource)
    {
        this.loopbackDataSource = loopbackDataSource;
    }

    // implement FarragoMedDataServer
    public FarragoMedNameDirectory getNameDirectory()
        throws SQLException
    {
        return null;
    }

    // implement FarragoMedDataServer
    public Object getRuntimeSupport(Object param)
        throws SQLException
    {
        return null;
    }

    // implement FarragoMedDataServer
    public void registerRules(RelOptPlanner planner)
    {
    }

    // implement FarragoMedDataServer
    public void registerRelMetadataProviders(ChainedRelMetadataProvider chain)
    {
    }

    // implement FarragoAllocation
    public void closeAllocation()
    {
    }

    // implement FarragoMedDataServer
    public void releaseResources()
    {
    }
}

// End MedAbstractDataServer.java
