/*
// $Id$
// SFDC Connector is an Eigenbase SQL/MED connector for Salesforce.com
// Copyright (C) 2010 The Eigenbase Project
// Copyright (C) 2010 SQLstream, Inc.
// Copyright (C) 2010 DynamoBI Corporation
//
// This library is free software; you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation; either version 2.1 of the License, or (at
// your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
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
