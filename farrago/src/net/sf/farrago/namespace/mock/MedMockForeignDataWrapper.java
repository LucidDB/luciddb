/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
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
