/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.farrago.namespace.mock;

import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.catalog.*;
import net.sf.farrago.resource.*;

import java.sql.*;
import java.util.*;

/**
 * MedMockForeignDataWrapper provides a mock implementation of the
 * {@link FarragoMedDataWrapper} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class MedMockForeignDataWrapper
    extends MedAbstractDataWrapper
{
    /**
     * Creates a new data wrapper instance.
     */
    public MedMockForeignDataWrapper()
    {
    }

    // implement FarragoMedDataWrapper
    public String getSuggestedName()
    {
        return "MOCK_DATA_WRAPPER";
    }
    
    // implement FarragoMedDataWrapper
    public String getDescription(Locale locale)
    {
        // TODO: localize
        return "Foreign data wrapper for mock data";
    }
    
    // TODO:  DriverPropertyInfo calls
    
    // implement FarragoMedDataWrapper
    public void initialize(
        FarragoRepos repos,
        Properties props) throws SQLException
    {
        super.initialize(repos,props);
    }

    // implement FarragoMedDataWrapper
    public FarragoMedDataServer newServer(
        String serverMofId,
        Properties props) throws SQLException
    {
        MedMockDataServer server =
            new MedMockDataServer(serverMofId,props);
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
