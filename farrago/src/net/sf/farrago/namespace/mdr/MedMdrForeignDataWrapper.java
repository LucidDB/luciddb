/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
*/
package net.sf.farrago.namespace.mdr;

import java.sql.*;
import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;


/**
 * MedMdrForeignDataWrapper implements the FarragoMedDataWrapper
 * interface by representing classes in an MDR repository as tables
 * (mapping the class extent to a corresponding set of rows).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class MedMdrForeignDataWrapper extends MedAbstractDataWrapper
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Default constructor for access to external repositories.
     */
    public MedMdrForeignDataWrapper()
    {
    }

    //~ Methods ---------------------------------------------------------------

    // implement FarragoMedDataWrapper
    public String getSuggestedName()
    {
        return "MDR_DATA_WRAPPER";
    }

    // implement FarragoMedDataWrapper
    public String getDescription(Locale locale)
    {
        // TODO: localize
        return "Foreign data wrapper for MDR repository extents";
    }

    // TODO:  DriverPropertyInfo calls
    // implement FarragoMedDataWrapper
    public void initialize(
        FarragoRepos repos,
        Properties props)
        throws SQLException
    {
        super.initialize(repos, props);
        assert (props.isEmpty());
    }

    // implement FarragoMedDataWrapper
    public FarragoMedDataServer newServer(
        String serverMofId,
        Properties props)
        throws SQLException
    {
        MedMdrDataServer server =
            new MedMdrDataServer(serverMofId, props,
                getRepos());
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


// End MedMdrForeignDataWrapper.java
