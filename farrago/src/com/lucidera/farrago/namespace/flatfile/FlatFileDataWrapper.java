/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
package com.lucidera.farrago.namespace.flatfile;

import java.sql.*;
import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.resource.*;


/**
 * FlatFileDataWrapper provides an implementation of the
 * {@link FarragoMedDataWrapper} interface for reading from flat files.
 *
 * @author John V. Pham
 * @version $Id$
 */
public class FlatFileDataWrapper extends MedAbstractDataWrapper
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new data wrapper instance.
     */
    public FlatFileDataWrapper()
    {
    }

    //~ Methods ---------------------------------------------------------------

    // implement FarragoMedDataWrapper
    public String getSuggestedName()
    {
        return "FLATFILE_DATA_WRAPPER";
    }

    // implement FarragoMedDataWrapper
    public String getDescription(Locale locale)
    {
        // TODO: localize
        return "Foreign data wrapper for flatfile tables";
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
        FlatFileDataServer server = new FlatFileDataServer(
            this, serverMofId, props);
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


// End FlatFileDataWrapper.java
