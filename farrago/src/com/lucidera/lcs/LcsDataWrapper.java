/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
package com.lucidera.lcs;

import java.sql.*;
import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.resource.*;


/**
 * LcsDataWrapper implements the {@link FarragoMedDataWrapper}
 * interface for LucidDB column-store data.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LcsDataWrapper extends MedAbstractDataWrapper
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new data wrapper instance.
     */
    public LcsDataWrapper()
    {
    }

    //~ Methods ---------------------------------------------------------------

    // implement FarragoMedDataWrapper
    public String getSuggestedName()
    {
        return "LCS_DATA_WRAPPER";
    }

    // implement FarragoMedDataWrapper
    public String getDescription(Locale locale)
    {
        // TODO: localize
        return "Local data wrapper for LucidEra column-store data";
    }

    // TODO:  DriverPropertyInfo calls
    // implement FarragoMedDataWrapper
    public FarragoMedDataServer newServer(
        String serverMofId,
        Properties props)
        throws SQLException
    {
        return new LcsDataServer(
            serverMofId,
            props,
            getRepos());
    }

    // implement FarragoMedDataWrapper
    public boolean isForeign()
    {
        return false;
    }
}


// End LcsDataWrapper.java
