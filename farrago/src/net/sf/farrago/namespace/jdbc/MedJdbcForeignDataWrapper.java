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
package net.sf.farrago.namespace.jdbc;

import java.sql.*;
import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.resource.*;


/**
 * MedJdbcForeignDataWrapper implements the FarragoMedDataWrapper
 * interface by accessing foreign tables provided by any JDBC driver.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class MedJdbcForeignDataWrapper extends MedAbstractDataWrapper
{
    //~ Static fields/initializers --------------------------------------------

    public static final String PROP_DRIVER_CLASS_NAME = "DRIVER_CLASS";

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new data wrapper instance.
     */
    public MedJdbcForeignDataWrapper()
    {
    }

    //~ Methods ---------------------------------------------------------------

    // implement FarragoMedDataWrapper
    public String getSuggestedName()
    {
        return "JDBC_DATA_WRAPPER";
    }

    // implement FarragoMedDataWrapper
    public String getDescription(Locale locale)
    {
        // TODO: localize
        return "Foreign data wrapper for JDBC data";
    }

    // TODO:  DriverPropertyInfo calls
    // implement FarragoMedDataWrapper
    public void initialize(
        FarragoRepos repos,
        Properties props)
        throws SQLException
    {
        super.initialize(repos, props);

        String driverClassName = props.getProperty(PROP_DRIVER_CLASS_NAME);
        if (driverClassName == null) {
            // REVIEW:  should we support connecting at the wrapper level,
            // and then allowing different servers to represent different
            // subsets of the data from the same connection?
            assert (props.isEmpty());
        } else {
            assert (props.size() == 1);
            loadDriverClass(driverClassName);
        }
    }

    private void loadDriverClass(String driverClassName)
    {
        try {
            Class.forName(driverClassName);
        } catch (ClassNotFoundException ex) {
            throw FarragoResource.instance().JdbcDriverLoadFailed.ex(driverClassName,
                ex);
        }
    }

    // implement FarragoMedDataWrapper
    public FarragoMedDataServer newServer(
        String serverMofId,
        Properties props)
        throws SQLException
    {
        String driverClassName = props.getProperty(PROP_DRIVER_CLASS_NAME);
        if (driverClassName != null) {
            loadDriverClass(driverClassName);
        }
        MedJdbcDataServer server = new MedJdbcDataServer(serverMofId, props);
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


// End MedJdbcForeignDataWrapper.java
