/*
// $Id$
// Firewater is a scaleout column store DBMS.
// Copyright (C) 2009-2009 John V. Sichi
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
package net.sf.firewater;

import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.jdbc.*;

import java.sql.*;
import java.util.*;

/**
 * FirewaterDataWrapper implements the
 * {@link net.sf.farrago.namespace.FarragoMedDataWrapper}
 * interface for Firewater distributed tables.
 *
 * @author John Sichi
 * @version $Id$
 */
public class FirewaterDataWrapper extends MedJdbcForeignDataWrapper
{
    // implement FarragoMedDataWrapper
    public String getSuggestedName()
    {
        return "FIREWATER_DATA_WRAPPER";
    }

    // implement FarragoMedDataWrapper
    public String getDescription(Locale locale)
    {
        // TODO: localize
        return "Local data wrapper for Firewater distributed tables";
    }

    // override MedJdbcForeignDataWrapper
    public boolean isForeign()
    {
        return false;
    }

    // override MedJdbcForeignDataWrapper
    public FarragoMedDataServer newServer(
        String serverMofId,
        Properties props)
        throws SQLException
    {
        String driverClassName = props.getProperty(PROP_DRIVER_CLASS_NAME);
        if (driverClassName != null) {
            loadDriverClass(driverClassName);
        }
        Properties chainedProps = new Properties(getProperties());
        chainedProps.putAll(props);
        FirewaterDataServer server =
            new FirewaterDataServer(serverMofId, chainedProps);
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

// End FirewaterDataWrapper.java
