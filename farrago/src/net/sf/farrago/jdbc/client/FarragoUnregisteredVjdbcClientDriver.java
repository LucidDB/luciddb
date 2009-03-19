/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2009 The Eigenbase Project
// Copyright (C) 2006-2009 SQLstream, Inc.
// Copyright (C) 2006-2009 LucidEra, Inc.
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
package net.sf.farrago.jdbc.client;

import de.simplicit.vjdbc.*;

import java.sql.*;

import java.util.*;

import net.sf.farrago.jdbc.*;
import net.sf.farrago.release.*;


/**
 * FarragoUnregisteredJdbcClientDriver implements the Farrago client side of the
 * {@link java.sql.Driver} interface via the VJDBC proxy. It does not register
 * itself; for that, use {@link FarragoVjdbcClientDriver}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoUnregisteredVjdbcClientDriver
    extends FarragoAbstractJdbcDriver
{
    //~ Methods ----------------------------------------------------------------

    /**
     * @return the prefix for JDBC URL's understood by this driver; subclassing
     * drivers can override this to customize the URL scheme
     */
    public String getUrlPrefix()
    {
        return getBaseUrl() + "rmi://";
    }

    // implement Driver
    public Connection connect(
        String url,
        Properties info)
        throws SQLException
    {
        if (!acceptsURL(url)) {
            return null;
        }

        // connection property precedence:
        // connect string (URI), info props, connection defaults

        // don't modify user's properties:
        //  copy input props backed by connection defaults,
        //  move any params from the URI to the properties
        Properties driverProps = applyDefaultConnectionProps(info);
        String driverUrl = parseConnectionParams(url, driverProps);

        Driver rmiDriver;
        try {
            rmiDriver = new VirtualDriver();
        } catch (Exception ex) {
            // TODO: use FarragoJdbcUtil.newSqlException, see Jira FRG-122
            throw new SQLException(ex.getMessage());
        }

        // transform the URL into a form understood by VJDBC
        String urlRmi = driverUrl.substring(getUrlPrefix().length());
        String [] split = urlRmi.split(":");
        if (split.length == 1) {
            // no port number, so append default
            FarragoReleaseProperties props =
                FarragoReleaseProperties.instance();
            urlRmi = urlRmi + ":" + props.jdbcUrlPortDefault.get();
        }
        urlRmi = "jdbc:vjdbc:rmi://" + urlRmi + "/VJdbc,FarragoDBMS";

        // NOTE:  can't call DriverManager.connect here, because that
        // would deadlock in the case where client and server are
        // running in the same VM
        return rmiDriver.connect(urlRmi, driverProps);
    }
}

// End FarragoUnregisteredVjdbcClientDriver.java
