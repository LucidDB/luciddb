/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
package net.sf.farrago.jdbc;

import java.sql.*;
import java.util.*;


/**
 * FarragoAbstractJdbcDriver is an abstract base for the client and engine
 * sides of the Farrago JDBC driver.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoAbstractJdbcDriver implements Driver
{
    //~ Methods ---------------------------------------------------------------

    // implement Driver
    public boolean jdbcCompliant()
    {
        // TODO:  true once we pass compliance tests and SQL92 entry level
        return false;
    }

    /**
     * @return the prefix for JDBC URL's understood by this driver
     */
    public abstract String getUrlPrefix();

    /**
     * @return the base JDBC URL for this driver;
     * subclassing drivers can override this to customize the URL scheme
     */
    public String getBaseUrl()
    {
        return "jdbc:farrago:";
    }

    /**
     * @return the JDBC URL interpreted by the engine driver
     * as a connection from an RMI client; subclassing drivers
     * can override this to customize the URL scheme
     */
    public String getClientUrl()
    {
        // NOTE jvs 27-March-2005:  At the moment, the driver interprets
        // embedded and client URL's as the same.  However, we distinguish
        // the actual URL's since in the future we may want to
        // react to them differently.
        return getBaseUrl() + "client_rmi";
    }

    // implement Driver
    public int getMajorVersion()
    {
        // TODO
        return 0;
    }

    // implement Driver
    public int getMinorVersion()
    {
        // TODO
        return 0;
    }

    // implement Driver
    public DriverPropertyInfo [] getPropertyInfo(
        String url,
        Properties info)
        throws SQLException
    {
        // TODO
        return new DriverPropertyInfo[0];
    }

    // implement Driver
    public boolean acceptsURL(String url)
        throws SQLException
    {
        return url.startsWith(getUrlPrefix());
    }

    public void register()
    {
        try {
            DriverManager.registerDriver(this);
        } catch (SQLException e) {
            System.out.println("Error occurred while registering JDBC driver "
                + this + ": " + e.toString());
        }
    }
}


// End FarragoAbstractJdbcDriver.java
