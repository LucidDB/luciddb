/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
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

package net.sf.saffron.jdbc;

import net.sf.saffron.util.Util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import java.sql.*;

import java.util.Properties;


/**
 * A <code>SaffronJdbcDriver</code> is a JDBC {@link Driver} for a Saffron
 * database.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since Mar 19, 2003
 */
public class SaffronJdbcDriver implements Driver
{
    //~ Static fields/initializers --------------------------------------------

    private static final DriverPropertyInfo [] propertyInfo =
        new DriverPropertyInfo[0];

    static {
        new SaffronJdbcDriver().register();
    }

    //~ Constructors ----------------------------------------------------------

    public SaffronJdbcDriver()
    {
    }

    //~ Methods ---------------------------------------------------------------

    public int getMajorVersion()
    {
        return 0;
    }

    public int getMinorVersion()
    {
        return 0;
    }

    public DriverPropertyInfo [] getPropertyInfo(String url,Properties info)
        throws SQLException
    {
        return propertyInfo;
    }

    public boolean acceptsURL(String url) throws SQLException
    {
        return url.startsWith(getUrlPrefix());
    }

    public Connection connect(String url,Properties info)
        throws SQLException
    {
        if (!acceptsURL(url)) {
            return null;
        }

        // Use reflection, to avoid pulling in too many dependencies when the
        // driver is loaded.
        try {
            Class clazz = Class.forName(getConnectionClassName());
            Constructor constructor =
                clazz.getConstructor(
                    new Class [] { String.class,Properties.class });
            try {
                final Connection connection =
                    (Connection) constructor.newInstance(
                        new Object [] { url,info });
                return connection;
            } catch (InvocationTargetException ex) {
                // NOTE jvs 12-Jun-2003:  specifically unwrap
                // InvocationTargetException so that in case of a problem
                // during construction we get a nested stack trace
                throw Util.newInternal(ex);
            }
        } catch (Throwable e) {
            throw new SQLException(
                "Error while loading connection class: " + e);
        }
    }

    public boolean jdbcCompliant()
    {
        return true;
    }

    protected String getConnectionClassName()
    {
        return "net.sf.saffron.jdbc.SaffronJdbcConnection";
    }

    protected String getUrlPrefix()
    {
        return getUrlPrefixStatic();
    }

    /**
     * Implicitly registers this driver when class is loaded (for example, if
     * someone executes 'Class.forName("saffron.jdbc.SaffronJdbcDriver")')
     */
    protected void register()
    {
        try {
            DriverManager.registerDriver(this);
        } catch (SQLException e) {
            System.out.println(
                "Error occurred while registering JDBC driver " + this + ": "
                + e.toString());
        }
    }

    static String getUrlPrefixStatic()
    {
        return "jdbc:saffron:";
    }
}


// End SaffronJdbcDriver.java
