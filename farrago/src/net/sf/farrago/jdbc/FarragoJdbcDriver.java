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

package net.sf.farrago.jdbc;

import net.sf.farrago.util.*;
import net.sf.farrago.session.*;
import net.sf.farrago.db.*;

import java.sql.*;

import java.util.*;
import java.util.logging.*;

import koala.dynamicjava.interpreter.error.*;

/**
 * FarragoJdbcDriver implements the {@link java.sql.Driver} interface as
 * the Farrago JDBC driver.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJdbcDriver implements Driver
{
    //~ Static fields/initializers --------------------------------------------

    private static Logger tracer =
        TraceUtil.getClassTrace(FarragoJdbcDriver.class);

    static {
        new FarragoJdbcDriver().register();
    }

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoJdbcDriver object.
     */
    public FarragoJdbcDriver()
    {
    }

    //~ Methods ---------------------------------------------------------------

    // implement Driver
    public boolean jdbcCompliant()
    {
        // TODO:  true once we pass compliance tests and SQL92 entry level
        return false;
    }
    
    private String getUrlPrefix()
    {
        return "jdbc:farrago:";
    }

    // implement Driver
    public int getMajorVersion()
    {
        return 0;
    }

    // implement Driver
    public int getMinorVersion()
    {
        return 0;
    }

    // implement Driver
    public DriverPropertyInfo [] getPropertyInfo(String url,Properties info)
        throws SQLException
    {
        // TODO
        return new DriverPropertyInfo[0];
    }

    // implement Driver
    public boolean acceptsURL(String url) throws SQLException
    {
        return url.startsWith(getUrlPrefix());
    }

    // implement Driver
    public Connection connect(String url,Properties info)
        throws SQLException
    {
        if (!acceptsURL(url)) {
            return null;
        }

        try {
            return new FarragoJdbcConnection(
                url,
                info,
                newSessionFactory());
        } catch (Throwable ex) {
            throw newSqlException(ex);
        }
    }

    /**
     * Creates a new FarragoSessionFactory which will govern the behavior
     * of connections established through this driver.  Subclassing
     * drivers can override this to customize Farrago behavior.
     *
     * @return new factory (FarragoDbSessionFactory by default)
     */
    protected FarragoSessionFactory newSessionFactory()
    {
        return new FarragoDbSessionFactory();
    }
    
    private void register()
    {
        try {
            DriverManager.registerDriver(this);
        } catch (SQLException e) {
            System.out.println(
                "Error occurred while registering JDBC driver " + this + ": "
                + e.toString());
        }
    }
    
    /**
     * Converter from any Exception to SQLException.
     *
     * @param ex Throwable to be converted
     *
     * @return ex as a SQLException
     */
    static SQLException newSqlException(Throwable ex)
    {
        tracer.severe(ex.getMessage());
        tracer.throwing("FarragoJdbcDriver","newSqlException",ex);

        if (ex instanceof CatchedExceptionError) {
            ex = ((CatchedExceptionError) ex).getException();
        }
        
        SQLException sqlExcn;
        if (ex instanceof FarragoException) {
            // TODO:  map for SQLState
            sqlExcn = new SQLException(ex.getMessage());
        } else if (ex instanceof SQLException) {
            sqlExcn = (SQLException) ex;
        } else {
            // for anything else, include the class name
            // as part of what went wrong
            sqlExcn = new SQLException(
                ex.getClass().getName() + ": " + ex.getMessage());
        }

        // preserve additional attributes of the original excn
        sqlExcn.setStackTrace(ex.getStackTrace());

        // convert to SQLException-style chaining
        Throwable cause = ex.getCause();
        if (cause != null) {
            SQLException sqlCause = newSqlException(cause);
            sqlExcn.setNextException(sqlCause);
        }
        return sqlExcn;
    }
}


// End FarragoJdbcDriver.java
