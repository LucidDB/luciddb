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

import net.sf.saffron.jdbc.*;

import java.sql.*;

import java.util.*;
import java.util.logging.*;

import koala.dynamicjava.interpreter.error.*;

/**
 * FarragoJdbcDriver subclasses SaffronJdbcDriver to implement
 * Farrago-specific details.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJdbcDriver extends SaffronJdbcDriver
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
    
    // implement SaffronJdbcDriver
    protected String getConnectionClassName()
    {
        return "net.sf.farrago.jdbc.FarragoJdbcConnection";
    }

    // implement SaffronJdbcDriver
    protected String getUrlPrefix()
    {
        return getUrlPrefixStatic();
    }

    // implement SaffronJdbcDriver
    static String getUrlPrefixStatic()
    {
        return "jdbc:farrago:";
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
