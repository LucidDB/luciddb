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

package net.sf.farrago.jdbc.engine;

import net.sf.farrago.jdbc.*;
import net.sf.farrago.util.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.session.*;
import net.sf.farrago.db.*;
import net.sf.farrago.resource.*;
import net.sf.saffron.util.SaffronException;

import java.sql.*;

import java.util.*;
import java.util.logging.*;

import koala.dynamicjava.interpreter.error.*;

/**
 * FarragoJdbcEngineDriver implements the Farrago engine/server side of
 * the {@link java.sql.Driver} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJdbcEngineDriver
    extends FarragoAbstractJdbcDriver
    implements FarragoJdbcServerDriver
{
    private static final Logger tracer =
        FarragoTrace.getFarragoJdbcEngineDriverTracer();

    static {
        new FarragoJdbcEngineDriver().register();
    }

    /**
     * Creates a new FarragoJdbcEngineDriver object.
     */
    public FarragoJdbcEngineDriver()
    {
    }

    /**
     * @return the prefix for JDBC URL's understood by this driver;
     * subclassing drivers can override this to customize the URL scheme
     */
    public String getUrlPrefix()
    {
        return getBaseUrl();
    }
    
    // implement Driver
    public Connection connect(String url,Properties info)
        throws SQLException
    {
        if (!acceptsURL(url)) {
            return null;
        }

        try {
            if (url.equals(getBaseUrl()) || url.equals(getClientUrl())) {
                return new FarragoJdbcEngineConnection(
                    url,
                    info,
                    newSessionFactory());
            } else {
                throw FarragoResource.instance().newJdbcInvalidUrl(url);
            }
        } catch (Throwable ex) {
            throw newSqlException(ex);
        }
    }

    // implement FarragoJdbcServerDriver
    public FarragoSessionFactory newSessionFactory()
    {
        return new FarragoDbSessionFactory();
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
        tracer.throwing("FarragoJdbcEngineDriver","newSqlException",ex);

        if (ex instanceof CatchedExceptionError) {
            ex = ((CatchedExceptionError) ex).getException();
        }
        
        SQLException sqlExcn;
        if (ex instanceof FarragoException || ex instanceof SaffronException) {
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

// End FarragoJdbcEngineDriver.java
