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
package net.sf.farrago.jdbc.engine;

import java.sql.*;
import java.util.*;
import java.util.logging.*;

import net.sf.farrago.db.*;
import net.sf.farrago.jdbc.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.util.*;
import net.sf.farrago.plugin.*;

/**
 * FarragoJdbcEngineDriver implements the Farrago engine/server side of
 * the {@link java.sql.Driver} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJdbcEngineDriver extends FarragoAbstractJdbcDriver
    implements FarragoJdbcServerDriver
{
    //~ Static fields/initializers --------------------------------------------

    private static final Logger tracer =
        FarragoTrace.getFarragoJdbcEngineDriverTracer();

    static {
        new FarragoJdbcEngineDriver().register();
        new FarragoJdbcRoutineDriver().register();
    }

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoJdbcEngineDriver object.
     */
    public FarragoJdbcEngineDriver()
    {
    }

    //~ Methods ---------------------------------------------------------------

    // implement FarragoAbstractJdbcDriver
    public String getUrlPrefix()
    {
        return getBaseUrl();
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
        String libraryName = 
            FarragoProperties.instance().defaultSessionFactoryLibraryName.get();
        try {
            FarragoPluginClassLoader classLoader =
                new FarragoPluginClassLoader();
            Class c = classLoader.loadClassFromLibraryManifest(
                libraryName,"SessionFactoryClassName");
            return (FarragoSessionFactory)
                classLoader.newPluginInstance(c);
        } catch (Throwable ex) {
            throw FarragoResource.instance().newPluginInitFailed(
                libraryName,ex);
        }
    }

    /**
     * Converter from any Throwable to SQLException.
     *
     * @param ex Throwable to be converted
     *
     * @return ex as a SQLException
     */
    static SQLException newSqlException(Throwable ex)
    {
        return FarragoUtil.newSqlException(ex, tracer);
    }
}


// End FarragoJdbcEngineDriver.java
