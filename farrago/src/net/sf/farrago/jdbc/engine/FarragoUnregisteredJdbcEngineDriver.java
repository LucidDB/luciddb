/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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

import net.sf.farrago.jdbc.*;
import net.sf.farrago.plugin.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.util.*;

/**
 * FarragoJdbcEngineDriver implements the Farrago engine/server side of the
 * {@link java.sql.Driver} interface.  It does not register itself;
 * for that, use {@link FarragoJdbcEngineDriver}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoUnregisteredJdbcEngineDriver
    extends FarragoAbstractJdbcDriver
    implements FarragoJdbcServerDriver
{

    //~ Static fields/initializers ---------------------------------------------

    private static final Logger tracer =
        FarragoTrace.getFarragoJdbcEngineDriverTracer();

    static {
        // NOTE jvs 29-July-2006:  Even though we don't register ourselves,
        // we do register the loopback driver, because that always needs
        // to be registered regardless of which driver is being used to
        // access the engine from outside.
        new FarragoJdbcRoutineDriver().register();
    }

    //~ Methods ----------------------------------------------------------------

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

        // connection property precedence:
        // connect string (URI), info props, connection defaults

        // don't modify user's properties:
        //  copy input props backed by connection defaults,
        //  move any params from the URI to the properties
        Properties driverProps = applyDefaultConnectionProps(info);
        String driverUrl = parseConnectionParams(url, driverProps);

        try {
            if (driverUrl.equals(getBaseUrl())
                || driverUrl.equals(getClientUrl())) {
                return
                    new FarragoJdbcEngineConnection(
                        driverUrl,
                        driverProps,
                        newSessionFactory());
            } else {
                throw FarragoResource.instance().JdbcInvalidUrl.ex(driverUrl);
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
            Class c =
                classLoader.loadClassFromLibraryManifest(
                    libraryName,
                    "SessionFactoryClassName");
            return (FarragoSessionFactory) classLoader.newPluginInstance(c);
        } catch (Throwable ex) {
            throw FarragoResource.instance().PluginInitFailed.ex(
                libraryName,
                ex);
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
        return FarragoJdbcUtil.newSqlException(ex, tracer);
    }
}

// End FarragoUnregisteredJdbcEngineDriver.java
