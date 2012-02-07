/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package net.sf.farrago.namespace.jdbc;

import java.sql.*;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.resource.*;

import org.eigenbase.util.*;


/**
 * MedJdbcForeignDataWrapper implements the FarragoMedDataWrapper interface by
 * accessing foreign tables provided by any JDBC driver.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class MedJdbcForeignDataWrapper
    extends MedAbstractDataWrapper
{
    //~ Static fields/initializers ---------------------------------------------

    public static final String PROP_DRIVER_CLASS_NAME = "DRIVER_CLASS";

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new data wrapper instance.
     */
    public MedJdbcForeignDataWrapper()
    {
    }

    //~ Methods ----------------------------------------------------------------

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

    // TODO:  more DriverPropertyInfo calls

    // implement FarragoMedDataWrapper
    public DriverPropertyInfo [] getServerPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps)
    {
        // TODO:  use locale

        DriverPropertyInfo [] driverArray = null;
        boolean extOpts =
            getBooleanProperty(
                serverProps,
                MedJdbcDataServer.PROP_EXT_OPTIONS,
                false);
        String url = serverProps.getProperty(MedJdbcDataServer.PROP_URL);
        if ((url != null) && extOpts) {
            // User has proposed a URL, and they want extended properties.
            // (Note that we ignore any template URL from the wrapper
            // definition.)  Attempt to use that to augment our own properties
            // with those specific to the driver.
            Properties driverProps = new Properties();
            driverProps.putAll(serverProps);
            MedJdbcDataServer.removeNonDriverProps(driverProps);
            try {
                final String className =
                    serverProps.getProperty(
                        PROP_DRIVER_CLASS_NAME,
                        wrapperProps.getProperty(
                            PROP_DRIVER_CLASS_NAME));
                Driver driver = loadDriverClass(className);
                driverArray = driver.getPropertyInfo(url, driverProps);
            } catch (Throwable ex) {
                // Squelch it and move on.
            }
        }

        // serverProps takes precedence over wrapperProps
        Properties chainedProps = new Properties(wrapperProps);
        chainedProps.putAll(serverProps);
        MedPropertyInfoMap infoMap =
            new MedPropertyInfoMap(
                FarragoResource.instance(),
                "MedJdbc",
                chainedProps);
        infoMap.addPropInfo(
            MedJdbcDataServer.PROP_DRIVER_CLASS,
            true);
        infoMap.addPropInfo(
            MedJdbcDataServer.PROP_URL,
            true);
        infoMap.addPropInfo(
            MedJdbcDataServer.PROP_USER_NAME);
        infoMap.addPropInfo(
            MedJdbcDataServer.PROP_PASSWORD);

        // TODO jvs 19-June-2006: Other properties like catalog name; how
        // much should we expose?  Make it leveled via an "ADVANCED" property,
        // use DatabaseMetaData to make it smart with choices, and let wrapper
        // definition override DatabaseMetaData.
        infoMap.addPropInfo(
            MedJdbcDataServer.PROP_EXT_OPTIONS,
            true,
            BOOLEAN_CHOICES_DEFAULT_FALSE);

        // determine if need SCHEMA_NAME property (e.g. MySQL)
        if (url != null) {
            FarragoMedDataServer server = null;
            try {
                server = newServer("faux-mofid", chainedProps);
                if (server instanceof MedJdbcDataServer) {
                    MedJdbcDataServer mjds = (MedJdbcDataServer) server;
                    String term = mjds.getDatabaseMetaData().getSchemaTerm();
                    if (mjds.useSchemaNameAsForeignQualifier
                        || (term == null)
                        || (term.length() == 0))
                    {
                        // add optional SCHEMA_NAME property
                        String [] list = null;
                        if (mjds.supportsMetaData) {
                            // collect names for list of choices
                            list =
                                getArtificialSchemas(
                                    mjds.getDatabaseMetaData());
                        }
                        infoMap.addPropInfo(
                            MedJdbcDataServer.PROP_SCHEMA_NAME,
                            false,
                            list);
                    }
                }
            } catch (SQLException e) {
                // swallow and ignore
                Util.swallow(e, null);
            } finally {
                if (server != null) {
                    server.releaseResources();
                    server.closeAllocation();
                }
            }
        }

        DriverPropertyInfo [] mapArray = infoMap.toArray();
        if (driverArray == null) {
            return mapArray;
        } else {
            DriverPropertyInfo [] result =
                new DriverPropertyInfo[mapArray.length + driverArray.length];
            System.arraycopy(mapArray, 0, result, 0, mapArray.length);
            System.arraycopy(
                driverArray,
                0,
                result,
                mapArray.length,
                driverArray.length);
            return result;
        }
    }

    // implement FarragoMedDataWrapper
    public void initialize(
        FarragoRepos repos,
        Properties props)
        throws SQLException
    {
        super.initialize(repos, props);

        // ignore other props like BROWSE_CONNECT_DESCRIPTION
        String driverClassName = props.getProperty(PROP_DRIVER_CLASS_NAME);
        if (driverClassName != null) {
            loadDriverClass(driverClassName);
        }
    }

    protected Driver loadDriverClass(String driverClassName)
    {
        try {
            Class<?> clazz = Class.forName(driverClassName);
            return (Driver) clazz.newInstance();
        } catch (Exception ex) {
            throw FarragoResource.instance().JdbcDriverLoadFailed.ex(
                driverClassName,
                ex);
        }
    }

    // gets database names for use as SCHEMA_NAME values
    private String [] getArtificialSchemas(DatabaseMetaData meta)
        throws SQLException
    {
        // REVIEW hersker 2005-05-31:
        // Testing with MySQL 5.0 shows that DatabaseMetaData.getSchemas()
        // returns an empty ResultSet, but DatabaseMetadata.getCatalogs()
        // returns database names that can be used as schema qualifiers.
        // Other tested DBs (Oracle 10.2g, SQL Server 2005, PostgreSQL 8.0)
        // do not need artificial schema names.
        List<String> list = new ArrayList<String>();
        ResultSet rs = meta.getCatalogs();
        try {
            while (rs.next()) {
                String name = rs.getString(1);
                list.add(name);
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
        }

        if (list.size() == 0) {
            return null;
        }

        String [] schemas = new String[list.size()];
        list.toArray(schemas);
        Arrays.sort(schemas);
        return schemas;
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
        Properties chainedProps = new Properties(getProperties());
        chainedProps.putAll(props);
        MedJdbcDataServer server = newServerImpl(serverMofId, chainedProps);
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

    protected MedJdbcDataServer newServerImpl(
        String serverMofId, Properties chainedProps)
    {
        MedJdbcDataServer server =
            new MedJdbcDataServer(serverMofId, chainedProps);
        return server;
    }

}

// End MedJdbcForeignDataWrapper.java
