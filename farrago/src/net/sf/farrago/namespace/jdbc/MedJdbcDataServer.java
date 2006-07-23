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
package net.sf.farrago.namespace.jdbc;

import java.lang.reflect.*;

import java.sql.*;

import java.util.*;

import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.query.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.rel.jdbc.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;


// TODO:  throw exception on unknown option?

/**
 * MedJdbcDataServer implements the {@link FarragoMedDataServer} interface for
 * JDBC data.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedJdbcDataServer
    extends MedAbstractDataServer
{

    //~ Static fields/initializers ---------------------------------------------

    public static final String PROP_URL = "URL";
    public static final String PROP_DRIVER_CLASS = "DRIVER_CLASS";
    public static final String PROP_USER_NAME = "USER_NAME";
    public static final String PROP_PASSWORD = "PASSWORD";
    public static final String PROP_CATALOG_NAME = "QUALIFYING_CATALOG_NAME";
    public static final String PROP_SCHEMA_NAME = "SCHEMA_NAME";
    public static final String PROP_TABLE_NAME = "TABLE_NAME";
    public static final String PROP_TABLE_TYPES = "TABLE_TYPES";
    public static final String PROP_EXT_OPTIONS = "EXTENDED_OPTIONS";
    public static final String PROP_TYPE_SUBSTITUTION = "TYPE_SUBSTITUTION";

    // REVIEW jvs 19-June-2006:  What are these doing here?
    public static final String PROP_VERSION = "VERSION";
    public static final String PROP_NAME = "NAME";
    public static final String PROP_TYPE = "TYPE";

    //~ Instance fields --------------------------------------------------------

    // TODO:  add a parameter for JNDI lookup of a DataSource so we can support
    // app servers and distributed txns
    Connection connection;
    String url;
    String catalogName;
    String schemaName;
    String [] tableTypes;
    boolean supportsMetaData;
    DatabaseMetaData databaseMetaData;

    //~ Constructors -----------------------------------------------------------

    MedJdbcDataServer(
        String serverMofId,
        Properties props)
    {
        super(serverMofId, props);
    }

    //~ Methods ----------------------------------------------------------------

    void initialize()
        throws SQLException
    {
        Properties props = getProperties();
        Properties connectProps = null;
        requireProperty(props, PROP_URL);
        url = props.getProperty(PROP_URL);
        String userName = props.getProperty(PROP_USER_NAME);
        String password = props.getProperty(PROP_PASSWORD);
        schemaName = props.getProperty(PROP_SCHEMA_NAME);
        catalogName = props.getProperty(PROP_CATALOG_NAME);

        if (getBooleanProperty(props, PROP_EXT_OPTIONS, false)) {
            connectProps = (Properties) props.clone();
            removeNonDriverProps(connectProps);
        }

        String tableTypeString = props.getProperty(PROP_TABLE_TYPES);
        if (tableTypeString == null) {
            tableTypes = null;
        } else {
            tableTypes = tableTypeString.split(",");
        }

        if (connectProps != null) {
            connection = DriverManager.getConnection(url, connectProps);
        } else if (userName == null) {
            connection = DriverManager.getConnection(url);
        } else {
            connection = DriverManager.getConnection(url, userName, password);
        }
        try {
            databaseMetaData = connection.getMetaData();
            supportsMetaData = true;
        } catch (Exception ex) {
            // driver can't even support getMetaData(); treat it
            // as brain-damaged
            databaseMetaData =
                (DatabaseMetaData) Proxy.newProxyInstance(
                    null,
                    new Class[] { DatabaseMetaData.class },
                    new SqlUtil.DatabaseMetaDataInvocationHandler(
                        "UNKNOWN",
                        ""));
        }
    }

    static void removeNonDriverProps(Properties props)
    {
        // TODO jvs 19-June-2006:  Make this metadata-driven.
        props.remove(PROP_URL);
        props.remove(PROP_DRIVER_CLASS);
        props.remove(PROP_SCHEMA_NAME);
        props.remove(PROP_VERSION);
        props.remove(PROP_NAME);
        props.remove(PROP_TYPE);
        props.remove(PROP_EXT_OPTIONS);
        props.remove(PROP_TYPE_SUBSTITUTION);
    }

    // implement FarragoMedDataServer
    public FarragoMedNameDirectory getNameDirectory()
        throws SQLException
    {
        return getSchemaNameDirectory();
    }

    private MedJdbcNameDirectory getSchemaNameDirectory()
    {
        return new MedJdbcNameDirectory(this);
    }

    // implement FarragoMedDataServer
    public FarragoMedColumnSet newColumnSet(
        String [] localName,
        Properties tableProps,
        FarragoTypeFactory typeFactory,
        RelDataType rowType,
        Map columnPropMap)
        throws SQLException
    {
        assert (connection != null);
        if (schemaName == null) {
            requireProperty(tableProps, PROP_SCHEMA_NAME);
        }
        String tableSchemaName = tableProps.getProperty(PROP_SCHEMA_NAME);
        if (tableSchemaName == null) {
            tableSchemaName = schemaName;
        } else if (schemaName != null) {
            if (!tableSchemaName.equals(schemaName)) {
                throw FarragoResource.instance().MedPropertyMismatch.ex(
                    schemaName,
                    tableSchemaName,
                    PROP_SCHEMA_NAME);
            }
        }
        requireProperty(tableProps, PROP_TABLE_NAME);
        String tableName = tableProps.getProperty(PROP_TABLE_NAME);
        MedJdbcNameDirectory directory =
            new MedJdbcNameDirectory(this, tableSchemaName);
        return
            directory.lookupColumnSetAndImposeType(
                typeFactory,
                tableName,
                localName,
                rowType);
    }

    // implement FarragoMedDataServer
    public Object getRuntimeSupport(Object param)
        throws SQLException
    {
        assert (connection != null);

        String sql = (String) param;
        Statement stmt = connection.createStatement();
        FarragoStatementAllocation stmtAlloc =
            new FarragoStatementAllocation(stmt);
        try {
            stmtAlloc.setResultSet(stmt.executeQuery(sql));
            stmt = null;
            return stmtAlloc;
        } finally {
            if (stmt != null) {
                stmtAlloc.closeAllocation();
            }
        }
    }

    // implement FarragoMedDataServer
    public void registerRules(RelOptPlanner planner)
    {
        super.registerRules(planner);
        JdbcQuery.register(planner);

        // tell optimizer how to convert data from JDBC into Farrago
        planner.addRule(
            new ConverterRule(RelNode.class,
                CallingConvention.RESULT_SET,
                CallingConvention.ITERATOR,
                "ResultSetToFarragoIteratorRule") {
                public RelNode convert(RelNode rel)
                {
                    return
                        new ResultSetToFarragoIteratorConverter(
                            rel.getCluster(),
                            rel);
                }

                public boolean isGuaranteed()
                {
                    return true;
                }
            });

        // optimizer sometimes can't figure out how to convert data
        // from JDBC directly into Fennel, so help it out
        planner.addRule(
            new ConverterRule(RelNode.class,
                CallingConvention.RESULT_SET,
                FennelRel.FENNEL_EXEC_CONVENTION,
                "ResultSetToFennelRule") {
                public RelNode convert(RelNode rel)
                {
                    return
                        new IteratorToFennelConverter(
                            rel.getCluster(),
                            new ResultSetToFarragoIteratorConverter(
                                rel.getCluster(),
                                rel));
                }

                public boolean isGuaranteed()
                {
                    return true;
                }
            });
    }

    // implement FarragoAllocation
    public void closeAllocation()
    {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ex) {
                // TODO:  trace?
            }
        }
    }
}

// End MedJdbcDataServer.java
