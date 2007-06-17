/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
import java.util.regex.*;

import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.query.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.rel.jdbc.*;
import org.eigenbase.rel.metadata.*;
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
public class MedJdbcDataServer
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
    public static final String PROP_OBJECT = "OBJECT";
    public static final String PROP_TABLE_TYPES = "TABLE_TYPES";
    public static final String PROP_EXT_OPTIONS = "EXTENDED_OPTIONS";
    public static final String PROP_TYPE_SUBSTITUTION = "TYPE_SUBSTITUTION";
    public static final String PROP_TYPE_MAPPING = "TYPE_MAPPING";
    public static final String PROP_LOGIN_TIMEOUT = "LOGIN_TIMEOUT";
    public static final String PROP_VALIDATION_QUERY = "VALIDATION_QUERY";
    public static final String PROP_FETCH_SIZE = "FETCH_SIZE";
    public static final String PROP_AUTOCOMMIT = "AUTOCOMMIT";
    public static final String PROP_USE_SCHEMA_NAME_AS_FOREIGN_QUALIFIER =
        "USE_SCHEMA_NAME_AS_FOREIGN_QUALIFIER";
    public static final String PROP_LENIENT = "LENIENT";
    public static final String PROP_DISABLED_PUSHDOWN_REL_PATTERN =
        "DISABLED_PUSHDOWN_REL_PATTERN";
    public static final String PROP_SCHEMA_MAPPING = "SCHEMA_MAPPING";

    // REVIEW jvs 19-June-2006:  What are these doing here?
    public static final String PROP_VERSION = "VERSION";
    public static final String PROP_NAME = "NAME";
    public static final String PROP_TYPE = "TYPE";

    public static final boolean DEFAULT_USE_SCHEMA_NAME_AS_FOREIGN_QUALIFIER =
        false;
    public static final boolean DEFAULT_LENIENT = false;
    public static final String DEFAULT_DISABLED_PUSHDOWN_REL_PATTERN = "";
    public static final int DEFAULT_FETCH_SIZE = -1;
    public static final boolean DEFAULT_AUTOCOMMIT = true;

    //~ Instance fields --------------------------------------------------------

    // TODO:  add a parameter for JNDI lookup of a DataSource so we can support
    // app servers and distributed txns
    protected Connection connection;
    protected Properties connectProps;
    protected String userName;
    protected String password;
    protected String url;
    protected String catalogName;
    protected String schemaName;
    protected String [] tableTypes;
    protected String loginTimeout;
    protected boolean supportsMetaData;
    protected DatabaseMetaData databaseMetaData;
    protected boolean validateConnection = false;
    protected String validationQuery;
    protected boolean useSchemaNameAsForeignQualifier;
    protected boolean lenient;
    protected Pattern disabledPushdownPattern;
    private int fetchSize;
    private boolean autocommit;
    protected HashMap schemaMaps;

    //~ Constructors -----------------------------------------------------------

    protected MedJdbcDataServer(
        String serverMofId,
        Properties props)
    {
        super(serverMofId, props);
    }

    //~ Methods ----------------------------------------------------------------

    public void initialize()
        throws SQLException
    {
        Properties props = getProperties();
        connectProps = null;
        requireProperty(props, PROP_URL);
        url = props.getProperty(PROP_URL);
        userName = props.getProperty(PROP_USER_NAME);
        password = props.getProperty(PROP_PASSWORD);
        schemaName = props.getProperty(PROP_SCHEMA_NAME);
        catalogName = props.getProperty(PROP_CATALOG_NAME);
        loginTimeout = props.getProperty(PROP_LOGIN_TIMEOUT);
        validationQuery = props.getProperty(PROP_VALIDATION_QUERY);
        schemaMaps = new HashMap<String, HashMap>();

        if (getBooleanProperty(props, PROP_EXT_OPTIONS, false)) {
            connectProps = (Properties) props.clone();
            removeNonDriverProps(connectProps);
        }

        useSchemaNameAsForeignQualifier =
            getBooleanProperty(
                props,
                PROP_USE_SCHEMA_NAME_AS_FOREIGN_QUALIFIER,
                DEFAULT_USE_SCHEMA_NAME_AS_FOREIGN_QUALIFIER);

        lenient =
            getBooleanProperty(
                props,
                PROP_LENIENT,
                DEFAULT_LENIENT);

        disabledPushdownPattern =
            Pattern.compile(
                props.getProperty(
                    PROP_DISABLED_PUSHDOWN_REL_PATTERN,
                    DEFAULT_DISABLED_PUSHDOWN_REL_PATTERN));

        String tableTypeString = props.getProperty(PROP_TABLE_TYPES);
        if (tableTypeString == null) {
            tableTypes = null;
        } else {
            tableTypes = tableTypeString.split(",");
        }

        if (loginTimeout != null) {
            try {
                DriverManager.setLoginTimeout(Integer.parseInt(loginTimeout));
            } catch (NumberFormatException ne) {
                // ignore the timeout
            }
        }

        fetchSize = getIntProperty(props, PROP_FETCH_SIZE, DEFAULT_FETCH_SIZE);
        autocommit =
            getBooleanProperty(props, PROP_AUTOCOMMIT, DEFAULT_AUTOCOMMIT);

        createConnection();

        // schema mapping
        String schemaMapping = props.getProperty(PROP_SCHEMA_MAPPING);
        if (schemaMapping != null) {
            createSchemaMaps(schemaMapping);
        }
    }

    protected void createConnection()
        throws SQLException
    {
        if ((connection != null) && !connection.isClosed()) {
            if (validateConnection && (validationQuery != null)) {
                Statement testStatement = connection.createStatement();
                try {
                    testStatement.executeQuery(validationQuery);
                } catch (Exception ex) {
                    // need to re-create connection
                    closeAllocation();
                    connection = null;
                    validateConnection = false;
                } finally {
                    if (testStatement != null) {
                        try {
                            testStatement.close();
                        } catch (SQLException ex) {
                            // do nothing
                        }
                    }
                }
            } else {
                return;
            }

            if (validateConnection) { // validation query successful
                validateConnection = false;
                return;
            }
        }

        if (connectProps != null) {
            if (userName != null) {
                connectProps.setProperty("user", userName);
            }
            if (password != null) {
                connectProps.setProperty("password", password);
            }
            connection = DriverManager.getConnection(url, connectProps);
        } else if (userName == null) {
            connection = DriverManager.getConnection(url);
        } else {
            connection = DriverManager.getConnection(url, userName, password);
        }
        if (!autocommit) {
            connection.setAutoCommit(false);
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

    public Connection getConnection()
        throws SQLException
    {
        createConnection();
        return connection;
    }

    protected static void removeNonDriverProps(Properties props)
    {
        // TODO jvs 19-June-2006:  Make this metadata-driven.
        props.remove(PROP_URL);
        props.remove(PROP_DRIVER_CLASS);
        props.remove(PROP_CATALOG_NAME);
        props.remove(PROP_SCHEMA_NAME);
        props.remove(PROP_USER_NAME);
        props.remove(PROP_PASSWORD);
        props.remove(PROP_VERSION);
        props.remove(PROP_NAME);
        props.remove(PROP_TYPE);
        props.remove(PROP_EXT_OPTIONS);
        props.remove(PROP_TYPE_SUBSTITUTION);
        props.remove(PROP_TYPE_MAPPING);
        props.remove(PROP_TABLE_TYPES);
        props.remove(PROP_LOGIN_TIMEOUT);
        props.remove(PROP_USE_SCHEMA_NAME_AS_FOREIGN_QUALIFIER);
        props.remove(PROP_LENIENT);
        props.remove(PROP_DISABLED_PUSHDOWN_REL_PATTERN);
        props.remove(PROP_FETCH_SIZE);
        props.remove(PROP_AUTOCOMMIT);
        props.remove(PROP_SCHEMA_MAPPING);
    }

    // implement FarragoMedDataServer
    public FarragoMedNameDirectory getNameDirectory()
        throws SQLException
    {
        return getSchemaNameDirectory();
    }

    protected MedJdbcNameDirectory getSchemaNameDirectory()
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
        } else if ((schemaName != null) && !useSchemaNameAsForeignQualifier) {
            if (!tableSchemaName.equals(schemaName)) {
                throw FarragoResource.instance().MedPropertyMismatch.ex(
                    schemaName,
                    tableSchemaName,
                    PROP_SCHEMA_NAME);
            }
        }

        String tableName = tableProps.getProperty(PROP_OBJECT);
        if (tableName == null) {
            requireProperty(tableProps, PROP_TABLE_NAME);
            tableName = tableProps.getProperty(PROP_TABLE_NAME);
        }
        MedJdbcNameDirectory directory =
            new MedJdbcNameDirectory(this, tableSchemaName);
        return directory.lookupColumnSetAndImposeType(
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
        Statement stmt = getConnection().createStatement();
        FarragoStatementAllocation stmtAlloc =
            new FarragoStatementAllocation(stmt);
        try {
            if (fetchSize != DEFAULT_FETCH_SIZE) {
                stmt.setFetchSize(fetchSize);
            }
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
    public void registerRelMetadataProviders(ChainedRelMetadataProvider chain)
    {
        chain.addProvider(new MedJdbcMetadataProvider());
    }

    // implement FarragoMedDataServer
    public void registerRules(RelOptPlanner planner)
    {
        super.registerRules(planner);
        JdbcQuery.register(planner);

        // tell optimizer how to convert data from JDBC into Farrago
        planner.addRule(
            new ConverterRule(
                RelNode.class,
                CallingConvention.RESULT_SET,
                CallingConvention.ITERATOR,
                "ResultSetToFarragoIteratorRule") {
                public RelNode convert(RelNode rel)
                {
                    return new ResultSetToFarragoIteratorConverter(
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
            new ConverterRule(
                RelNode.class,
                CallingConvention.RESULT_SET,
                FennelRel.FENNEL_EXEC_CONVENTION,
                "ResultSetToFennelRule") {
                public RelNode convert(RelNode rel)
                {
                    return new IteratorToFennelConverter(
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

        // case 1: projection on top of a filter (with push down projection)
        // ie: filtering on variables which are not in projection
        MedJdbcPushDownRule r1 =
            new MedJdbcPushDownRule(
                new RelOptRuleOperand(
                    ProjectRel.class,
                    new RelOptRuleOperand[] {
                        new RelOptRuleOperand(
                            FilterRel.class,
                            new RelOptRuleOperand[] {
                                new RelOptRuleOperand(
                                    ProjectRel.class,
                                    new RelOptRuleOperand[] {
                                        new RelOptRuleOperand(
                                            MedJdbcQueryRel.class,
                                            null)
                                    })
                            })
                    }),
                "proj on filter on proj");

        // case 2: filter with push down projection
        // ie: proj only has values which are already in filter expression
        MedJdbcPushDownRule r2 =
            new MedJdbcPushDownRule(
                new RelOptRuleOperand(
                    FilterRel.class,
                    new RelOptRuleOperand[] {
                        new RelOptRuleOperand(
                            ProjectRel.class,
                            new RelOptRuleOperand[] {
                                new RelOptRuleOperand(
                                    MedJdbcQueryRel.class,
                                    null)
                            })
                    }),
                "filter on proj");

        // case 3: filter with no projection to push down.
        // ie: select *
        MedJdbcPushDownRule r3 =
            new MedJdbcPushDownRule(
                new RelOptRuleOperand(
                    FilterRel.class,
                    new RelOptRuleOperand[] {
                        new RelOptRuleOperand(MedJdbcQueryRel.class, null)
                    }),
                "filter");

        // all pushdown rules
        ArrayList<MedJdbcPushDownRule> pushdownRuleList =
            new ArrayList<MedJdbcPushDownRule>();
        pushdownRuleList.add(r1);
        pushdownRuleList.add(r2);
        pushdownRuleList.add(r3);

        // add the non-disabled pushdown rules
        for (MedJdbcPushDownRule rule : pushdownRuleList) {
            boolean ruledOut = false;
            for (RelOptRuleOperand op : rule.getOperands()) {
                if (disabledPushdownPattern.matcher(
                        op.getMatchedClass().getSimpleName()).matches())
                {
                    ruledOut = true;
                    break;
                }
            }
            if (!ruledOut) {
                planner.addRule(rule);
            }
        }
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

    // implement FarragoMedDataServer
    public void releaseResources()
    {
        validateConnection = true;
    }

    private void createSchemaMaps(String mapping)
        throws SQLException
    {
        String [] allMapping = mapping.split(";");

        for (String s : allMapping) {
            String [] map = s.split(":");

            // not a valid mapping
            if (map.length != 2) {
                continue;
            }
            String key = map[0].trim();
            String value = map[1].trim();

            if (!key.equals("") && !value.equals("")) {
                HashMap h = new HashMap();
                if (schemaMaps.get(value) != null) {
                    h = (HashMap) schemaMaps.get(value);
                }
                ResultSet resultSet = null;
                try {
                    resultSet =
                        databaseMetaData.getTables(
                            catalogName,
                            key,
                            null,
                            tableTypes);
                    if (resultSet == null) {
                        continue;
                    }
                    while (resultSet.next()) {
                        h.put(resultSet.getString(3), key);
                    }
                    schemaMaps.put(value, h);
                } catch (Throwable ex) {
                    // assume unsupported
                    continue;
                } finally {
                    if (resultSet != null) {
                        resultSet.close();
                    }
                }
            }
        }
    }
}

// End MedJdbcDataServer.java
