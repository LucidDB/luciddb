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
import java.util.logging.*;
import java.util.regex.*;

import javax.naming.*;
import javax.sql.*;

import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.query.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.apache.commons.dbcp.*;
import org.apache.commons.pool.*;
import org.apache.commons.pool.impl.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.rel.jdbc.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;

// TODO:  throw exception on unknown option?

/**
 * MedJdbcDataServer implements the {@link FarragoMedDataServer} interface for
 * JDBC data.
 * 
 * <p>MedJdbcDataServer provides three modes of operation:
 * <ol>
 * <li>
 * When a JNDI resource name is provided, it obtains a {@link DataSource} 
 * object from JNDI and obtains database connections from it.  The DataSource
 * is assumed to represent a connection pool.  Validation queries and login
 * timeouts are the responsibility of the data source.  When
 * {@link #getConnection()} (or {@link #getDatabaseMetaData()}) are invoked, 
 * a single {@link Connection} is borrowed from the pool and held until
 * {@link #releaseResources()} or {@link #closeAllocation()} is invoked. A
 * separate Connection is borrowed from the pool for each call to 
 * {@link #getRuntimeSupport(Object)} and is returned when the associated
 * {@link FarragoStatementAllocation} object is closed.
 * </li>
 * <li>
 * When JDBC connection information is given (e.g., driver, URL, username,
 * password), MedJdbcDataServer uses Apache Commons-DBCP to create a DataSource
 * backed by a connection pool.  If a validation query is specified,the 
 * DataSource is configured to execute it.  When {@link #getConnection()} 
 * (or {@link #getDatabaseMetaData()}) are invoked, a single {@link Connection}
 * is borrowed from the pool and held until {@link #releaseResources()} or 
 * {@link #closeAllocation()} is invoked. A separate Connection is borrowed 
 * from the pool for each call to {@link #getRuntimeSupport(Object)} and is 
 * returned when the associated {@link FarragoStatementAllocation} object is 
 * closed.
 * </li>
 * <li>
 * When JDBC connection information is given and connection pooling is 
 * {@link #PROP_DISABLE_CONNECTION_POOL disabled}, MedJdbcDataServer behaves 
 * as it did before the introduction of connection pooling.  A single 
 * {@link Connection} is obtained using the {@link DriverManager}. Validation
 * queries are executed when the Connection is next used after a call to 
 * {@link #releaseResources()}.  The same Connection is also used for 
 * {@link #getRuntimeSupport(Object)}.  The Connection is held until 
 * {@link #closeAllocation()} is invoked.
 * </li>
 * </ol>
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
    public static final String PROP_JNDI_NAME = "JNDI_NAME";
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
    public static final String PROP_TABLE_MAPPING = "TABLE_MAPPING";
    public static final String PROP_TABLE_PREFIX_MAPPING = 
        "TABLE_PREFIX_MAPPING";
    public static final String PROP_MAX_IDLE_CONNECTIONS = 
        "MAX_IDLE_CONNECTIONS";
    public static final String PROP_EVICTION_TIMER_PERIOD_MILLIS =
        "EVICTION_TIMER_PERIOD_MILLIS";
    public static final String PROP_MIN_EVICTION_IDLE_MILLIS =
        "MIN_EVICTION_IDLE_MILLIS";
    public static final String PROP_VALIDATION_TIMING = "VALIDATION_TIMING";
    public static final String PROP_VALIDATION_TIMING_ON_BORROW = 
        "ON_BORROW";
    public static final String PROP_VALIDATION_TIMING_ON_RETURN = 
        "ON_RETURN";
    public static final String PROP_VALIDATION_TIMING_WHILE_IDLE = 
        "WHILE_IDLE";
    public static final String PROP_DISABLE_CONNECTION_POOL = 
        "DISABLE_CONNECTION_POOL";
    
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
    public static final int DEFAULT_MAX_IDLE_CONNECTIONS = 1;
    public static final long DEFAULT_EVICTION_TIMER_PERIOD = -1L;
    public static final long DEFAULT_MIN_EVICTION_IDLE_MILLIS = -1L;
    public static final String DEFAULT_VALIDATION_TIMING = 
        PROP_VALIDATION_TIMING_ON_BORROW;
    public static final boolean DEFAULT_DISABLE_CONNECTION_POOL = false;
    
    private static final Logger logger = 
        FarragoTrace.getClassTracer(MedJdbcDataServer.class);

    
    //~ Instance fields --------------------------------------------------------

    // TODO:  add support for distributed txns

    protected DataSource dataSource;
    
    // Generic connection pool support
    protected Properties connectProps;
    protected String userName;
    protected String password;
    protected String url;
    private GenericObjectPool connectionPool;
    private int maxIdleConnections;
    private long evictionTimerPeriodMillis;
    private long minEvictionIdleMillis;

    // JNDI DataSource name
    protected String jndiName;
    
    // Prepare-time connection and metadata
    private Connection connection;
    protected boolean supportsMetaData;
    private DatabaseMetaData databaseMetaData;
    
    /*
     * When set to true, MedJdbcDataServer behaves as it did prior to the
     * introduction of connection pooling.
     */
    private boolean disableConnectionPool;
    
    /**
     * If {@link #disableConnectionPool} is true, used to determine when to
     * re-validate the connection.
     */
    private boolean validateConnection = false;
    
    protected String catalogName;
    protected String schemaName;
    protected String [] tableTypes;
    protected String loginTimeout;
    protected String validationQuery;
    private boolean validateOnBorrow;
    private boolean validateOnReturn;
    private boolean validateWhileIdle;
    protected boolean useSchemaNameAsForeignQualifier;
    protected boolean lenient;
    protected Pattern disabledPushdownPattern;
    private int fetchSize;
    private boolean autocommit;
    protected HashMap<String, Map<String, String>> schemaMaps;
    protected HashMap<String, Map<String, Source>> tableMaps;
    protected Map<String, List<WildcardMapping>> tablePrefixMaps;
    
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

        jndiName = props.getProperty(PROP_JNDI_NAME);

        if (jndiName == null) {
            requireProperty(props, PROP_URL);
        }
        
        url = props.getProperty(PROP_URL);
        userName = props.getProperty(PROP_USER_NAME);
        password = props.getProperty(PROP_PASSWORD);
        
        disableConnectionPool = 
            getBooleanProperty(
                props, 
                PROP_DISABLE_CONNECTION_POOL,
                DEFAULT_DISABLE_CONNECTION_POOL);
        
        if (jndiName != null) {
            if (url != null) {
                throw FarragoResource.instance().PluginPropsConflict.ex(
                    PROP_JNDI_NAME, PROP_URL);
            }
            if (userName != null) {
                throw FarragoResource.instance().PluginPropsConflict.ex(
                    PROP_JNDI_NAME, PROP_USER_NAME);                
            }
            if (password != null) {
                throw FarragoResource.instance().PluginPropsConflict.ex(
                    PROP_JNDI_NAME, PROP_PASSWORD);                
            }
            if (disableConnectionPool) {
                throw FarragoResource.instance().PluginPropsConflict.ex(
                    PROP_JNDI_NAME, PROP_DISABLE_CONNECTION_POOL);                
            }
        }
        
        schemaName = props.getProperty(PROP_SCHEMA_NAME);
        catalogName = props.getProperty(PROP_CATALOG_NAME);
        if (jndiName == null) {
            loginTimeout = props.getProperty(PROP_LOGIN_TIMEOUT);
            validationQuery = props.getProperty(PROP_VALIDATION_QUERY);
            
            if (!disableConnectionPool) {
                String validationTimingProp = 
                    props.getProperty(
                        PROP_VALIDATION_TIMING, DEFAULT_VALIDATION_TIMING);
                for(String validationTiming: validationTimingProp.split(",")) {
                    validationTiming = validationTiming.trim().toUpperCase();
                    if (validationTiming.equals(
                            PROP_VALIDATION_TIMING_ON_BORROW))
                    {
                        validateOnBorrow = true;
                    } else if (validationTiming.equals(
                        PROP_VALIDATION_TIMING_ON_RETURN))
                    {
                        validateOnReturn = true;
                    } else if (validationTiming.equals(
                        PROP_VALIDATION_TIMING_WHILE_IDLE)) 
                    {
                        validateWhileIdle = true;
                    } else {
                        throw 
                            FarragoResource.instance().PluginInvalidStringProp.ex(
                                validationTiming, PROP_VALIDATION_TIMING);
                    }
                }
            }
        }
        
        schemaMaps = new HashMap<String, Map<String, String>>();
        tableMaps = new HashMap<String, Map<String, Source>>();
        tablePrefixMaps = new HashMap<String, List<WildcardMapping>>();

        if (getBooleanProperty(props, PROP_EXT_OPTIONS, false)) {
            if (jndiName != null) {
                throw FarragoResource.instance().PluginPropsConflict.ex(
                    PROP_JNDI_NAME, PROP_EXT_OPTIONS);                                
            }
            
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

        // Ignore login timeout if JNDI lookup will be used.
        if (loginTimeout != null && jndiName == null) {
            try {
                // REVIEW: SWZ: 2008-09-03: This is a global setting. If
                // multiple MedJdbcDataServers are configured with different
                // values they'll step on each other.  Not to mention other
                // plugins which may make their own calls! (See FRG-343)
                DriverManager.setLoginTimeout(Integer.parseInt(loginTimeout));
            } catch (NumberFormatException ne) {
                // ignore the timeout
            }
        }

        fetchSize = getIntProperty(props, PROP_FETCH_SIZE, DEFAULT_FETCH_SIZE);
        autocommit =
            getBooleanProperty(props, PROP_AUTOCOMMIT, DEFAULT_AUTOCOMMIT);

        if (!disableConnectionPool) {
            maxIdleConnections = 
                getIntProperty(
                    props,
                    PROP_MAX_IDLE_CONNECTIONS, 
                    DEFAULT_MAX_IDLE_CONNECTIONS);
            evictionTimerPeriodMillis =
                getLongProperty(
                    props,
                    PROP_EVICTION_TIMER_PERIOD_MILLIS,
                    DEFAULT_EVICTION_TIMER_PERIOD);
            minEvictionIdleMillis =
                getLongProperty(
                    props, 
                    PROP_MIN_EVICTION_IDLE_MILLIS,
                    DEFAULT_MIN_EVICTION_IDLE_MILLIS);
    
            initializeDataSource();
        }
        
        DatabaseMetaData databaseMetaData = getDatabaseMetaData();
        
        String schemaMapping = props.getProperty(PROP_SCHEMA_MAPPING);
        String tableMapping = props.getProperty(PROP_TABLE_MAPPING);
        String tablePrefix = props.getProperty(PROP_TABLE_PREFIX_MAPPING);
        String tablePrefixMapping = 
            props.getProperty(PROP_TABLE_PREFIX_MAPPING);

        try {
            if ((schemaMapping != null && tableMapping != null) ||
                (schemaMapping != null && tablePrefixMapping != null) ||
                (tableMapping != null && tablePrefixMapping != null))
            {
                throw FarragoResource.instance().MedJdbc_InvalidTableSchemaMapping
                    .ex();
            }

            if (schemaMapping != null) {
                parseMapping(databaseMetaData, schemaMapping, false, false);
            } else if (tableMapping != null) {
                parseMapping(databaseMetaData, tableMapping, true, false);
            } else if (tablePrefix != null) {
                parseMapping(databaseMetaData, tablePrefixMapping, true, true);
            }
        } catch(SQLException e) {
            logger.log(Level.SEVERE, "Error initializing MedJdbc mappings", e);
            closeAllocation();
            throw e;
        } catch(RuntimeException e) {
            logger.log(Level.SEVERE, "Error initializing MedJdbc mappings", e);
            closeAllocation();
            throw e;
        }
        
    }

    private void initMetaData()
    {
        try {
            databaseMetaData = connection.getMetaData();
            supportsMetaData = true;
        } catch (Exception ex) {
            Util.swallow(ex, logger);
        }
        
        if (databaseMetaData == null) {
            // driver can't even support getMetaData(); treat it
            // as brain-damaged
            databaseMetaData =
                (DatabaseMetaData) Proxy.newProxyInstance(
                    null,
                    new Class[] { DatabaseMetaData.class },
                    new SqlUtil.DatabaseMetaDataInvocationHandler(
                        "UNKNOWN",
                        ""));
            supportsMetaData = false;
        }
    }
    
    private void initializeDataSource() throws SQLException
    {
        assert(!disableConnectionPool);
        
        if (jndiName != null) {
            try {
                InitialContext initCtx = new InitialContext();
                dataSource = (DataSource)initCtx.lookup(jndiName);
                return;
            } catch(NamingException e) {
                throw FarragoResource.instance().MedJdbc_InvalidDataSource.ex(
                    jndiName);
            }
        }
        
        String userName = getUserName();
        String password = getPassword();
        
        ConnectionFactory connectionFactory;
        if (connectProps != null) {
            if (userName != null) {
                connectProps.setProperty("user", userName);
            }
            if (password != null) {
                connectProps.setProperty("password", password);
            }

            connectionFactory = 
                new DriverManagerConnectionFactory(url, connectProps);
        } else if (userName == null) {
            connectionFactory = 
                new DriverManagerConnectionFactory(url, new Properties());
        } else {
            if (password == null) {
                password = "";
            }
            
            connectionFactory = 
                new DriverManagerConnectionFactory(
                    url, userName, password);
        }

        if (validateWhileIdle && evictionTimerPeriodMillis <= 0L) {
            logger.warning(
                "Request to validate on idle ignored: property " +
                PROP_EVICTION_TIMER_PERIOD_MILLIS +
                " must be > 0");
            
            if (validationQuery != null &&
                !validateOnBorrow &&
                !validateOnReturn)
            {
                validateOnBorrow = true;
                
                logger.warning("Enabling validation on request");
            }
        }

        connectionPool = new GenericObjectPool();
        connectionPool.setWhenExhaustedAction(
            GenericObjectPool.WHEN_EXHAUSTED_GROW);
        connectionPool.setMaxActive(-1);
        
        connectionPool.setTestOnBorrow(validateOnBorrow);
        connectionPool.setTestOnReturn(validateOnReturn);
        connectionPool.setTestWhileIdle(validateWhileIdle);
        
        connectionPool.setMaxIdle(maxIdleConnections);
        connectionPool.setTimeBetweenEvictionRunsMillis(
            evictionTimerPeriodMillis);
        connectionPool.setMinEvictableIdleTimeMillis(minEvictionIdleMillis);
        
        CustomPoolableConnectionFactory poolableConnectionFactory =
            new CustomPoolableConnectionFactory(
                connectionFactory,
                connectionPool,
                validationQuery,
                autocommit,
                null);
        
        connectionPool.setFactory(poolableConnectionFactory);
        PoolingDataSource pds = new PoolingDataSource(connectionPool);
        pds.setAccessToUnderlyingConnectionAllowed(true);
        dataSource = pds;
        
    }
    
    /**
     * Retrieves the configured user name for this data server.  Subclasses may
     * override this method to obtain the user name from an alternate source.
     * 
     * @return user name for this data server
     */
    protected String getUserName()
    {
        return userName;
    }
    
    /**
     * Retrieves the configured password for this data server.  Subclasses may
     * override this method to obtain the password from an alternate source.
     * 
     * @return password for this data server
     */
    protected String getPassword()
    {
        return password;
    }
    
    /**
     * Retrieves a Connection to this data server's configured database.
     * The Connection returned by the first call to this method will continue
     * to be returned until {@link #releaseResources()} is invoked.
     * 
     * <p>This Connection is <b>not</b> to be used for runtime query support,
     * although DDL (such as IMPORT FOREIGN SCHEMA) may use it.
     * 
     * <p><b>NOTE:</b> if connection pooling is 
     * {@link #PROP_DISABLE_CONNECTION_POOL disabled}, the Connection returned
     * by this method will be re-used for runtime support and will be
     * returned even after a call to {@link #releaseResources()}.
     * 
     * @return Connection to the database
     * @throws SQLException if there's an error obtaining a connection
     */
    protected Connection getConnection()
        throws SQLException
    {
        if (connection == null || connection.isClosed()) {
            connection = newConnection();
            initMetaData();
        } else if (disableConnectionPool && 
                   validateConnection && 
                   validationQuery != null)
        {
            boolean validated = false;
            Statement testStatement = connection.createStatement();
            try {
                testStatement.executeQuery(validationQuery);
                validated = true;
            } catch (Exception ex) {
                // need to re-create connection
                closeAllocation();
            } finally {
                if (testStatement != null) {
                    try {
                        testStatement.close();
                    } catch (SQLException ex) {
                        // do nothing
                    }
                }
            }
            
            if (!validated) {
                // Validation failed.
                connection = newConnection();
                initMetaData();
            }
            
            validateConnection = false;
        }
        
        return connection;
    }
    
    /**
     * Retrieves a Connection object from the DataSource and set auto-commit
     * mode if necessary.
     * 
     * @return a connection from the datasource
     */
    private Connection newConnection() throws SQLException
    {
        if (disableConnectionPool) {
            // Subclasses may obtain or modify the username and password stored
            // in the properties. Give them their chance here.
            String userName = getUserName();
            String password = getPassword();

            Connection conn;
            if (connectProps != null) {
                if (userName != null) {
                    connectProps.setProperty("user", userName);
                }
                if (password != null) {
                    connectProps.setProperty("password", password);
                }
                conn = DriverManager.getConnection(url, connectProps);
            } else if (userName == null) {
                conn = DriverManager.getConnection(url);
            } else {
                conn = DriverManager.getConnection(url, userName, password);
            }

            markLoopbackConnection(conn);
            
            if (!autocommit) {
                conn.setAutoCommit(false);
            }

            return conn;
        } else {
            Connection conn = dataSource.getConnection();
    
            // Skip fiddling with auto-commit if we've made our own connection
            // pool: it's already calling setAutoCommit for us.
            if (connectionPool == null && !autocommit) {
                conn.setAutoCommit(false);
            }
    
            return conn;
        }
    }

    private void markLoopbackConnection(Connection conn)
    {
        if (conn instanceof FarragoJdbcEngineConnection) {
            FarragoSession session =
                ((FarragoJdbcEngineConnection)conn).getSession();
            session.setLoopback();
        }
    }

    
    /**
     * Retrieves database metadata for this data server's configured database.
     * This method automatically invoked {@link #getConnection()} to obtain
     * a Connection to the database.  The same {@link DatabaseMetaData} object
     * will be returned for each call to this method until 
     * {@link #releaseResources()} is invoked.
     * 
     * <p>This {@link DatabaseMetaData} object is <b>not</b> to be used for 
     * runtime query support, although DDL (such as IMPORT FOREIGN SCHEMA) may
     * use it.
     * 
     * <p><b>NOTE:</b> if connection pooling is 
     * {@link #PROP_DISABLE_CONNECTION_POOL disabled}, the 
     * {@link DatabaseMetaData} object returned by this method will be re-used 
     * even after a call to {@link #releaseResources()}.
     * 
     * @return database metadata
     * @throws SQLException if there's an error obtaining a connection or 
     *                      metadata
     */
    protected DatabaseMetaData getDatabaseMetaData() throws SQLException
    {
        if (connection == null) {
            getConnection();

            assert(connection != null);
        }
        
        assert(databaseMetaData != null);

        return databaseMetaData;
    }
    
    // implement FarragoMedDataServer
    public void releaseResources()
    {
        if (disableConnectionPool) {
            validateConnection = true;
        } else {
            // TODO: release connection pool's conn?  double check that
            // auto commit is only being set once for prep and once for exec
            closeConnection();
        }
    }

    private void closeConnection()
    {
        if (connection != null) {
            Connection conn = connection;
            databaseMetaData = null;
            connection = null;
            try {
                conn.close();
            } catch(SQLException e) {
                logger.log(
                    Level.SEVERE, "Error closing resource connection", e);
            }
        }
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
        props.remove(PROP_TABLE_MAPPING);
        props.remove(PROP_TABLE_PREFIX_MAPPING);
        props.remove(PROP_JNDI_NAME);
        props.remove(PROP_MAX_IDLE_CONNECTIONS);
        props.remove(PROP_EVICTION_TIMER_PERIOD_MILLIS);
        props.remove(PROP_MIN_EVICTION_IDLE_MILLIS);
        props.remove(PROP_VALIDATION_TIMING);
        props.remove(PROP_DISABLE_CONNECTION_POOL);
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
        Map<String, Properties> columnPropMap)
        throws SQLException
    {
        assert (dataSource != null);
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
            rowType,
            true);
    }

    // implement FarragoMedDataServer
    public Object getRuntimeSupport(Object param)
        throws SQLException
    {
        String sql = (String) param;
        
        FarragoStatementAllocation stmtAlloc;
        Statement stmt;
        if (disableConnectionPool) {
            Connection conn = getConnection();
            stmt = conn.createStatement();
            
            // Leave connection open (closed by release resources)
            stmtAlloc = new FarragoStatementAllocation(stmt);            
        } else {
            // N.B.: do not invoke getConnection(): We want to obtain multiple
            // connections if there are multiple XOs requiring runtime support.
            // MySQL (with streaming results) and loopback connections require 
            // this behavior.
            Connection conn = newConnection();
            stmt = conn.createStatement();
            
            // Closes connection when no longer needed, which returns it to the 
            // pool.
            stmtAlloc = new FarragoStatementAllocation(conn, stmt);            
        }
        
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
                    new RelOptRuleOperand(
                        FilterRel.class,
                        new RelOptRuleOperand(
                            ProjectRel.class,
                            new RelOptRuleOperand(
                                MedJdbcQueryRel.class,
                                RelOptRule.ANY)))),
                "proj on filter on proj");

        // case 2: filter with push down projection
        // ie: proj only has values which are already in filter expression
        MedJdbcPushDownRule r2 =
            new MedJdbcPushDownRule(
                new RelOptRuleOperand(
                    FilterRel.class,
                    new RelOptRuleOperand(
                        ProjectRel.class,
                        new RelOptRuleOperand(
                            MedJdbcQueryRel.class,
                            RelOptRule.ANY))),
                "filter on proj");

        // case 3: filter with no projection to push down.
        // ie: select *
        MedJdbcPushDownRule r3 =
            new MedJdbcPushDownRule(
                new RelOptRuleOperand(
                    FilterRel.class,
                    new RelOptRuleOperand(
                        MedJdbcQueryRel.class,
                        RelOptRule.ANY)),
                "filter");

        // case 4: only projection, no filter
        MedJdbcPushDownRule r4 =
            new MedJdbcPushDownRule(
                new RelOptRuleOperand(
                    ProjectRel.class,
                    new RelOptRuleOperand(
                        MedJdbcQueryRel.class,
                        RelOptRule.ANY)),
                "proj");

        // all pushdown rules
        List<MedJdbcPushDownRule> pushdownRuleList =
            new ArrayList<MedJdbcPushDownRule>();
        pushdownRuleList.add(r1);
        pushdownRuleList.add(r2);
        pushdownRuleList.add(r3);
        pushdownRuleList.add(r4);

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
        closeConnection();
        
        if (connectionPool != null) {
            try {
                dataSource = null;
                GenericObjectPool pool = connectionPool;
                connectionPool = null;
                pool.close();
            } catch(Exception e) {
                logger.log(Level.SEVERE, "Error closing connection pool", e);
            }
        }
    }

    private void parseMapping(
        DatabaseMetaData databaseMetaData,
        String mapping,
        boolean isTableMapping,
        boolean isTablePrefixMapping)
    throws SQLException
    {
        if (!isTableMapping) {
            // Force valid parameters.
            isTablePrefixMapping = false;
        }
        
        String srcSchema = null;
        String srcTable = null;
        String targetSchema = null;
        String targetTable = null;

        StringBuffer buffer = new StringBuffer(64);
        buffer.setLength(0);

        boolean insideQuotes = false;
        boolean atSource = true;
        
        int len = mapping.length();
        int i = 0;

        while (i < len) {
            char c = mapping.charAt(i);
            switch (c) {
            case '"':
                if (!isQuoteChar(mapping, i)) {
                    // escape character, add one quote
                    buffer.append(c);
                    i++;
                } else {
                    if (insideQuotes) {
                        // this is the endQuote
                        insideQuotes = false;
                    } else {
                        // this is the startQuote
                        insideQuotes = true;
                    }
                }
                i++;
                break;
            case '.':
                if (!isTableMapping) {
                    // non special characters
                    buffer.append(c);
                } else {
                    // in table mapping, "." is a special character
                    if (insideQuotes) {
                        buffer.append(c);
                    } else {
                        if (atSource) {
                            srcSchema = buffer.toString();
                            srcSchema = srcSchema.trim();
                        } else {
                            targetSchema = buffer.toString();
                            targetSchema = targetSchema.trim();
                        }
                        buffer.setLength(0);
                    }
                }
                i++;
                break;
            case ':':
                if (insideQuotes) {
                    buffer.append(c);
                } else {
                    srcTable = buffer.toString();
                    srcTable = srcTable.trim();
                    atSource = false;
                    buffer.setLength(0);
                }
                i++;
                break;
            case ';':
                if (insideQuotes) {
                    buffer.append(c);
                } else {
                    targetTable = buffer.toString();
                    targetTable = targetTable.trim();
                    atSource = true;
                    buffer.setLength(0);
                    if (isTableMapping) {
                        if (isTablePrefixMapping) {
                            createTablePrefixMaps(
                                srcSchema,
                                srcTable,
                                targetSchema,
                                targetTable);
                        } else {
                            createTableMaps(
                                srcSchema,
                                srcTable,
                                targetSchema,
                                targetTable);
                        }
                    } else {
                        createSchemaMaps(
                            databaseMetaData,
                            srcTable,
                            targetTable);
                    }
                }
                i++;
                break;
            default:
                // non special characters
                buffer.append(c);
                i++;
                break;
            }
            if (i == len) {
                targetTable = buffer.toString();
                targetTable = targetTable.trim();
                buffer.setLength(0);
                if (isTableMapping) {
                    if (isTablePrefixMapping) {
                        createTablePrefixMaps(
                            srcSchema,
                            srcTable,
                            targetSchema,
                            targetTable);
                    } else {
                        createTableMaps(
                            srcSchema,
                            srcTable,
                            targetSchema,
                            targetTable);
                    }
                } else {
                    createSchemaMaps(
                        databaseMetaData,
                        srcTable,
                        targetTable);
                }
            }
        }
    }

    private void createSchemaMaps(
        DatabaseMetaData databaseMetaData, String key, String value)
    throws SQLException
    {
        if (key == null || value == null) {
            return;
        }

        if (!key.equals("") && !value.equals("")) {
            Map<String, String> h = new HashMap<String, String>();
            if (schemaMaps.get(value) != null) {
                h = schemaMaps.get(value);
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
                    return;
                }
                while (resultSet.next()) {
                    h.put(resultSet.getString(3), key);
                }
                schemaMaps.put(value, h);
            } catch (Throwable ex) {
                // assume unsupported
                return;
            } finally {
                if (resultSet != null) {
                    resultSet.close();
                }
            }
        }
    }

    private void createTableMaps(
        String srcSchema,
        String srcTable,
        String targetSchema,
        String targetTable)
    throws SQLException
    {
        if (srcSchema == null ||
            srcTable == null ||
            targetSchema == null ||
            targetTable == null) {
            return;
        }
        
        Map<String, Source> h = tableMaps.get(targetSchema);
        if (h == null) {
            h = new HashMap<String, Source>();
        }

        // validate that the same table name is not mapped to the same schema
        // name
        Source src = h.get(targetTable);
        if (src != null) {
            // forgive the instance where the same source_schema and
            // source_table are mapped again
            if (!src.getSchema().equals(srcSchema) ||
                !src.getTable().equals(srcTable)) {
                throw FarragoResource.instance().MedJdbc_InvalidTableMapping
                    .ex(
                        src.getSchema(), src.getTable(), srcSchema, srcTable,
                        targetSchema, targetTable);
            }
        }
        h.put(targetTable, new Source(srcSchema, srcTable));
        tableMaps.put(targetSchema, h);
    }

    private void createTablePrefixMaps(
        String srcSchema,
        String srcTablePrefix,
        String targetSchema,
        String targetTablePrefix)
    throws SQLException
    {
        if (srcSchema == null ||
            srcTablePrefix == null ||
            targetSchema == null ||
            targetTablePrefix == null)
        {
            return;
        }
        
        if (srcTablePrefix.endsWith("%")) {
            srcTablePrefix = 
                srcTablePrefix.substring(0, srcTablePrefix.length() - 1);
        }
        
        if (targetTablePrefix.endsWith("%")) {
            targetTablePrefix = 
                targetTablePrefix.substring(0, targetTablePrefix.length() - 1);
        }
        
        List<WildcardMapping> list = tablePrefixMaps.get(targetSchema);
        if (list == null) {
            list = new ArrayList<WildcardMapping>();
            tablePrefixMaps.put(targetSchema, list);
        }
        
        WildcardMapping mapping = 
            new WildcardMapping(
                targetTablePrefix,
                srcSchema,
                srcTablePrefix);
        
        for(WildcardMapping m: list) {
            if (m.targetTablePrefix.equals(targetTablePrefix)) {
                // forgive the instance where the same source_schema and
                // souoce_table are mapped again
                if (!m.getSourceSchema().equals(srcSchema) ||
                    !m.getSourceTablePrefix().equals(srcTablePrefix)) {
                    throw FarragoResource.instance().MedJdbc_InvalidTablePrefixMapping
                        .ex(
                            m.getSourceSchema(), 
                            m.getSourceTablePrefix(), 
                            srcSchema, 
                            srcTablePrefix,
                            targetSchema, 
                            targetTablePrefix);
                }
            }
        }
        
        list.add(mapping);
    }
    
    private boolean isQuoteChar(String mapping, int index)
    {
        boolean isQuote = false;

        for (int i = index; i < mapping.length(); i++) {
            if (mapping.charAt(i) == '"') {
                isQuote = !isQuote;
            } else {
                break;
            }
        }
        return isQuote;
    }

    public static class Source
    {
        final String schema;
        final String table;

        Source(String sch, String tab)
        {
            this.schema = sch;
            this.table = tab;
        }

        public String getSchema()
        {
            return this.schema;
        }

        public String getTable()
        {
            return this.table;
        }
    }

    public static class WildcardMapping
    {
        final String targetTablePrefix;
        final String sourceSchema;
        final String sourceTablePrefix;
        
        WildcardMapping(
            String targetTablePrefix, 
            String sourceSchema, 
            String sourceTablePrefix)
        {
            this.targetTablePrefix = targetTablePrefix;
            this.sourceSchema = sourceSchema;
            this.sourceTablePrefix = sourceTablePrefix;
        }
        
        public String getTargetTablePrefix()
        {
            return targetTablePrefix;
        }
        
        public String getSourceSchema()
        {
            return sourceSchema;
        }
        
        public String getSourceTablePrefix()
        {
            return sourceTablePrefix;
        }
        
        public boolean equals(Object o)
        {
            WildcardMapping that = (WildcardMapping)o;
            
            return 
                this.targetTablePrefix.equals(that.targetTablePrefix) &&
                this.sourceSchema.equals(that.sourceSchema) &&
                this.sourceTablePrefix.equals(that.sourceTablePrefix);
        }
    }
    
    public static class WildcardTarget
    {
        final String tablePrefix;
        
        WildcardTarget(String tablePrefix)
        {
            this.tablePrefix = tablePrefix;
        }
        
        public String getTablePrefix()
        {
            return tablePrefix;
        }
        public int hashCode()
        {
            return tablePrefix.hashCode();
        }
        
        public boolean equals(Object o)
        {
            WildcardTarget that = (WildcardTarget)o;
            
            return this.tablePrefix.equals(that.tablePrefix);
        }
    }
    
    /**
     * CustomPoolableConnectionFactory is similar to DBCP's
     * {@link PoolableConnectionFactory}, but allows us to better control
     * when {@link Connection#setAutoCommit(boolean)} and 
     * {@link Connection#setReadOnly(boolean)} are called. DBCP's 
     * implementation always calls at least <code>setAutoCommit</code>.
     * 
     * <p>Examples: HSQLDB's <code>setReadOnly(false)</code> throws if 
     * read-only mode is enabled in the URL.  CsvJdbc's 
     * <code>setAutoCommit(boolean)</code> always throws.
     */
    private class CustomPoolableConnectionFactory
        implements PoolableObjectFactory
    {
        private ConnectionFactory connectionFactory;
        private ObjectPool objectPool;
        private String validationQuery;
        private boolean autoCommit;
        private Boolean readOnly;
        
        public CustomPoolableConnectionFactory(
            ConnectionFactory connectionFactory,
            ObjectPool objectPool,
            String validationQuery,
            boolean autoCommit,
            Boolean readOnly)
        {
            this.connectionFactory = connectionFactory;
            this.objectPool = objectPool;
            this.validationQuery = validationQuery;
            this.autoCommit = autoCommit;
            this.readOnly = readOnly;
        }
        
        public Object makeObject() throws Exception 
        {
            Connection connection = connectionFactory.createConnection();
            
            markLoopbackConnection(connection);
            
            return new CustomPoolableConnection(connection, objectPool);
        }

        public void destroyObject(Object obj) throws Exception
        {
            if (obj instanceof PoolableConnection) {
                ((PoolableConnection)obj).reallyClose();
            }
        }

        public boolean validateObject(Object obj)
        {
            CustomPoolableConnection connection = 
                (CustomPoolableConnection)obj;
            try {
                return validateConnection(connection);
            } catch(Exception e) {
                return false;
            }           
        }

        private boolean validateConnection(Connection conn) 
            throws SQLException
        {
            if (conn.isClosed()) {
                return false;
            }
            
            if (validationQuery != null) {
                Statement stmt = conn.createStatement();
                try {
                    stmt.executeQuery(validationQuery);
                } finally {
                    stmt.close();
                }
            }
            
            return true;
        }
        
        public void activateObject(Object obj) throws Exception
        {
            CustomPoolableConnection connection = 
                (CustomPoolableConnection)obj;
            
            connection.activate();

            if (getAutoCommit(connection) != autoCommit) {
                connection.setAutoCommit(autoCommit);
            }
            
            if (readOnly != null && connection.isReadOnly() != readOnly) {
                connection.setReadOnly(readOnly);
            }
        }
        
        public void passivateObject(Object obj) throws Exception
        {
            CustomPoolableConnection connection = 
                (CustomPoolableConnection)obj;

            // Only rollback if transactions and writes are enabled.
            if (!getAutoCommit(connection) && !connection.isReadOnly()) {
                connection.rollback();
            }
            
            connection.clearWarnings();
            
            connection.passivate();
        }
        
        // Handle drivers that don't support reading autocommit state
        private boolean getAutoCommit(Connection connection)
        {
            try {
                return connection.getAutoCommit();
            } catch(Exception e) {
                return true;
            }
        }        
    }
    
    private static class CustomPoolableConnection
        extends PoolableConnection
    {
        public CustomPoolableConnection(Connection connection, ObjectPool pool)
        {
            super(connection, pool);
        }
        
        protected void activate()
        {
            super.activate();
        }
        
        protected void passivate() throws SQLException
        {
            super.passivate();
        }
    }
}

// End MedJdbcDataServer.java
