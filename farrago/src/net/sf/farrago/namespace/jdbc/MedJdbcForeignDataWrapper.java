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

package net.sf.farrago.namespace.jdbc;

import net.sf.farrago.namespace.*;
import net.sf.farrago.util.*;
import net.sf.farrago.type.*;
import net.sf.farrago.catalog.*;
import net.sf.farrago.resource.*;

import net.sf.saffron.core.*;
import net.sf.saffron.util.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.rel.convert.*;
import net.sf.saffron.rel.jdbc.*;

import java.sql.*;
import java.util.*;

import javax.sql.*;
import javax.jmi.model.*;
import javax.jmi.reflect.*;

// TODO:  change most asserts into proper exceptions

// TODO:  throw exception on unknown option?

/**
 * MedJdbcForeignDataWrapper implements the FarragoForeignDataWrapper
 * interface by accessing foreign tables provided by any JDBC driver.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class MedJdbcForeignDataWrapper
    implements FarragoForeignDataWrapper
{
    public static final String PROP_DRIVER_CLASS_NAME = "DRIVER_CLASS";
    
    public static final String PROP_URL = "URL";
    
    public static final String PROP_USER_NAME = "USER_NAME";
    
    public static final String PROP_PASSWORD = "PASSWORD";
    
    public static final String PROP_SCHEMA_NAME = "SCHEMA_NAME";
    
    public static final String PROP_TABLE_NAME = "TABLE_NAME";
    
    // TODO:  add a parameter for JNDI lookup of a DataSource so we can support
    // app servers and distributed txns

    Connection connection;
    
    FarragoCatalog catalog;

    String serverMofId;

    String url;

    String schemaName;

    DatabaseMetaData databaseMetaData;
    
    /**
     * Creates a new data wrapper instance.
     */
    public MedJdbcForeignDataWrapper()
    {
    }

    /**
     * Creates a new server instance.
     *
     * @param serverMofId MOFID of server definition in repository
     *
     * @param url URL used to create connection
     *
     * @param connection JDBC connection to server
     *
     * @param schemaName if JDBC driver does not support schemas,
     * schema name to use as fake qualifier for all tables
     */
    MedJdbcForeignDataWrapper(
        String serverMofId,
        String url,
        Connection connection,
        String schemaName) throws SQLException
    {
        this.serverMofId = serverMofId;
        this.connection = connection;
        this.schemaName = schemaName;
        this.url = url;
        databaseMetaData = connection.getMetaData();
    }

    // implement FarragoForeignDataWrapper
    public String getSuggestedName()
    {
        return "JDBC";
    }
    
    // implement FarragoForeignDataWrapper
    public String getDescription(Locale locale)
    {
        // TODO: localize
        return "Foreign data wrapper for JDBC data";
    }
    
    // implement FarragoForeignDataWrapper
    public DriverPropertyInfo [] getWrapperPropertyInfo(Locale locale)
    {
        // TODO
        return new DriverPropertyInfo[0];
    }
    
    // implement FarragoForeignDataWrapper
    public DriverPropertyInfo [] getServerPropertyInfo(Locale locale)
    {
        // TODO
        return new DriverPropertyInfo[0];
    }
    
    // implement FarragoForeignDataWrapper
    public DriverPropertyInfo [] getColumnSetPropertyInfo(Locale locale)
    {
        // TODO
        return new DriverPropertyInfo[0];
    }
    
    // implement FarragoForeignDataWrapper
    public DriverPropertyInfo [] getColumnPropertyInfo(Locale locale)
    {
        // TODO
        return new DriverPropertyInfo[0];
    }
    
    // implement FarragoForeignDataWrapper
    public void initialize(
        FarragoCatalog catalog,
        Properties props) throws SQLException
    {
        this.catalog = catalog;

        String driverClassName = props.getProperty(PROP_DRIVER_CLASS_NAME);
        if (driverClassName == null) {
            // REVIEW:  should we support connecting at the wrapper level,
            // and then allowing different servers to represent different
            // subsets of the data from the same connection?
            assert(props.isEmpty());
        } else {
            assert(props.size() == 1);
            loadDriverClass(driverClassName);
        }
    }

    private void loadDriverClass(String driverClassName)
    {
        try {
            Class.forName(driverClassName);
        } catch (ClassNotFoundException ex) {
            throw FarragoResource.instance().newJdbcDriverLoadFailed(
                driverClassName,ex);
        }
    }
    
    // implement FarragoForeignDataWrapper
    public FarragoForeignDataWrapper newServer(
        String serverMofId,
        Properties props) throws SQLException
    {
        String driverClassName = props.getProperty(PROP_DRIVER_CLASS_NAME);
        if (driverClassName != null) {
            loadDriverClass(driverClassName);
        }
        String url = props.getProperty(PROP_URL);
        assert(url != null);
        String userName = props.getProperty(PROP_USER_NAME);
        String password = props.getProperty(PROP_PASSWORD);
        String schemaName = props.getProperty(PROP_SCHEMA_NAME);
            
        if (userName == null) {
            connection = DriverManager.getConnection(url);
        } else {
            connection = DriverManager.getConnection(url,userName,password);
        }
        
        return new MedJdbcForeignDataWrapper(
            serverMofId,url,connection,schemaName);
    }

    // implement FarragoForeignDataWrapper
    public FarragoNameDirectory getNameDirectory()
        throws SQLException
    {
        return getJdbcNameDirectory();
    }

    private MedJdbcNameDirectory getJdbcNameDirectory()
    {
        return new MedJdbcNameDirectory(this);
    }
    
    // implement FarragoForeignDataWrapper
    public FarragoNamedColumnSet newColumnSet(
        String [] localName,
        Properties tableProps,
        FarragoTypeFactory typeFactory,
        SaffronType rowType,
        Map columnPropMap)
        throws SQLException
    {
        assert(connection != null);
        String tableSchemaName = tableProps.getProperty(PROP_SCHEMA_NAME);
        if (tableSchemaName == null) {
            tableSchemaName = schemaName;
        }
        assert(tableSchemaName != null);
        String tableName = tableProps.getProperty(PROP_TABLE_NAME);
        String [] foreignName = new String [] 
            {
                tableSchemaName,
                tableName
            };
        return getJdbcNameDirectory().lookupColumnSetAndImposeType(
            typeFactory,
            foreignName,
            localName,
            rowType);
    }

    // implement FarragoForeignDataWrapper
    public Object getRuntimeSupport(Object param) throws SQLException
    {
        assert(connection != null);
        
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

    // implement FarragoForeignDataWrapper
    public void registerRules(SaffronPlanner planner)
    {
        JdbcQuery.register(planner);
        
        // tell optimizer how to convert data from JDBC into Farrago
        planner.addRule(
            new ConverterRule(
                SaffronRel.class,
                CallingConvention.RESULT_SET,
                CallingConvention.ITERATOR,
                "ResultSetToFarragoIteratorRule")
            {
                public SaffronRel convert(SaffronRel rel)
                {
                    return new ResultSetToFarragoIteratorConverter(
                        rel.getCluster(),
                        rel);
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

// End MedJdbcForeignDataWrapper.java
