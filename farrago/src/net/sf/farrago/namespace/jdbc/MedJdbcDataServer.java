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
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.util.*;
import net.sf.farrago.type.*;
import net.sf.farrago.catalog.*;
import net.sf.farrago.resource.*;

import org.eigenbase.sql.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.rel.jdbc.*;

import java.sql.*;
import java.util.*;
import java.lang.reflect.*;

import javax.sql.*;

// TODO:  change most asserts into proper exceptions

// TODO:  throw exception on unknown option?

/**
 * MedJdbcDataServer implements the {@link FarragoMedDataServer} interface
 * for JDBC data.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedJdbcDataServer extends MedAbstractDataServer
{
    public static final String PROP_URL = "URL";
    
    public static final String PROP_USER_NAME = "USER_NAME";
    
    public static final String PROP_PASSWORD = "PASSWORD";
    
    public static final String PROP_SCHEMA_NAME = "SCHEMA_NAME";
    
    public static final String PROP_TABLE_NAME = "TABLE_NAME";
    
    // TODO:  add a parameter for JNDI lookup of a DataSource so we can support
    // app servers and distributed txns

    Connection connection;
    
    String url;

    String schemaName;

    DatabaseMetaData databaseMetaData;
    
    MedJdbcDataServer(
        String serverMofId,
        Properties props)
    {
        super(serverMofId,props);
    }

    void initialize()
        throws SQLException
    {
        Properties props = getProperties();
        url = props.getProperty(PROP_URL);
        assert(url != null);
        String userName = props.getProperty(PROP_USER_NAME);
        String password = props.getProperty(PROP_PASSWORD);
        schemaName = props.getProperty(PROP_SCHEMA_NAME);
        
        if (userName == null) {
            connection = DriverManager.getConnection(url);
        } else {
            connection = DriverManager.getConnection(url,userName,password);
        }
        try {
            databaseMetaData = connection.getMetaData();
        } catch (Exception ex) {
            // driver can't even support getMetaData(); treat it
            // as brain-damaged
            databaseMetaData = (DatabaseMetaData) Proxy.newProxyInstance(
                null,
                new Class [] {DatabaseMetaData.class},
                new StupidDatabaseMetaData());
        }
    }

    public static class StupidDatabaseMetaData
        extends SqlUtil.DatabaseMetaDataInvocationHandler
    {
        public String getDatabaseProductName() throws SQLException
        {
            return "UNKNOWN";
        }

        public String getIdentifierQuoteString() throws SQLException
        {
            return "";
        }
    }

    // implement FarragoMedDataServer
    public FarragoMedNameDirectory getNameDirectory()
        throws SQLException
    {
        return getJdbcNameDirectory();
    }

    private MedJdbcNameDirectory getJdbcNameDirectory()
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

    // implement FarragoMedDataServer
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
                "ResultSetToFarragoIteratorRule")
            {
                public RelNode convert(RelNode rel)
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

// End MedJdbcDataServer.java
