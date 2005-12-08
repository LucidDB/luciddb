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

import java.sql.*;
import java.util.*;

import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.util.*;


/**
 * MedJdbcNameDirectory implements the FarragoMedNameDirectory
 * interface by mapping the metadata provided by any JDBC driver.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedJdbcNameDirectory extends MedAbstractNameDirectory
{
    //~ Instance fields -------------------------------------------------------

    final MedJdbcDataServer server;

    final String schemaName;

    //~ Constructors ----------------------------------------------------------

    MedJdbcNameDirectory(MedJdbcDataServer server)
    {
        this(server, null);
    }

    MedJdbcNameDirectory(MedJdbcDataServer server, String schemaName)
    {
        this.server = server;
        this.schemaName = schemaName;
    }

    //~ Methods ---------------------------------------------------------------

    // implement FarragoMedNameDirectory
    public FarragoMedColumnSet lookupColumnSet(
        FarragoTypeFactory typeFactory,
        String foreignName,
        String [] localName)
        throws SQLException
    {
        return lookupColumnSetAndImposeType(
            typeFactory, foreignName,
            localName, null);
    }

    FarragoMedColumnSet lookupColumnSetAndImposeType(
        FarragoTypeFactory typeFactory,
        String foreignName,
        String [] localName,
        RelDataType rowType)
        throws SQLException
    {
        if (schemaName == null) {
            return null;
        }

        String [] foreignQualifiedName;
        if (server.schemaName != null) {
            foreignQualifiedName =
                new String [] { foreignName };
        } else if (server.catalogName != null) {
            foreignQualifiedName =
                new String [] { server.catalogName, schemaName, foreignName };
        } else {
            foreignQualifiedName =
                new String [] { schemaName, foreignName };
        }
        
        SqlDialect dialect = new SqlDialect(server.databaseMetaData);
        SqlStdOperatorTable opTab = SqlStdOperatorTable.instance();
        SqlSelect select =
            opTab.selectOperator.createCall(
                null,
                new SqlNodeList(
                    Collections.singletonList(
                        new SqlIdentifier("*", SqlParserPos.ZERO)),
                    SqlParserPos.ZERO),
                new SqlIdentifier(foreignQualifiedName, SqlParserPos.ZERO),
                null,
                null,
                null,
                null,
                null,
                SqlParserPos.ZERO);

        if (rowType == null) {
            String sql = select.toSqlString(dialect);
            sql = normalizeQueryString(sql);

            PreparedStatement ps = null;
            try {
                ps = server.connection.prepareStatement(sql);
            } catch (Exception ex) {
                // Some drivers don't support prepareStatement
            }
            Statement stmt = null;
            ResultSet rs = null;
            try {
                ResultSetMetaData md = null;
                try {
                    if (ps != null) {
                        md = ps.getMetaData();
                    }
                } catch (SQLException ex) {
                    // Some drivers can't return metadata before execution.
                    // Fall through to recovery below.
                }
                if (md == null) {
                    if (ps != null) {
                        rs = ps.executeQuery();
                    } else {
                        stmt = server.connection.createStatement();
                        rs = stmt.executeQuery(sql);
                    }
                    md = rs.getMetaData();
                }
                rowType = typeFactory.createResultSetType(md);
            } finally {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
                if (ps != null) {
                    ps.close();
                }
            }
        } else {
            // REVIEW:  should we at least check to see if the inferred
            // row type is compatible with the imposed row type?
        }

        return new MedJdbcColumnSet(
            this, foreignQualifiedName, localName, select,
            dialect, rowType);
    }

    String normalizeQueryString(String sql)
    {
        // some drivers don't like multi-line SQL, so convert all
        // whitespace into plain spaces
        return sql.replaceAll("\\s", " ");
    }

    // implement FarragoMedNameDirectory
    public FarragoMedNameDirectory lookupSubdirectory(String foreignName)
        throws SQLException
    {
        if (schemaName == null) {
            return new MedJdbcNameDirectory(server, foreignName);
        } else {
            return null;
        }
    }
    
    // implement FarragoMedNameDirectory
    public boolean queryMetadata(
        FarragoMedMetadataQuery query,
        FarragoMedMetadataSink sink)
        throws SQLException
    {
        if (schemaName == null) {
            boolean wantSchemas = query.getResultObjectTypes().contains(
                FarragoMedMetadataQuery.OTN_SCHEMA);
            if (wantSchemas) {
                return querySchemas(query, sink);
            }
            return true;
        } else {
            boolean wantTables = query.getResultObjectTypes().contains(
                FarragoMedMetadataQuery.OTN_TABLE);
            boolean wantColumns = query.getResultObjectTypes().contains(
                FarragoMedMetadataQuery.OTN_COLUMN);
            List tableListActual = new ArrayList();
            List tableListOptimized = new ArrayList();
            if (wantTables) {
                if (!queryTables(
                        query, sink, tableListActual, tableListOptimized))
                {
                    return false;
                }
            }
            if (wantColumns) {
                if (!queryColumns(
                        query, sink, tableListActual, tableListOptimized))
                {
                    return false;
                }
            }
            return true;
        }
    }

    private boolean querySchemas(
        FarragoMedMetadataQuery query,
        FarragoMedMetadataSink sink)
        throws SQLException
    {
        assert(schemaName == null);
        
        ResultSet resultSet;
        try {
            resultSet = server.databaseMetaData.getSchemas();
            if (resultSet == null) {
                return false;
            }
        } catch (Throwable ex) {
            // assume unsupported
            return false;
        }

        try {
            while (resultSet.next()) {
                String schemaName = resultSet.getString(1);
                String catalogName = resultSet.getString(2);
                if (server.catalogName != null) {
                    if (!server.catalogName.equals(catalogName)) {
                        continue;
                    }
                }
                sink.writeObjectDescriptor(
                    schemaName,
                    FarragoMedMetadataQuery.OTN_SCHEMA,
                    null,
                    Collections.EMPTY_MAP);
            }
        } finally {
            resultSet.close();
        }

        return true;
    }
    
    private boolean queryTables(
        FarragoMedMetadataQuery query,
        FarragoMedMetadataSink sink,
        List tableListActual,
        List tableListOptimized)
        throws SQLException
    {
        assert(schemaName != null);
        
        // In order to optimize column retrieval, we keep track of the
        // number of tables returned by our metadata query and the
        // ones actually accepted by the sink.  It would be better
        // to let the optimizer handle this, but KISS for now.
        int nTablesReturned = 0;
        
        String schemaPattern = getSchemaPattern();
        String tablePattern = getFilterPattern(
            query, FarragoMedMetadataQuery.OTN_TABLE);
            
        ResultSet resultSet;
        try {
            resultSet = server.databaseMetaData.getTables(
                server.catalogName,
                schemaPattern,
                tablePattern,
                server.tableTypes);
            if (resultSet == null) {
                return false;
            }
        } catch (Throwable ex) {
            // assume unsupported
            return false;
        }

        Properties props = new Properties();
        try {
            while (resultSet.next()) {
                ++nTablesReturned;
                String schemaName = resultSet.getString(2);
                if (!matchSchema(schemaPattern, schemaName)) {
                    continue;
                }
                String tableName = resultSet.getString(3);
                String remarks = resultSet.getString(5);
                if (schemaName != null) {
                    props.put(MedJdbcDataServer.PROP_SCHEMA_NAME, schemaName);
                }
                props.put(MedJdbcDataServer.PROP_TABLE_NAME, tableName);
                boolean include = sink.writeObjectDescriptor(
                    tableName,
                    FarragoMedMetadataQuery.OTN_TABLE,
                    remarks,
                    props);
                if (include) {
                    tableListActual.add(tableName);
                }
            }
        } finally {
            resultSet.close();
        }

        // decide on column retrieval plan
        double dMatching = (double) tableListActual.size();
        // +1:  avoid divided by zero
        double dReturned = (double) nTablesReturned + 1;
        if (dMatching / dReturned > 0.3) {
            // a significant portion of the tables returned are matches,
            // so just scan all columns at once and post-filter them,
            // rather than making repeated single-table metadata calls
            tableListOptimized.add("*");
        } else {
            tableListOptimized.addAll(tableListActual);
        }
        
        return true;
    }
    
    private boolean queryColumns(
        FarragoMedMetadataQuery query,
        FarragoMedMetadataSink sink,
        List tableListActual,
        List tableListOptimized)
        throws SQLException
    {
        if (tableListOptimized.equals(Collections.singletonList("*"))) {
            return queryColumnsImpl(
                query, sink, null, new HashSet(tableListActual));
        } else {
            Iterator iter = tableListOptimized.iterator();
            while (iter.hasNext()) {
                String tableName = (String) iter.next();
                if (!queryColumnsImpl(query, sink, tableName, null)) {
                    return false;
                }
            }
            return true;
        }
    }
    
    private boolean queryColumnsImpl(
        FarragoMedMetadataQuery query,
        FarragoMedMetadataSink sink,
        String tableName,
        Set tableSet)
        throws SQLException
    {
        String schemaPattern = getSchemaPattern();
        String tablePattern;
        if (tableName != null) {
            tablePattern = tableName;
        } else {
            tablePattern = getFilterPattern(
                query, 
                FarragoMedMetadataQuery.OTN_TABLE);
        }
        String columnPattern = getFilterPattern(
            query, 
            FarragoMedMetadataQuery.OTN_COLUMN);
            
        ResultSet resultSet;
        try {
            resultSet = server.databaseMetaData.getColumns(
                server.catalogName,
                schemaPattern,
                tablePattern,
                columnPattern);
            if (resultSet == null) {
                return false;
            }
        } catch (Throwable ex) {
            // assume unsupported
            return false;
        }
        
        try {
            while (resultSet.next()) {
                String schemaName = resultSet.getString(2);
                if (!matchSchema(schemaPattern, schemaName)) {
                    continue;
                }
                String returnedTableName = resultSet.getString(3);
                if (tableSet != null) {
                    if (!tableSet.contains(returnedTableName)) {
                        continue;
                    }
                }
                String columnName = resultSet.getString(4);
                RelDataType type;
                try {
                    type =
                        sink.getTypeFactory().createJdbcColumnType(resultSet);
                    // TODO jvs 7-Dec-2005: get rid of this once
                    // we support DECIMAL type; for now fake it as VARCHAR
                    if (type.getSqlTypeName() == SqlTypeName.Decimal) {
                        type = sink.getTypeFactory().createSqlType(
                            SqlTypeName.Double);
                    }
                    if (SqlTypeFamily.Datetime.getTypeNames().contains(
                            type.getSqlTypeName()))
                    {
                        // TODO jvs 7-Dec-2005: proper precision lowering
                        // once we support anything greater than 0
                        // for datetime precision; for now we just
                        // toss the precision.
                        type = sink.getTypeFactory().createSqlType(
                            type.getSqlTypeName());
                    }
                } catch (Throwable ex) {
                    // TODO jvs 7-Dec-2005: post this as a warning once we have
                    // warning support set up.  For now the only way to see it
                    // is to look at the trace log.  The reason we carry on
                    // here is that a lot of tables may contain types we don't
                    // support, and it's a pain for the user to have to exclude
                    // them one by one via trial and error.
                    type =
                        sink.getTypeFactory().createSqlType(
                            SqlTypeName.Varchar,
                            1024);
                }
                String remarks = resultSet.getString(12);
                String defaultValue = resultSet.getString(13);
                int ordinalZeroBased = resultSet.getInt(17) - 1;
                sink.writeColumnDescriptor(
                    returnedTableName,
                    columnName,
                    ordinalZeroBased,
                    type,
                    remarks,
                    defaultValue,
                    Collections.EMPTY_MAP);
            }
        } finally {
            resultSet.close();
        }
        
        return true;
    }

    private String getSchemaPattern()
    {
        if (server.schemaName != null) {
            // schemaName is fake; don't use it
            return null;
        } else {
            return schemaName;
        }
    }

    private String getFilterPattern(
        FarragoMedMetadataQuery query,
        String typeName)
    {
        String pattern = "%";
        FarragoMedMetadataFilter filter = (FarragoMedMetadataFilter)
            query.getFilterMap().get(typeName);
        if (filter != null) {
            if (!filter.isExclusion() && filter.getPattern() != null) {
                pattern = filter.getPattern();
            }
        }
        return pattern;
    }

    private boolean matchSchema(String s1, String s2)
    {
        if ((s1 == null) || (s2 == null)) {
            return true;
        }
        return s1.equals(s2);
    }
}


// End MedJdbcNameDirectory.java
