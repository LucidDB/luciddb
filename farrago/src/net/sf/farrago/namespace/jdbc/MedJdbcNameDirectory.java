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

import java.sql.*;

import java.util.*;

import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.type.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;


/**
 * MedJdbcNameDirectory implements the FarragoMedNameDirectory interface by
 * mapping the metadata provided by any JDBC driver.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedJdbcNameDirectory
    extends MedAbstractNameDirectory
{
    //~ Instance fields --------------------------------------------------------

    final MedJdbcDataServer server;

    String schemaName;

    final boolean shouldSubstituteTypes;

    final Properties typeMapping;

    //~ Constructors -----------------------------------------------------------

    MedJdbcNameDirectory(MedJdbcDataServer server)
    {
        this(server, null);
    }

    MedJdbcNameDirectory(MedJdbcDataServer server, String schemaName)
    {
        this.server = server;
        this.schemaName = schemaName;
        shouldSubstituteTypes =
            getBooleanProperty(
                server.getProperties(),
                MedJdbcDataServer.PROP_TYPE_SUBSTITUTION,
                true);
        this.typeMapping = new Properties();
        String mappingsString =
            server.getProperties().getProperty(
                MedJdbcDataServer.PROP_TYPE_MAPPING,
                "");
        String [] mappingsArray = mappingsString.split(";");
        for (int i = 0; i < mappingsArray.length; i++) {
            addTypeMapping(mappingsArray[i]);
        }
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoMedNameDirectory
    public FarragoMedColumnSet lookupColumnSet(
        FarragoTypeFactory typeFactory,
        String foreignName,
        String [] localName)
        throws SQLException
    {
        return lookupColumnSetAndImposeType(
            typeFactory,
            foreignName,
            localName,
            null);
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
        if ((server.schemaName != null)
            && !server.useSchemaNameAsForeignQualifier)
        {
            foreignQualifiedName = new String[] { foreignName };
        } else {
            // schema mapping
            Map<String, String> schemaMap = server.schemaMaps.get(schemaName);
            if (schemaMap != null) {
                schemaName = schemaMap.get(foreignName);
            }
            // table mapping
            Map<String, MedJdbcDataServer.Source> tableMap =
                server.tableMaps.get(schemaName);
            if (tableMap != null) {
                MedJdbcDataServer.Source sources = tableMap.get(foreignName);
                if (sources != null) {
                    schemaName = sources.getSchema();
                    foreignName = sources.getTable();
                }
            }
            if (schemaName == null || foreignName == null) {
                return null;
            }
            if (server.catalogName != null) {
                foreignQualifiedName =
                    new String[] {
                        server.catalogName, schemaName, foreignName
                    };
            } else {
                foreignQualifiedName = new String[] {
                    schemaName, foreignName
                };
            }
        }
        RelDataType origRowType = null;
        RelDataType mdRowType = null;

        SqlDialect dialect = new SqlDialect(server.databaseMetaData);
        SqlSelect select =
            SqlStdOperatorTable.selectOperator.createCall(
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

        String sql = select.toSqlString(dialect);
        sql = normalizeQueryString(sql);

        PreparedStatement ps = null;
        try {
            ps = server.getConnection().prepareStatement(sql);
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
                // Some drivers can't return metadata before execution. Fall
                // through to recovery below.
            }
            if (md == null) {
                if (ps != null) {
                    rs = ps.executeQuery();
                } else {
                    stmt = server.getConnection().createStatement();
                    rs = stmt.executeQuery(sql);
                }
                md = rs.getMetaData();
            }
            if (rowType == null) {
                rowType =
                    typeFactory.createResultSetType(
                        md,
                        shouldSubstituteTypes,
                        typeMapping);
                origRowType = rowType;
                mdRowType = rowType;
            } else {
                origRowType = rowType;
                mdRowType =
                    typeFactory.createResultSetType(
                        md,
                        true,
                        typeMapping);

                // if LENIENT, map names
                if (server.lenient) {
                    rowType =
                        updateRowType(
                            typeFactory,
                            rowType,
                            mdRowType);

                    List<SqlIdentifier> projList =
                        new ArrayList<SqlIdentifier>();
                    for (RelDataTypeField field : rowType.getFieldList()) {
                        projList.add(
                            new SqlIdentifier(
                                field.getName(),
                                SqlParserPos.ZERO));
                    }

                    // push down projections, if any
                    if (projList.size() > 0) {
                        select =
                            SqlStdOperatorTable.selectOperator.createCall(
                                null,
                                new SqlNodeList(
                                    Collections.unmodifiableList(
                                        projList),
                                    SqlParserPos.ZERO),
                                new SqlIdentifier(
                                    foreignQualifiedName,
                                    SqlParserPos.ZERO),
                                null,
                                null,
                                null,
                                null,
                                null,
                                SqlParserPos.ZERO);
                    }
                } else {
                    // Server is strict: make sure the inferred
                    // row type is compatible with the imposed row type
                    validateRowType(
                        rowType,
                        typeFactory.createResultSetType(
                            md,
                            true,
                            typeMapping));
                }
            }
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

        return new MedJdbcColumnSet(
            this,
            foreignQualifiedName,
            localName,
            select,
            dialect,
            rowType,
            origRowType,
            mdRowType);
    }

    String normalizeQueryString(String sql)
    {
        // some drivers don't like multi-line SQL, so convert all
        // whitespace into plain spaces, and also mask Windows
        // line-ending diffs
        sql = sql.replaceAll("\\r\\n", " ");
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
            boolean wantSchemas =
                query.getResultObjectTypes().contains(
                    FarragoMedMetadataQuery.OTN_SCHEMA);
            if (wantSchemas) {
                return querySchemas(query, sink);
            }
            return true;
        } else {
            boolean wantTables =
                query.getResultObjectTypes().contains(
                    FarragoMedMetadataQuery.OTN_TABLE);
            boolean wantColumns =
                query.getResultObjectTypes().contains(
                    FarragoMedMetadataQuery.OTN_COLUMN);
            List<String> tableListActual = new ArrayList<String>();
            List<String> schemaListActual = new ArrayList<String>();
            List<String> tableListOptimized = new ArrayList<String>();

            // FRG-137: Since we rely on queryTable to populate the lists, we
            // need to do it for wantColumns even when !wantTables.
            if (wantTables || wantColumns) {
                if (!queryTables(
                        query,
                        sink,
                        tableListActual,
                        schemaListActual,
                        tableListOptimized))
                {
                    return false;
                }
            }
            if (wantColumns) {
                if (!queryColumns(
                        query,
                        sink,
                        tableListActual,
                        schemaListActual,
                        tableListOptimized))
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
        assert (schemaName == null);

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
                if (server.catalogName != null) {
                    String catalogName = resultSet.getString(2);
                    if (!server.catalogName.equals(catalogName)) {
                        continue;
                    }
                }

                sink.writeObjectDescriptor(
                    schemaName,
                    FarragoMedMetadataQuery.OTN_SCHEMA,
                    null,
                    new Properties());
            }
        } finally {
            resultSet.close();
        }

        return true;
    }

    private boolean queryTables(
        FarragoMedMetadataQuery query,
        FarragoMedMetadataSink sink,
        List<String> tableListActual,
        List<String> schemaListActual,
        List<String> tableListOptimized)
        throws SQLException
    {
        assert (schemaName != null);

        // In order to optimize column retrieval, we keep track of the
        // number of tables returned by our metadata query and the
        // ones actually accepted by the sink.  It would be better
        // to let the optimizer handle this, but KISS for now.
        int nTablesReturned = 0;

        String [] schemaPatterns = getSchemaPattern();

        ResultSet resultSet = null;
        boolean noResults = true;
        try {
            for (String schemaPattern : schemaPatterns) {
                String [] tablePatterns = getTablePattern(
                    query, FarragoMedMetadataQuery.OTN_TABLE, schemaPattern);
                for (String tablePattern : tablePatterns) {
                    try {
                        resultSet =
                            server.databaseMetaData.getTables(
                                server.catalogName,
                                schemaPattern,
                                tablePattern,
                                server.tableTypes);
                    } catch (Throwable ex) {
                        // assume unsupported
                        return false;
                    }
                    if (resultSet == null) {
                        continue;
                    }
                    noResults = false;

                    Properties props = new Properties();
                    while (resultSet.next()) {
                        ++nTablesReturned;
                        String schemaName = resultSet.getString(2);
                        if (!matchSchema(schemaPattern, schemaName)) {
                            continue;
                        }
                        String tableName = resultSet.getString(3);
                        String remarks = resultSet.getString(5);
                        if (schemaName != null) {
                            props.put(
                                MedJdbcDataServer.PROP_SCHEMA_NAME,
                                schemaName);
                        }
                        props.put(MedJdbcDataServer.PROP_TABLE_NAME,
                            tableName);
                        // table mapping
                        String mappedTableName = getMappedTableName(
                            schemaName, tableName, this.schemaName);
                        boolean include =
                            sink.writeObjectDescriptor(
                                mappedTableName,
                                FarragoMedMetadataQuery.OTN_TABLE,
                                remarks,
                                props);
                        if (include) {
                            tableListActual.add(tableName);
                            schemaListActual.add(schemaPattern);
                        }
                    }
                }
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }

        // decide on column retrieval plan
        double dMatching = (double) tableListActual.size();

        // +1:  avoid division by zero
        double dReturned = (double) nTablesReturned + 1;
        if ((dMatching / dReturned) > 0.3 &&
            server.tableMaps.get(this.schemaName) == null) {
            // a significant portion of the tables returned are matches,
            // so just scan all columns at once and post-filter them,
            // rather than making repeated single-table metadata calls
            tableListOptimized.add("*");
        } else {
            tableListOptimized.addAll(tableListActual);
        }

        return !noResults;
    }

    private boolean queryColumns(
        FarragoMedMetadataQuery query,
        FarragoMedMetadataSink sink,
        List<String> tableListActual,
        List<String> schemaListActual,
        List<String> tableListOptimized)
        throws SQLException
    {
        if (tableListOptimized.equals(Collections.singletonList("*"))) {
            return queryColumnsImpl(
                query,
                sink,
                null,
                null,
                new HashSet<String>(tableListActual));
        } else {
            Iterator<String> iter = tableListOptimized.iterator();
            Iterator<String> iter2 = schemaListActual.iterator();
            while (iter.hasNext()) {
                String tableName = iter.next();
                String actualSchemaName = iter2.next();
                if (!queryColumnsImpl(
                        query, sink, tableName, actualSchemaName, null)) {
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
        String actualSchemaName,
        Set<String> tableSet)
        throws SQLException
    {
        String [] schemaPatterns = getSchemaPattern();
        String tablePattern;
        if (tableName != null) {
            tablePattern = tableName;
        } else {
            tablePattern =
                getFilterPattern(
                    query,
                    FarragoMedMetadataQuery.OTN_TABLE);
        }
        String columnPattern =
            getFilterPattern(
                query,
                FarragoMedMetadataQuery.OTN_COLUMN);

        ResultSet resultSet = null;
        boolean noResults = true;
        try {
            for (String schemaPattern : schemaPatterns) {
                if ((actualSchemaName != null) &&
                    !schemaPattern.equals(actualSchemaName)) {
                    continue;
                }
                try {
                    resultSet =
                        server.databaseMetaData.getColumns(
                            server.catalogName,
                            schemaPattern,
                            tablePattern,
                            columnPattern);
                } catch (Throwable ex) {
                    // assume unsupported
                    return false;
                }
                if (resultSet == null) {
                    continue;
                }
                noResults = false;
                while (resultSet.next()) {
                    String schemaName = resultSet.getString(2);
                    if (!matchSchema(schemaPattern, schemaName)) {
                        continue;
                    }
                    String returnedTableName = resultSet.getString(3);
                    returnedTableName = getMappedTableName(
                        schemaName, returnedTableName, this.schemaName);
                    if (tableSet != null) {
                        if (!tableSet.contains(returnedTableName)) {
                            continue;
                        }
                    }
                    String columnName = resultSet.getString(4);
                    RelDataType type =
                        sink.getTypeFactory().createJdbcColumnType(
                            resultSet,
                            shouldSubstituteTypes,
                            typeMapping);
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
                        new Properties());
                }
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }
        return !noResults;
    }

    private String [] getSchemaPattern()
    {
        if ((server.schemaName != null)
            && !server.useSchemaNameAsForeignQualifier)
        {
            // schemaName is fake; don't use it
            return new String[] { null };
        }

        List<String> allSchemas = new ArrayList<String>();

        // schema mapping
        if (server.getProperties().getProperty(
                MedJdbcDataServer.PROP_SCHEMA_MAPPING) != null) {
            Map<String, String> map = server.schemaMaps.get(schemaName);
            if (map != null) {
                for (String s : map.values()) {
                    if (!allSchemas.contains(s)) {
                        allSchemas.add(s);
                    }
                }
            }
        }

        // table mapping
        if (server.getProperties().getProperty(
                MedJdbcDataServer.PROP_TABLE_MAPPING) != null) {
            Map<String, MedJdbcDataServer.Source> map =
                server.tableMaps.get(schemaName);
            if (map != null) {
                for (MedJdbcDataServer.Source source : map.values()) {
                    String sch = source.getSchema();
                    if (!allSchemas.contains(sch)) {
                        allSchemas.add(sch);
                    }
                }
            }
        }

        if (allSchemas.size() > 0) {
            return allSchemas.toArray(
                new String[allSchemas.size()]);
        } else {
            return new String[] { schemaName };
        }

    }

    private String [] getTablePattern(
        FarragoMedMetadataQuery query,
        String typeName,
        String schema)
    {
        List<String> allTables = new ArrayList<String>();
        String mapping = server.getProperties().getProperty(
            MedJdbcDataServer.PROP_TABLE_MAPPING);
        if (mapping != null) {
            Map<String, MedJdbcDataServer.Source> map =
                server.tableMaps.get(schemaName);
            if (map != null) {
                for (MedJdbcDataServer.Source source : map.values()) {
                    String sch = source.getSchema();
                    if (sch.equals(schema)) {
                        allTables.add(source.getTable());
                    }
                }
            }
        }
        if (allTables.size() == 0) {
            return new String[] {getFilterPattern(query, typeName)};
        }
        return allTables.toArray(
            new String[allTables.size()]);
    }

    private String getFilterPattern(
        FarragoMedMetadataQuery query,
        String typeName)
    {
        String pattern = "%";
        FarragoMedMetadataFilter filter =
            (FarragoMedMetadataFilter) query.getFilterMap().get(typeName);
        if (filter != null) {
            if (!filter.isExclusion() && (filter.getPattern() != null)) {
                pattern = filter.getPattern();
            }
        }
        return pattern;
    }

    private String getMappedTableName(
        String schema,
        String table,
        String origSchema)
    {
        Map<String, MedJdbcDataServer.Source> map =
            server.tableMaps.get(origSchema);
        if (map == null) {
            return table;
        }

        for (Map.Entry<String, MedJdbcDataServer.Source> entry
            : map.entrySet())
        {
            if (schema.equals(entry.getValue().getSchema())
                && table.equals(entry.getValue().getTable())) {
                return entry.getKey();
            }
        }
        return table;
    }

    private boolean matchSchema(String s1, String s2)
    {
        if ((s1 == null) || (s2 == null)) {
            return true;
        }
        return s1.equals(s2);
    }

    private void addTypeMapping(String s)
    {
        String [] map = s.split(":");
        if (map.length != 2) {
            return;
        }

        // store in Properties as DATATYPE(P,S);
        // ie. all upper-case and lose whitespace
        String key = map[0].trim().toUpperCase().replaceAll("\\s", "");
        String value = map[1].trim().toUpperCase().replaceAll("\\s", "");

        if (!key.equals("") && !value.equals("")) {
            this.typeMapping.setProperty(key, value);
        }
    }

    private RelDataType updateRowType(
        FarragoTypeFactory typeFactory,
        RelDataType currRowType,
        RelDataType srcRowType)
    {
        Map<String, RelDataType> srcMap =
            new HashMap<String, RelDataType>();
        for (RelDataTypeField srcField : srcRowType.getFieldList()) {
            srcMap.put(srcField.getName(), srcField.getType());
        }

        List<String> fieldList = new ArrayList<String>();
        List<RelDataType> typeList = new ArrayList<RelDataType>();

        for (RelDataTypeField currField : currRowType.getFieldList()) {
            RelDataType type = srcMap.get(currField.getName());
            if (type != null
                && SqlTypeUtil.canCastFrom(currField.getType(), type, true))
            {
                fieldList.add(currField.getName());
                typeList.add(type);
            }
        }

        return typeFactory.createStructType(typeList, fieldList);
    }

    private void validateRowType(RelDataType rowType, RelDataType srcRowType)
    {
        RelDataTypeField [] fieldList = rowType.getFields();
        RelDataTypeField [] srcFieldList = srcRowType.getFields();

        // check that the number of fields match
        if (fieldList.length != srcFieldList.length) {
            throw FarragoResource.instance().NumberOfColumnsMismatch.ex(
                Integer.toString(fieldList.length),
                Integer.toString(srcFieldList.length));
        }

        // check that types of fields are compatible
        for (int i = 0; i < fieldList.length; i++) {
            if (!SqlTypeUtil.canCastFrom(
                    fieldList[i].getType(),
                    srcFieldList[i].getType(),
                    true))
            {
                throw FarragoResource.instance().TypesOfColumnsMismatch.ex(
                    srcFieldList[i].getName(),
                    srcFieldList[i].getType().toString(),
                    fieldList[i].getType().toString(),
                    fieldList[i].getName());
            }
        }
    }
}

// End MedJdbcNameDirectory.java
