/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
import java.util.logging.*;

import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.type.*;
import net.sf.farrago.trace.FarragoTrace;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.util.*;


/**
 * MedJdbcNameDirectory implements the FarragoMedNameDirectory interface by
 * mapping the metadata provided by any JDBC driver.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class MedJdbcNameDirectory
    extends MedAbstractNameDirectory
{
    final protected static Logger tracer =
        FarragoTrace.getFarragoMedJdbcTracer();

    //~ Instance fields --------------------------------------------------------

    final protected MedJdbcDataServer server;

    String schemaName;

    final boolean shouldSubstituteTypes;

    final Properties typeMapping;

    //~ Constructors -----------------------------------------------------------

    public MedJdbcNameDirectory(MedJdbcDataServer server)
    {
        this(server, null);
    }

    public MedJdbcNameDirectory(MedJdbcDataServer server, String schemaName)
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

    /**
     * @return the server exposing this directory
     */
    public MedJdbcDataServer getServer()
    {
        return server;
    }

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
            null,
            localName,
            null,
            false);
    }

    /**
     * Looks up the FarragoMedColumnSet for the given table.
     *
     * @param typeFactory typeFactory to use for type mapping
     * @param foreignName foreign table name
     * @param localName fully qualified local table name
     * @param rowType expected row type
     * @param tableAlreadyMapped if true, foreignName has already been mapped to
     * the foreign database name; if false the mapping has not yet been applied
     *
     * @return a FarragoMedColumnSet representing the table
     *
     * @throws SQLException if there is an error querying metadata in the
     * underlying database
     */
    FarragoMedColumnSet lookupColumnSetAndImposeType(
        FarragoTypeFactory typeFactory,
        String foreignName,
        Properties foreignTableProps,   // may add nuance to foreignName
        String [] localName,
        RelDataType rowType,
        boolean tableAlreadyMapped)
        throws SQLException
    {
        if (schemaName == null) {
            return null;
        }

        String [] foreignQualifiedName;
        if ((server.schemaName != null)
            && !server.useSchemaNameAsForeignQualifier)
        {
            if (!tableAlreadyMapped) {
                List<MedJdbcDataServer.WildcardMapping> tablePrefixMappings =
                    server.tablePrefixMaps.get(schemaName);
                if (tablePrefixMappings != null) {
                    for (
                        MedJdbcDataServer.WildcardMapping m
                        : tablePrefixMappings)
                    {
                        String targetTablePrefix = m.getTargetTablePrefix();
                        if (foreignName.startsWith(targetTablePrefix)) {
                            foreignName =
                                m.getSourceTablePrefix()
                                + foreignName.substring(
                                    targetTablePrefix.length());
                            break;
                        }
                    }
                }
            }

            foreignQualifiedName = new String[] { foreignName };
        } else {
            if (!tableAlreadyMapped) {
                // Expect only one of schema mapping, table mapping and table
                // prefix to be non-empty/null.

                // schema mapping
                Map<String, String> schemaMap =
                    server.schemaMaps.get(schemaName);
                if (schemaMap != null) {
                    schemaName = schemaMap.get(foreignName);
                }

                // table mapping
                Map<String, MedJdbcDataServer.Source> tableMap =
                    server.tableMaps.get(schemaName);
                if (tableMap != null) {
                    MedJdbcDataServer.Source sources =
                        tableMap.get(foreignName);
                    if (sources != null) {
                        schemaName = sources.getSchema();
                        foreignName = sources.getTable();
                    }
                }

                // table name prefix
                List<MedJdbcDataServer.WildcardMapping> tablePrefixMappings =
                    server.tablePrefixMaps.get(schemaName);
                if (tablePrefixMappings != null) {
                    for (
                        MedJdbcDataServer.WildcardMapping m
                        : tablePrefixMappings)
                    {
                        String targetTablePrefix = m.getTargetTablePrefix();
                        if (foreignName.startsWith(targetTablePrefix)) {
                            schemaName = m.getSourceSchema();
                            foreignName =
                                m.getSourceTablePrefix()
                                + foreignName.substring(
                                    targetTablePrefix.length());
                            break;
                        }
                    }
                }
            }

            if ((schemaName == null) || (foreignName == null)) {
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

        SqlDialect dialect = SqlDialect.create(server.getDatabaseMetaData());
        SqlSelect select =
            newSelectStarQuery(foreignQualifiedName, foreignTableProps);

        if (server.skipTypeCheck && (rowType != null)) {
            // tolerant mode:
            // skip type check when row type already defined in catalog
            origRowType = rowType;
            mdRowType = rowType;
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

        // fetch row type from foreign server
        SqlString sql = select.toSqlString(dialect);
        sql = normalizeQueryString(sql);
        if (tracer.isLoggable(Level.FINE)) {
            tracer.fine("get foreign table metadata using " + sql);
        }

        PreparedStatement ps = null;
        try {
            ps = server.getConnection().prepareStatement(sql.getSql());
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
                    rs = stmt.executeQuery(sql.getSql());
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
                        select = createSelectNode(
                            new SqlNodeList(
                                Collections.unmodifiableList(projList),
                                SqlParserPos.ZERO),
                            foreignQualifiedName);
                    }
                } else {
                    // Server is strict: make sure the inferred
                    // row type is compatible with the imposed row type
                    validateRowType(origRowType, mdRowType);
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

    protected SqlSelect
        newSelectStarQuery(String[] qualifiedName, Properties tableProps)
        throws SQLException
    {
        return createSelectNode(
            new SqlNodeList(
                Collections.singletonList(
                    new SqlIdentifier("*", SqlParserPos.ZERO)),
                SqlParserPos.ZERO),
            qualifiedName);
    }

    protected SqlSelect
        createSelectNode(SqlNodeList selectList, String[] foreignQualifiedName)
        throws SQLException
    {
        SqlSelect select =
            SqlStdOperatorTable.selectOperator.createCall(
                null,
                selectList,
                new SqlIdentifier(foreignQualifiedName, SqlParserPos.ZERO),
                null,
                null,
                null,
                null,
                null,
                SqlParserPos.ZERO);
        return select;
    }


    SqlString normalizeQueryString(SqlString sql)
    {
        // some drivers don't like multi-line SQL, so convert all
        // whitespace into plain spaces, and also mask Windows
        // line-ending diffs
        String s = sql.getSql();
        s = s.replaceAll("\\r\\n", " ");
        s = s.replaceAll("\\s", " ");
        return new SqlBuilder(sql.getDialect(), s).toSqlString();
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
            resultSet = server.getDatabaseMetaData().getSchemas();
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
                String [] tablePatterns =
                    getTablePattern(
                        query,
                        FarragoMedMetadataQuery.OTN_TABLE,
                        schemaPattern);
                for (String tablePattern : tablePatterns) {
                    try {
                        resultSet =
                            server.getDatabaseMetaData().getTables(
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
                        props.put(
                            MedJdbcDataServer.PROP_TABLE_NAME,
                            tableName);

                        // table mapping
                        String mappedTableName =
                            getMappedTableName(
                                schemaName,
                                tableName,
                                this.schemaName);
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
        if (((dMatching / dReturned) > 0.3)
            && (server.tableMaps.get(this.schemaName) == null))
        {
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
                        query,
                        sink,
                        tableName,
                        actualSchemaName,
                        null))
                {
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
                if ((actualSchemaName != null)
                    && !schemaPattern.equals(actualSchemaName))
                {
                    continue;
                }
                try {
                    resultSet =
                        server.getDatabaseMetaData().getColumns(
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
                    if (tableSet != null) {
                        if (!tableSet.contains(returnedTableName)) {
                            continue;
                        }
                    }
                    returnedTableName =
                        getMappedTableName(
                            schemaName,
                            returnedTableName,
                            this.schemaName);
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
                MedJdbcDataServer.PROP_SCHEMA_MAPPING)
            != null)
        {
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
                MedJdbcDataServer.PROP_TABLE_MAPPING)
            != null)
        {
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

        // table prefix mapping
        if (server.getProperties().getProperty(
                MedJdbcDataServer.PROP_TABLE_PREFIX_MAPPING)
            != null)
        {
            List<MedJdbcDataServer.WildcardMapping> list =
                server.tablePrefixMaps.get(schemaName);
            if (list != null) {
                for (MedJdbcDataServer.WildcardMapping mapping : list) {
                    String sch = mapping.getSourceSchema();
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
        String mapping =
            server.getProperties().getProperty(
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
            return new String[] { getFilterPattern(query, typeName) };
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

    /**
     * Convert the foreign database's schema and table into the "foreign" name
     * used locally by Farrago in <code>origSchema</code>.
     *
     * @param schema foreign database's schema
     * @param table foreign database's table
     * @param origSchema Farrago schema to which the table is mapped
     *
     * @return the Farrago foreign table to which schema.table is mapped, of
     * <code>table</code> if no mapping is found
     */
    private String getMappedTableName(
        String schema,
        String table,
        String origSchema)
    {
        Map<String, MedJdbcDataServer.Source> map =
            server.tableMaps.get(origSchema);
        if (map != null) {
            for (
                Map.Entry<String, MedJdbcDataServer.Source> entry
                : map.entrySet())
            {
                if (schema.equals(entry.getValue().getSchema())
                    && table.equals(entry.getValue().getTable()))
                {
                    return entry.getKey();
                }
            }
        }

        List<MedJdbcDataServer.WildcardMapping> list =
            server.tablePrefixMaps.get(origSchema);
        if (list != null) {
            for (MedJdbcDataServer.WildcardMapping mapping : list) {
                String sourceTablePrefix = mapping.getSourceTablePrefix();
                if ((!server.useSchemaNameAsForeignQualifier
                        || schema.equals(mapping.getSourceSchema()))
                    && table.startsWith(sourceTablePrefix))
                {
                    return mapping.getTargetTablePrefix()
                        + table.substring(sourceTablePrefix.length());
                }
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
        Map<String, RelDataType> srcMap = new HashMap<String, RelDataType>();
        for (RelDataTypeField srcField : srcRowType.getFieldList()) {
            srcMap.put(srcField.getName(), srcField.getType());
        }

        List<String> fieldList = new ArrayList<String>();
        List<RelDataType> typeList = new ArrayList<RelDataType>();

        for (RelDataTypeField currField : currRowType.getFieldList()) {
            RelDataType type = srcMap.get(currField.getName());
            if ((type != null)
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

    /**
     * Tests whether a remote SQL query is valid by attempting
     * to prepare it.  This is intended for use by pushdown rules
     * constructing remote SQL from fragments of relational algebra.
     *
     * @param sqlNode SQL query to be tested
     *
     * @return true if statement is valid
     */
    protected boolean isRemoteSqlValid(SqlNode sqlNode)
    {
        try {
            SqlDialect dialect =
                SqlDialect.create(server.getDatabaseMetaData());
            SqlString sql = sqlNode.toSqlString(dialect);
            sql = normalizeQueryString(sql);

            // test if sql can be executed against source
            ResultSet rs = null;
            PreparedStatement ps = null;
            Statement testStatement = null;
            try {
                // Workaround for Oracle JDBC thin driver, where
                // PreparedStatement.getMetaData does not actually get metadata
                // before execution
                if (dialect.getDatabaseProduct()
                    == SqlDialect.DatabaseProduct.ORACLE)
                {
                    SqlBuilder buf = new SqlBuilder(dialect);
                    buf.append(
                        " DECLARE"
                        + "   test_cursor integer;"
                        + " BEGIN"
                        + "   test_cursor := dbms_sql.open_cursor;"
                        + "   dbms_sql.parse(test_cursor, ");
                    buf.literal(dialect.quoteStringLiteral(sql.getSql()));
                    buf.append(
                        ", "
                        + "   dbms_sql.native);"
                        + "   dbms_sql.close_cursor(test_cursor);"
                        + " EXCEPTION"
                        + " WHEN OTHERS THEN"
                        + "   dbms_sql.close_cursor(test_cursor);"
                        + "   RAISE;"
                        + " END;");
                    testStatement = server.getConnection().createStatement();
                    SqlString sqlTest = buf.toSqlString();
                    rs = testStatement.executeQuery(sqlTest.getSql());
                } else {
                    ps = server.getConnection().prepareStatement(sql.getSql());
                    if (ps != null) {
                        if (ps.getMetaData() == null) {
                            return false;
                        }
                    }
                }
            } catch (SQLException ex) {
                return false;
            } catch (RuntimeException ex) {
                return false;
            } finally {
                try {
                    if (rs != null) {
                        rs.close();
                    }
                    if (testStatement != null) {
                        testStatement.close();
                    }
                    if (ps != null) {
                        ps.close();
                    }
                } catch (SQLException sqe) {
                }
            }
        } catch (SQLException ex) {
            return false;
        }
        return true;
    }
}

// End MedJdbcNameDirectory.java
