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
package net.sf.farrago.service;

import java.sql.*;
import java.util.*;
import java.util.logging.Logger;
import javax.sql.DataSource;

import org.eigenbase.sql.SqlDialect;
import org.eigenbase.sql.util.SqlBuilder;
import org.eigenbase.util.Util;

/**
 * A collection of methods to query SQL/MED metadata.
 *
 * @author Julian Hyde
 * @version $Id$
 */
public class FarragoMedService
{
    private final DataSource dataSource;
    private final Locale locale;
    private final Logger tracer;

    /**
     * Creates the service.
     *
     * @param dataSource Connection factory
     * @param locale Locale
     * @param tracer Tracer to which messages are logged (usually FINE)
     */
    public FarragoMedService(
        DataSource dataSource,
        Locale locale,
        Logger tracer)
    {
        this.dataSource = dataSource;
        this.locale = locale;
        this.tracer = tracer;
    }

    private SqlBuilder createSqlBuilder()
    {
        return new SqlBuilder(SqlDialect.EIGENBASE);
    }

    /**
     * Converts a set of (name, value) options into a cursor that returns those
     * options, and appends that cursor to a SQL builder.
     *
     * <p>For example,
     * <blockquote>cursor(<br/>
     * &nbsp;&nbsp;values ('FOREIGN_SCHEMA_NAME', 'Larry'),<br>
     * &nbsp;&nbsp;&nbsp;&nbsp;('EXECUTOR_IMPL', 'FENNEL'))</blockquote>
     *
     * @param sqlBuilder SQL builder
     * @param options Map containing options
     * @return The SQL builder
     */
    private SqlBuilder toValues(
        SqlBuilder sqlBuilder,
        Map<String, String> options)
    {
        sqlBuilder.append("cursor(");
        if (options.isEmpty()) {
            sqlBuilder.append(
                // Attempt to work around dtbug-2387.
                false
                ? "table sys_boot.mgmt.browse_connect_empty_options"
                : "select * from (values ('k', 'v')) where true");
        } else {
            sqlBuilder.append("VALUES ");
            int n = 0;
            for (Map.Entry<String, String> entry : options.entrySet()) {
                if (n++ > 0) {
                    sqlBuilder.append(", ");
                }
                sqlBuilder.append("(")
                    .literal(entry.getKey())
                    .append(", ")
                    .literal(entry.getValue())
                    .append(")");
            }
        }
        sqlBuilder.append(")");
        return sqlBuilder;
    }

    /**
     * Generates a SQL query to get MED metadata from farrago.
     *
     * @param query Method name, e.g. "getColumnPropertyInfo"
     * @param libraryFileName Library file name
     * @param mofId MOF id of the wrapper
     * @return SQL query
     */
    private String generateMedQuery(
        String query,
        String libraryFileName,
        final String mofId)
    {
        final SqlBuilder sqlBuilder = createSqlBuilder();
        sqlBuilder
            .append(
                "select stream name, avalue, description, choices, required "
                + "from table(sys_boot.mgmt.")
            .append(query)
            .append("(")
            .literal(mofId)
            .append(", ")
            .literal(libraryFileName)
            .append(", ")
            .literal("")
            .append(", ")
            .literal("")
            .append(", ")
            .literal("")
            .append(", ")
            .literal(locale.toString())
            .append("))");
        return sqlBuilder.getSql();
    }

    /**
     * Moves a JDBC ResultSet from a Farrago MED query into a
     * DriverPropertyInfo array.
     *
     * <p>Assumes that columns are (OPTION_ORDINAL, OPTION_NAME,
     * OPTION_DESCRIPTION, IS_OPTION_REQUIRED, OPTION_CHOICE_ORDINAL,
     * OPTION_CHOICE_VALUE) and the relation is sorted by OPTION_ORDINAL,
     * OPTION_CHOICE_ORDINAL.
     *
     * @param resultSet The JDBC ResultSet to get data from
     * @param driverProperties List to write Farrago MED data into
     * @throws SQLException on database error
     */
    private void toDriverProperties(
        ResultSet resultSet,
        List<DriverPropertyInfo> driverProperties)
        throws SQLException
    {
        final List<String> optionChoiceValues = new ArrayList<String>();
        while (resultSet.next()) {
            int optionOrdinal = resultSet.getInt(1);
            Util.discard(optionOrdinal);
            String optionName = resultSet.getString(2);
            String optionDescription = resultSet.getString(3);
            boolean optionRequired = resultSet.getBoolean(4);
            int optionChoiceOptional = resultSet.getInt(5);
            String optionChoiceValue = resultSet.getString(6);
            if (optionChoiceOptional == -1) {
                flushChoices(driverProperties, optionChoiceValues);
                DriverPropertyInfo driverPropertyInfo =
                    new DriverPropertyInfo(optionName, optionChoiceValue);
                driverPropertyInfo.description = optionDescription;
                driverPropertyInfo.required = optionRequired;
                driverProperties.add(driverPropertyInfo);
            } else {
                optionChoiceValues.add(optionChoiceValue);
            }
        }
        flushChoices(driverProperties, optionChoiceValues);
    }

    private void flushChoices(
        List<DriverPropertyInfo> driverProperties,
        List<String> optionChoiceValues)
    {
        if (driverProperties.size() <= 0) {
            return;
        }
        final DriverPropertyInfo driverProperty =
            driverProperties.get(driverProperties.size() - 1);
        if (optionChoiceValues.size() > 0) {
            driverProperty.choices =
                optionChoiceValues.toArray(
                    new String[optionChoiceValues.size()]);
            optionChoiceValues.clear();
        }
        tracer.fine(
            "Got Farrago MED driver properties: "
            + "name [" + driverProperty.name
            + "] value [" + driverProperty.value
            + "] required [" + driverProperty.required
            + "] description [" + driverProperty.description
            + "] choices "
            + (driverProperty.choices == null
               ? Collections.<String>emptyList()
               : Arrays.asList(driverProperty.choices)));
    }

    public static String toString(DriverPropertyInfo oneInfo)
    {
        return new StringBuilder()
            .append("DriverProperty(name: ")
            .append(oneInfo.name)
            .append(", value: ")
            .append(oneInfo.value)
            .append(", description: ")
            .append(oneInfo.description)
            .append(", choices: ")
            .append(Arrays.toString(oneInfo.choices))
            .append(")")
            .toString();
    }

    /**
     * Gets a set of Farrago MED properties.  This acts as a worker method
     * for the getPluginProperties, etc. methods.  They setup the parameters
     * necessary to get specific data and this method fulfills the request.
     *
     * @param methodName Name of UDX; for debugging
     * @param sql The SQL query to perform to collect the Farrago MED data
     * @return An array of all the Farrago MED data requested
     */
    private List<DriverPropertyInfo> getProperties(
        String methodName,
        final String sql) throws SQLException
    {
        tracer.fine("Entered getProperties with query of: " + methodName);
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            List<DriverPropertyInfo> driverPropertyInfo =
                new ArrayList<DriverPropertyInfo>();
            assert resultSet.getMetaData().getColumnCount() == 6;
            toDriverProperties(resultSet, driverPropertyInfo);
            statement.close();
            connection.close();
            return driverPropertyInfo;
        } catch (SQLException exception) {
            tracer.fine(
                "Exception in getProperties with query of: " + methodName
                + "\nException is: " + exception);
            tracer.fine("Stack trace:\n" + Util.getStackTrace(exception));
            throw exception;
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException exception) {
                // Tried to close, now we give up
            }
        }
    }

    /**
     * Returns a description of the options available for a given wrapper.
     *
     * @param mofId MOF ID of wrapper
     * @param libraryFileName URI of SQL/MED wrapper's JAR file
     * @param wrapperOptions Proposed options to the wrapper
     * @return Requested options
     */
    public List<DriverPropertyInfo> getPluginProperties(
        String mofId,
        String libraryFileName,
        Map<String, String> wrapperOptions) throws SQLException
    {
        final String methodName = "get_plugin_property_info";
        final SqlBuilder sqlBuilder = createSqlBuilder();
        sqlBuilder
            .append("select * from table(sys_boot.mgmt.")
            .append(methodName)
            .append("(")
            .literal(mofId)
            .append(", ")
            .literal(libraryFileName)
            .append(", ");
        toValues(sqlBuilder, wrapperOptions)
            .append(", ")
            .literal(locale.toString())
            .append("))");
        final String sql = sqlBuilder.getSql();
        return getProperties(methodName, sql);
    }

    /**
     * Returns the properties describing a Farrago SQL/MED server.
     *
     * @param mofId MOF ID of wrapper
     * @param libraryFileName URI of SQL/MED wrapper's JAR file
     * @param serverOptions table of option NAME/VALUE pairs to use; this can be
     * empty to query for all options
     * @return An list containing the requested properties.
     */
    public List<DriverPropertyInfo> getServerProperties(
        String mofId,
        String libraryFileName,
        Map<String, String> wrapperOptions,
        Map<String, String> serverOptions)
        throws SQLException
    {
        final SqlBuilder selectSqlBuilder = createSqlBuilder();
        final String methodName = "get_server_property_info";
        selectSqlBuilder
            .append("select * from table(sys_boot.mgmt.")
            .append(methodName)
            .append("(")
            .literal(mofId)
            .append(", ")
            .literal(libraryFileName)
            .append(", ");
        toValues(selectSqlBuilder, wrapperOptions)
            .append(", ");
        toValues(selectSqlBuilder, serverOptions)
            .append(", ")
            .literal(locale.toString())
            .append("))");
        final String sql = selectSqlBuilder.getSql();
        return getProperties(methodName, sql);
    }

    /**
     * Returns the set of properties applicable to a table.
     *
     * @param serverName Name of MED server
     * @param tableOptions Table options
     * @return List of options for given table
     */
    public List<DriverPropertyInfo> browseTable(
        String serverName,
        Map<String, String> tableOptions)
        throws SQLException
    {
        final String methodName = "browse_table";
        final SqlBuilder selectSqlBuilder = createSqlBuilder();
        selectSqlBuilder
            .append("select * from table(sys_boot.mgmt.")
            .append(methodName)
            .append("(")
            .literal(serverName)
            .append(", ");
        toValues(selectSqlBuilder, tableOptions)
            .append(", ")
            .literal(locale.toString())
            .append("))");
        final String sql = selectSqlBuilder.getSql();
        return getProperties(methodName, sql);
    }

    /**
     * Returns the set of properties applicable to a column.
     *
     * @param serverName Name of MED server
     * @param tableOptions Table options
     * @param columnOptions Column options
     * @return List of options for given column
     */
    public List<DriverPropertyInfo> browseColumn(
        String serverName,
        Map<String, String> tableOptions,
        Map<String, String> columnOptions)
        throws SQLException
    {
        final String methodName = "browse_column";
        final SqlBuilder selectSqlBuilder = createSqlBuilder();
        selectSqlBuilder
            .append("select * from table(sys_boot.mgmt.")
            .append(methodName)
            .append("(")
            .literal(serverName)
            .append(", ");
        toValues(selectSqlBuilder, tableOptions)
            .append(", ");
        toValues(selectSqlBuilder, columnOptions)
            .append(", ")
            .literal(locale.toString())
            .append("))");
        final String sql = selectSqlBuilder.getSql();
        return getProperties(methodName, sql);
    }

    /**
     * Tests whether a library is valid. Throws an exception with info if the
     * library is not valid.
     *
     * @throws SQLException if library is not valid
     */
    public void checkLibraryValid(
        String mofId,
        String libraryFile)
        throws SQLException
    {
        List<DriverPropertyInfo> props =
            getPluginProperties(
                mofId,
                libraryFile,
                Collections.<String, String>emptyMap());
        assert props != null;
    }

    /**
     * Returns the set of schemas visible in a foreign server connector.
     *
     * @param serverName Name of MED server
     * @return List of name and description of each schema.
     */
    public List<ForeignSchemaInfo> browseForeignSchemas(String serverName)
        throws SQLException
    {
        final String methodName = "browse_foreign_schemas";
        final SqlBuilder selectSqlBuilder = createSqlBuilder();
        selectSqlBuilder
            .append("select * from table(sys_boot.mgmt.")
            .append(methodName)
            .append("(")
            .literal(serverName)
            .append("))");
        final String sql = selectSqlBuilder.getSql();
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        assert resultSet.getMetaData().getColumnCount() == 2;
        List<ForeignSchemaInfo> schemaInfo = new ArrayList<ForeignSchemaInfo>();
        while (resultSet.next()) {
            String schema = resultSet.getString(1);
            String desc = resultSet.getString(2);
            ForeignSchemaInfo info = new ForeignSchemaInfo(schema, desc);
            schemaInfo.add(info);
        }

        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException exception) {
            // Give up
        }
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException exception) {
            // Tried to close, now we give up
        }

        return schemaInfo;
    }

    /**
     * Returns the set of tables visible in a foreign server connector's
     * foreign schema.
     *
     * @param serverName Name of MED server
     * @param schemaName Name of foreign schema to look in for tables.
     * @return List of name and description of each table in the schema.
     */
    public List<ForeignSchemaTableAndColumnInfo> browseForeignSchemaTables(
            String serverName,
            String schemaName)
        throws SQLException
    {
        final String methodName = "browse_foreign_schema_tables";
        final SqlBuilder selectSqlBuilder = createSqlBuilder();
        selectSqlBuilder
            .append("select * from table(sys_boot.mgmt.")
            .append(methodName)
            .append("(")
            .literal(serverName)
            .append(",")
            .literal(schemaName)
            .append("))");
        final String sql = selectSqlBuilder.getSql();
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        assert resultSet.getMetaData().getColumnCount() == 2;
        List<ForeignSchemaTableAndColumnInfo> tableInfo =
            new ArrayList<ForeignSchemaTableAndColumnInfo>();
        while (resultSet.next()) {
            String table = resultSet.getString(1);
            String desc = resultSet.getString(2);
            ForeignSchemaTableAndColumnInfo info =
                new ForeignSchemaTableAndColumnInfo(table, desc);
            tableInfo.add(info);
        }

        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException exception) {
            // Give up
        }
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException exception) {
            // Tried to close, now we give up
        }

        return tableInfo;
    }

    /**
     * Returns the set of columns and their table that are visible in a
     * foreign server connector's foreign schema.
     *
     * @param serverName Name of MED server
     * @param schemaName Name of foreign schema to look in for tables.
     * @return List of name and description of each table in the schema.
     */
    public List<ForeignSchemaTableAndColumnInfo> browseForeignSchemaColumns(
            String serverName,
            String schemaName)
        throws SQLException
    {
        final String methodName = "browse_foreign_schema_columns";
        final SqlBuilder selectSqlBuilder = createSqlBuilder();
        selectSqlBuilder
            .append("select * from table(sys_boot.mgmt.")
            .append(methodName)
            .append("(")
            .literal(serverName)
            .append(",")
            .literal(schemaName)
            .append("))");
        final String sql = selectSqlBuilder.getSql();
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        assert resultSet.getMetaData().getColumnCount() == 10;
        List<ForeignSchemaTableAndColumnInfo> colInfo =
            new ArrayList<ForeignSchemaTableAndColumnInfo>();
        while (resultSet.next()) {
            ForeignSchemaTableAndColumnInfo info =
                new ForeignSchemaTableAndColumnInfo(
                        resultSet.getString(1),
                        resultSet.getString(2),
                        resultSet.getInt(3),
                        resultSet.getString(4),
                        resultSet.getInt(5),
                        resultSet.getInt(6),
                        resultSet.getBoolean(7),
                        resultSet.getString(8),
                        resultSet.getString(9),
                        resultSet.getString(10));
            colInfo.add(info);
        }

        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException exception) {
            // Give up
        }
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException exception) {
            // Tried to close, now we give up
        }

        return colInfo;
    }

    /**
     * Data structure for relaying information about browsed foreign schemas.
     */
    public class ForeignSchemaInfo
    {
        private String schemaName;
        private String description;

        public ForeignSchemaInfo(String schemaName, String description)
        {
            this.schemaName = schemaName;
            this.description = description;
        }

        public String getSchemaName()
        {
            return schemaName;
        }

        public String getDescription()
        {
            return description;
        }

        public String toString()
        {
            return "ForeignSchemaInfo(schemaName: " + schemaName
                + ", description: " + description + ")";
        }
    }

    /**
     * Data structure used for relaying information about tables and
     * table columns browsed in a foreign schema.
     */
    public class ForeignSchemaTableAndColumnInfo
    {
        private String tableName;
        private String description;
        private String columnName;
        private int ordinal;
        private String dataType;
        private int precision;
        private int decDigits;
        private boolean isNullable;
        private String formattedDataType;
        private String defaultValue;

        // if false, this represents table+column info:
        private boolean justTable;

        public ForeignSchemaTableAndColumnInfo(
                String tableName,
                String description)
        {
            this.tableName = tableName;
            this.description = description;
            this.justTable = true;
        }

        public ForeignSchemaTableAndColumnInfo(
                String tableName,
                String columnName,
                int ordinal,
                String dataType,
                int precision,
                int decDigits,
                boolean isNullable,
                String formattedDataType,
                String description,
                String defaultValue)
        {
            this.tableName = tableName;
            this.columnName = columnName;
            this.ordinal = ordinal;
            this.dataType = dataType;
            this.precision = precision;
            this.decDigits = decDigits;
            this.isNullable = isNullable;
            this.formattedDataType = formattedDataType;
            this.description = description;
            this.defaultValue = defaultValue;
            this.justTable = false;
        }

        public boolean getJustTable()
        {
            return justTable;
        }

        public String getTableName()
        {
            return tableName;
        }

        public String getColumnName()
        {
            return columnName;
        }

        public int getOrdinal()
        {
            return ordinal;
        }

        public String getDataType()
        {
            return dataType;
        }

        public int getPrecision()
        {
            return precision;
        }

        public int getDecDigits()
        {
            return decDigits;
        }

        public boolean getIsNullable()
        {
            return isNullable;
        }

        public String getFormattedDataType()
        {
            return formattedDataType;
        }

        public String getDescription()
        {
            return description;
        }

        public String getDefaultValue()
        {
            return defaultValue;
        }

        public String toString()
        {
            if (justTable) {
                return "ForeignSchemaTableAndColumnInfo(tableName: "
                    + tableName + ", description: " + description + ")";
            }
            return "ForeignSchemaTableAndColumnInfo("
                + "tableName: " + tableName + ", "
                + "columnName: " + columnName + ", "
                + "ordinal: " + ordinal + ", "
                + "dataType: " + dataType + ", "
                + "precision: " + precision + ", "
                + "decDigits: " + decDigits + ", "
                + "isNullable: " + isNullable + ", "
                + "formattedDataType: " + formattedDataType + ", "
                + "description: " + description + ", "
                + "defaultValue: " + defaultValue + ")";
        }

    }
}

// End FarragoMedService.java
