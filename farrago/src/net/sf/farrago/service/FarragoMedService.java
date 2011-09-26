/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2011 The Eigenbase Project
// Copyright (C) 2011 SQLstream, Inc.
// Copyright (C) 2011 Dynamo BI Corporation
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
public class FarragoMedService extends FarragoService
{
    private final Locale locale;

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
        this(dataSource, locale, tracer, false);
    }

    public FarragoMedService(
        DataSource dataSource,
        Locale locale,
        Logger tracer,
        boolean reusingConnection)
    {
        super(dataSource, tracer, reusingConnection);
        this.locale = locale;
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
                true
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
            connection = getConnection();
            Statement statement = getStatement(connection);
            ResultSet resultSet = statement.executeQuery(sql);
            List<DriverPropertyInfo> driverPropertyInfo =
                new ArrayList<DriverPropertyInfo>();
            assert resultSet.getMetaData().getColumnCount() == 6;
            toDriverProperties(resultSet, driverPropertyInfo);
            releaseStatement(statement);
            releaseConnection(connection);
            return driverPropertyInfo;
        } catch (SQLException exception) {
            tracer.fine(
                "Exception in getProperties with query of: " + methodName
                + "\nException is: " + exception);
            tracer.fine("Stack trace:\n" + Util.getStackTrace(exception));
            throw exception;
        } finally {
            releaseConnection(connection);
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
        final String sql = selectSqlBuilder.getSql().replace('\n', ' ');
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
        final String sql = selectSqlBuilder.getSql().replace('\n', ' ');
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
}

// End FarragoMedService.java
