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
import java.util.logging.*;

import javax.sql.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.pretty.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.util.*;

/**
 * Container for several SQL-related services, including<ul>
 * <li>SQL reserved and keywords</li>
 * <li>SQL validation</li>
 * <li>SQL completion/hints</li>
 * </ul>
 * <p>The purpose of these services is to enable clients to obtain information
 * about the server without needing to contain server code.
 *
 * @author chard
 * @version $Id$
 */
public class FarragoSqlAdvisorService
{

    protected DataSource dataSource;
    protected Logger tracer;

    private static final String keywordQuery =
        "SELECT * FROM TABLE(SYS_BOOT.MGMT.GET_KEYWORDS())";

    /**
     * Constructs a SQL advisor service on the specified DataSource and with
     * a Logger for errors.
     * @param dataSource DataSource for connections to the server
     * @param tracer Logger for tracing and error messages
     */
    public FarragoSqlAdvisorService(DataSource dataSource, Logger tracer)
    {
        this.dataSource = dataSource;
        this.tracer = tracer;
    }

    private SqlBuilder createSqlBuilder()
    {
        return new SqlBuilder(SqlDialect.EIGENBASE);
    }

    private String generateValidationQuery(String sql)
    {
        final SqlBuilder sqlBuilder = createSqlBuilder();
        sqlBuilder.append("SELECT * FROM TABLE(SYS_BOOT.MGMT.VALIDATE_SQL(")
            .literal(sql.replaceAll("\n", " "))
            .append("))");
        return sqlBuilder.getSql();
    }

    private String generateQualifierQuery(String sql, int offset)
    {
        final SqlBuilder sqlBuilder = createSqlBuilder();
        sqlBuilder.append("VALUES(SYS_BOOT.MGMT.GET_QUALIFIED_NAME(")
            .literal(sql.replaceAll("\n", " "))
            .append(", ")
            .append(offset)
            .append("))");
        return sqlBuilder.getSql();
    }

    private String generateIsValidQuery(String sql)
    {
        final SqlBuilder sqlBuilder = createSqlBuilder();
        sqlBuilder.append("VALUES(SYS_BOOT.MGMT.IS_VALID_SQL(")
            .literal(sql.replaceAll("\n", " "))
            .append("))");
        return sqlBuilder.getSql();
    }

    private String generateCompletionQuery(
        String sql,
        int offset)
    {
        final SqlBuilder sqlBuilder = createSqlBuilder();
        sqlBuilder.append("SELECT * FROM TABLE(SYS_BOOT.MGMT.COMPLETE_SQL(")
        .literal(sql.replaceAll("\n", " "))
        .append(",")
        .append(offset)
        .append("))");
        return sqlBuilder.getSql();
    }

    private String generateSetSchema(String schemaName)
    {
        final SqlBuilder sqlBuilder = createSqlBuilder();
        final String schemaIdentifier =
            sqlBuilder.identifier(schemaName).getSqlAndClear();
        sqlBuilder.append("SET SCHEMA ").literal(schemaIdentifier);
        return sqlBuilder.getSql();
    }

    /**
     * Converts an array of SQL &quot;chunks&quot; (as created by StringChunker)
     * into a VALUES clause appropriate for insertion into a UDX.
     * @param chunks Array of String chunks
     * @return SqlString containing a VALUES clause where each input chunk
     * is a value for a single-column row
     */
    private SqlString createChunkValues(String[] chunks)
    {
        final SqlBuilder sqlBuilder = createSqlBuilder();
        sqlBuilder.append("VALUES(");
        boolean continuing = false;
        for (String chunk : chunks) {
            if (continuing) {
                sqlBuilder.append(",");
            }
            sqlBuilder.append("(").literal(chunk).append(")");
            continuing = true;
        }
        sqlBuilder.append(")");
        return sqlBuilder.toSqlString();
    }

    private String generateFormatQuery(String sql, SqlFormatOptions options)
    {
        if (options == null) {  // if unspecified, use default formatting
            options = new SqlFormatOptions();
        }
        final SqlBuilder sqlBuilder = createSqlBuilder();
        SqlString sqlValues =
            createChunkValues(StringChunker.slice(sql.replaceAll("\n", " ")));
        sqlBuilder
            .append("SELECT * FROM TABLE(SYS_BOOT.MGMT.FORMAT_SQL(")
            .append("CURSOR(SELECT * FROM ( ")
            .append(sqlValues)
            .append(")),")
            .append(Boolean.toString(options.isAlwaysUseParentheses()))
            .append(",")
            .append(Boolean.toString(options.isCaseClausesOnNewLines()))
            .append(",")
            .append(Boolean.toString(options.isClauseStartsLine()))
            .append(",")
            .append(Boolean.toString(options.isKeywordsLowercase()))
            .append(",")
            .append(Boolean.toString(options.isQuoteAllIdentifiers()))
            .append(",")
            .append(
                Boolean.toString(options.isSelectListItemsOnSeparateLines()))
            .append(",")
            .append(Boolean.toString(options.isWhereListItemsOnSeparateLines()))
            .append(",")
            .append(Boolean.toString(options.isWindowDeclarationStartsLine()))
            .append(",")
            .append(
                Boolean.toString(options.isWindowListItemsOnSeparateLines()))
            .append(",")
            .append(options.getIndentation())
            .append(",")
            .append(options.getLineLength())
            .append("))");
        return sqlBuilder.getSql();
    }

    /**
     * Determines whether the server considers a given SQL statement to be
     * valid, and if not, why not.
     * @param sql String containing the SQL statement to validate
     * @param defaultSchema String indicating the default schema for scoping
     * @return List of ValidateErrorInfo objects indicating locations and
     * reasons why the statement is invalid. If the list is empty, the input
     * statement is valid.
     */
    public List<ValidateErrorInfo> validate(String sql, String defaultSchema)
    {
        List<ValidateErrorInfo> result =
            new ArrayList<ValidateErrorInfo>();
        Connection c = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            c = dataSource.getConnection();
            stmt = c.createStatement();
            if (!Util.isNullOrEmpty(defaultSchema)) {
                stmt.executeUpdate(generateSetSchema(defaultSchema));
            }
            rs = stmt.executeQuery(generateValidationQuery(sql));
            while (rs.next()) {
                result.add(
                    new ValidateErrorInfo(
                        rs.getInt(1),
                        rs.getInt(2),
                        rs.getInt(3),
                        rs.getInt(4),
                        rs.getString(5)));
            }
            rs.close();
            rs = null;
            stmt.close();
            stmt = null;
            c.close();
            c = null;
        } catch (SQLException e) {
            tracer.severe(
                "Error validating SQL statement '" + sql + "': "
                + e.getMessage());
            tracer.severe("Stack trace:\n" + Util.getStackTrace(e));
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                }
                rs = null;
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                }
                stmt = null;
            }
            if (c != null) {
                try {
                    c.close();
                } catch (SQLException e) {
                }
                c = null;
            }
        }
        return (result.isEmpty()) ? null : result;
    }

    /**
     * Finds the SQL identifier at the indicated offset in a SQL statement, and
     * returns the fully-qualified name for the object that identifier
     * represents (that is, catalog.schema.object).
     * @param sql String containing a syntactically correct SQL statement
     * @param offset 0-based index into the SQL statement, indicating the
     * location of a SQL identifier we want the path for
     * @param defaultSchema String containing default schema name
     * @return String containing the fully-qualified name of the object, or an
     * empty string if either no identifier is found or the statement is not
     * valid.
     */
    public String getQualifiedName(String sql, int offset, String defaultSchema)
    {
        Connection c = null;
        Statement stmt = null;
        ResultSet rs = null;
        String result = "";
        try {
            c = dataSource.getConnection();
            stmt = c.createStatement();
            if (!Util.isNullOrEmpty(defaultSchema)) {
                stmt.execute(generateSetSchema(defaultSchema));
            }
            rs = stmt.executeQuery(generateQualifierQuery(sql, offset));
            rs.next();
            result = rs.getString(1);
            rs.close();
            rs = null;
            stmt.close();
            stmt = null;
            c.close();
            c = null;
        } catch (SQLException e) {
            tracer.warning("Error qualifying SQL name. \nException was" + e);
            tracer.warning("Stack trace:\n" + Util.getStackTrace(e));
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                rs = null;
                if (stmt != null) {
                    stmt.close();
                }
                if (c != null) {
                    c.close();
                }
            } catch (SQLException e) {
            } finally {
                stmt = null;
                c = null;
            }
        }
        return result;
    }

    public boolean isValid(String sql)
    {
        Connection c = null;
        Statement stmt = null;
        ResultSet rs = null;
        boolean result = false;
        try {
            c = dataSource.getConnection();
            stmt = c.createStatement();
            rs = stmt.executeQuery(generateIsValidQuery(sql));
            rs.next();
            result = rs.getBoolean(1);
            rs.close();
            rs = null;
            stmt.close();
            stmt = null;
            c.close();
            c = null;
        } catch (SQLException e) {
            tracer.warning("Error checking SQL statement. \nException was" + e);
            tracer.warning("Stack trace:\n" + Util.getStackTrace(e));
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                rs = null;
                if (stmt != null) {
                    stmt.close();
                }
                if (c != null) {
                    c.close();
                }
            } catch (SQLException e) {
            } finally {
                stmt = null;
                c = null;
            }
        }
        return result;
    }

    /**
     * Gets all the SQL reserved words and tokens defined as key words by the
     * server.
     * @return List of Strings containing all the reserved and key words known
     * to the server, in no particular order
     */
    public List<String> getReservedAndKeyWords()
    {
        Connection c = null;
        Statement stmt = null;
        ResultSet rs = null;
        List<String> result = new ArrayList<String>();
        try {
            c = dataSource.getConnection();
            stmt = c.createStatement();
            rs = stmt.executeQuery(keywordQuery);
            while (rs.next()) {
                result.add(rs.getString(1));
            }
            rs.close();
            rs = null;
            stmt.close();
            stmt = null;
            c.close();
            c = null;
        } catch (SQLException e) {
            tracer.warning("Error getting SQL keywords. \nException was" + e);
            tracer.warning("Stack trace:\n" + Util.getStackTrace(e));
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                rs = null;
                if (stmt != null) {
                    stmt.close();
                }
                if (c != null) {
                    c.close();
                }
            } catch (SQLException e) {
            } finally {
                stmt = null;
                c = null;
            }
        }
        return result;
    }

    /**
     * Generate a list of SQL tokens that are valid at the indicated point in
     * a partial SQL statement.
     * @param sql String containing partial SQL to generate hints for
     * @param offset Location in the SQL statement where we want hints
     * @param replacedWords String[] to receive list of replaced words
     * @param defaultSchema Default schema for context
     * @return List of SqlItem objects representing valid hints for the given
     * location
     */
    public List<SqlItem> getCompletionHints(
        String sql,
        int offset,
        String[] replacedWords,
        String defaultSchema)
    {
        List<SqlItem> result = new ArrayList<SqlItem>();
        String itemType;
        String itemName;
        Connection c = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            c = dataSource.getConnection();
            stmt = c.createStatement();
            // if we have a default schema, issue SET SCHEMA
            if (!Util.isNullOrEmpty(defaultSchema)) {
                stmt.executeUpdate(generateSetSchema(defaultSchema));
            }
            rs = stmt.executeQuery(
                generateCompletionQuery(sql, offset));
            while (rs.next()) {
                itemType = rs.getString(1);
                itemName = rs.getString(2);
                if (itemType.equals("REPLACED")) {
                    replacedWords[0] = itemName;
                } else {
                    result.add(new SqlItem(itemType, itemName));
                }
            }
            rs.close();
            rs = null;
            stmt.close();
            stmt = null;
            c.close();
            c = null;
        } catch (SQLException se) {
            tracer.severe(
                "Error completing SQL statement '" + sql + "': "
                + se.getMessage());
            tracer.severe("Stack trace:\n" + Util.getStackTrace(se));
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                rs = null;
                if (stmt != null) {
                    stmt.close();
                }
                if (c != null) {
                    c.close();
                }
            } catch (SQLException e) {
            } finally {
                stmt = null;
                c = null;
            }
        }
        return result;
    }

    /**
     * Formats the input SQL statement in accordance with a set of options.
     * @param sql String containing the SQL statement to format
     * @param formatOptions SqlFormatOptions structure containing guidelines for
     * how to formate the code
     * @return String containing the code as reformatted
     */
    public String format(String sql, SqlFormatOptions formatOptions)
    {
        String result = sql;
        Connection c = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            c = dataSource.getConnection();
            stmt = c.createStatement();
            rs = stmt.executeQuery(generateFormatQuery(sql, formatOptions));
            result = StringChunker.readChunks(rs, 2);
            rs.close();
            rs = null;
            stmt.close();
            stmt = null;
            c.close();
            c = null;
        } catch (SQLException e) {
            tracer.severe(
                "Error formatting SQL statement '" + sql + "': "
                + e.getMessage());
            tracer.severe("Stack trace:\n" + Util.getStackTrace(e));
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                rs = null;
                if (stmt != null) {
                    stmt.close();
                }
                if (c != null) {
                    c.close();
                }
            } catch (SQLException e) {
            } finally {
                stmt = null;
                c = null;
            }
        }
        return result;
    }

    /**
     * Data structure used to relay information about an error in SQL
     * validation: where it starts and ends (line and column), plus a string
     * describing the error. Clients can then present this information.
     */
    public class ValidateErrorInfo
    {
        private int startLineNum;
        private int startColumnNum;
        private int endLineNum;
        private int endColumnNum;
        private String errorMsg;

        /**
         * Creates a new ValidateErrorInfo with the position coordinates and an
         * error string.
         *
         * @param startLineNum Start line number
         * @param startColumnNum Start column number
         * @param endLineNum End line number
         * @param endColumnNum End column number
         * @param errorMsg Error message
         */
        public ValidateErrorInfo(
            int startLineNum,
            int startColumnNum,
            int endLineNum,
            int endColumnNum,
            String errorMsg)
        {
            this.startLineNum = startLineNum;
            this.startColumnNum = startColumnNum;
            this.endLineNum = endLineNum;
            this.endColumnNum = endColumnNum;
            this.errorMsg = errorMsg;
        }

        /**
         * @return 1-based starting line number
         */
        public int getStartLineNum()
        {
            return startLineNum;
        }

        /**
         * @return 1-based starting column number
         */
        public int getStartColumnNum()
        {
            return startColumnNum;
        }

        /**
         * @return 1-based end line number
         */
        public int getEndLineNum()
        {
            return endLineNum;
        }

        /**
         * @return 1-based end column number
         */
        public int getEndColumnNum()
        {
            return endColumnNum;
        }

        /**
         * @return error message
         */
        public String getMessage()
        {
            return errorMsg;
        }
     }

    /**
     * Data structure used to express an item (such as a keyword or identifier)
     * within a SQL statement, comprising the item's name and type. This isn't
     * terribly elegant, but it allows the service to return the name and type
     * as a pair, thus preventing additional server calls.
     *
     * @author chard
     */
    public class SqlItem
    {
        String itemType;
        String itemName;

        public SqlItem(String itemType, String itemName)
        {
            this.itemType = itemType;
            this.itemName = itemName;
        }

        public String getItemType()
        {
            return itemType;
        }

        public String getItemName()
        {
            return itemName;
        }

        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append(itemType).append("(").append(itemName).append(")");
            return sb.toString();
        }
    }
}

// End FarragoSqlAdvisorService.java
