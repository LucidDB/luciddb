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
package net.sf.farrago.syslib;

import java.sql.*;
import java.util.*;
import java.util.logging.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.session.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.advise.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.pretty.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;

/**
 * UDX wrapper for functions that return information about the system as a
 * whole, such as keywords, or information that only the server can provide,
 * such as SQL validation or hints.
 *
 * @author chard
 */
public abstract class FarragoSqlAdvisorUDR
{

    /**
     * Gets the list of SQL reserved words from the parser, appends to it the
     * list of key words from the server's meta data, and inserts the combined
     * list into the supplied output sink.
     * @param resultInserter PreparedStatement to receive the word list, must
     * have a single VARCHAR column
     * @throws SQLException if any database error occurs
     */
    public static void getReservedAndKeyWords(PreparedStatement resultInserter)
    throws SQLException
    {
        // Gather the reserved words
        Collection<String> c = SqlAbstractParserImpl.getSql92ReservedWords();
        // Ask the parser for key words
        List<String> sqlKeywords = Arrays.asList(
            FarragoUdrRuntime.getSession().getDatabaseMetaData()
                .getSQLKeywords().split(","));
        // Combine the lists
        List<String> l = new ArrayList<String>(c);
        l.addAll(sqlKeywords);
        // Send them off to the output table
        for (String s : l) {
            resultInserter.setString(1, s);
            resultInserter.executeUpdate();
        }
    }

    /**
     * Validates the given SQL statement and inserts information from the errors
     * (if any) into the prepared statement for the UDX.
     * @param sql String containing SQL to validate
     * @param resultInserter PreparedStatement that will pass the validation
     * errors back through the UDX
     * @throws SQLException if an error occurs
     */
    public static void validate(String sql, PreparedStatement resultInserter)
    throws SQLException
    {
        FarragoSession session = FarragoUdrRuntime.getSession();
        FarragoReposTxnContext txn =
            new FarragoReposTxnContext(session.getRepos(), true);
        txn.beginReadTxn();
        FarragoSessionStmtValidator stmtValidator = null;
        try {
            stmtValidator = session.newStmtValidator();
            FarragoSessionPreparingStmt preparingStmt =
                session.getPersonality().newPreparingStmt(null, stmtValidator);
            SqlAdvisor advisor = preparingStmt.getAdvisor();
            List<SqlAdvisor.ValidateErrorInfo> errors = advisor.validate(sql);
            if (errors == null) {
                return;
            }
            for (SqlAdvisor.ValidateErrorInfo error : errors) {
                resultInserter.setInt(1, error.getStartLineNum());
                resultInserter.setInt(2, error.getStartColumnNum());
                resultInserter.setInt(3, error.getEndLineNum());
                resultInserter.setInt(4, error.getEndColumnNum());
                resultInserter.setString(5, error.getMessage());
                resultInserter.executeUpdate();
            }
        } finally {
            txn.commit();
            stmtValidator.closeAllocation();
        }
    }

    public static boolean isValid(String sql)
    {
        boolean result = false;
        FarragoSession session = FarragoUdrRuntime.getSession();
        FarragoReposTxnContext txn =
            new FarragoReposTxnContext(session.getRepos(), true);
        txn.beginReadTxn();
        FarragoSessionStmtValidator stmtValidator = null;
        try {
            stmtValidator = session.newStmtValidator();
            FarragoSessionPreparingStmt preparingStmt =
                session.getPersonality().newPreparingStmt(null, stmtValidator);
            SqlAdvisor advisor = preparingStmt.getAdvisor();
            result =
                advisor.isValid(sql);
        } finally {
            txn.commit();
            stmtValidator.closeAllocation();
        }
        return result;
    }

    /**
     * Generates completion hints for the given partial SQL statement.
     * @param sql String containing a partial SQL statement
     * @param offset index into the SQL statement where hints are wanted
     * @param resultInserter PreparedStatement to receive hints
     * @throws SQLException if an error occurs
     */
    public static void complete(
        String sql,
        int offset,
        PreparedStatement resultInserter)
    throws SQLException
    {
        FarragoSession session = FarragoUdrRuntime.getSession();
        FarragoReposTxnContext txn =
            new FarragoReposTxnContext(session.getRepos(), true);
        txn.beginReadTxn();
        FarragoSessionStmtValidator stmtValidator = null;
        String[] replacedWords = {null};
        try {
            stmtValidator = session.newStmtValidator();
            FarragoSessionPreparingStmt preparingStmt =
                session.getPersonality().newPreparingStmt(null, stmtValidator);
            SqlAdvisor advisor = preparingStmt.getAdvisor();
            List<SqlMoniker> hints =
                advisor.getCompletionHints(sql, offset, replacedWords);
            for (SqlMoniker hint : hints) {
                resultInserter.setString(1, hint.getType().toString());
                resultInserter.setString(2, hint.toString());
                resultInserter.executeUpdate();
            }
            resultInserter.setString(1, "REPLACED");
            resultInserter.setString(2, replacedWords[0]);
            resultInserter.executeUpdate();
        } finally {
            txn.commit();
            stmtValidator.closeAllocation();
        }
    }

    /**
     * Formats a SQL statement based on the options supplied.
     * @param sqlChunks ResultSet containing a set of string &quot;chunks&quot;
     * that cumulatively represent the statement to format
     * @param alwaysUseParentheses boolean inicating whether to always use
     * parentheses, even when not strictly necessary
     * @param caseClausesOnNewLines boolean indicating whether to start each
     * CASE clause on a new line
     * @param clauseStartsLine boolean indicating whether to start each new
     * clause on a new line
     * @param keywordsLowercase boolean indicating whether to force all SQL
     * keywords to lowercase
     * @param quoteAllIdentifiers boolean idication whether to put quotes around
     * all identifiers in the statement
     * @param selectListItemsOnSeparateLines boolean indicating whether to put
     * each item in a SELECT list on a separate line
     * @param whereListItemsOnSeparateLines boolean indicating whether to put
     * each item in a WHERE list on a separate line
     * @param windowDeclarationStartsLine boolean indicating whether to put
     * WINDOW declarations at the start of a line
     * @param windowListItemsOnSeparateLines boolean indicating whether to put
     * each item in a WINDOW list on a separate line
     * @param indentation number of spaces to indent lines
     * @param lineLength maximum length of a line in the statement, used to
     * advise line wrapping
     * @param resultInserter PreparedStatement to receive chunks of the
     * formatted SQL statement
     * @throws SQLException if an error occurs
     */
    public static void format(
        ResultSet sqlChunks,
        boolean alwaysUseParentheses,
        boolean caseClausesOnNewLines,
        boolean clauseStartsLine,
        boolean keywordsLowercase,
        boolean quoteAllIdentifiers,
        boolean selectListItemsOnSeparateLines,
        boolean whereListItemsOnSeparateLines,
        boolean windowDeclarationStartsLine,
        boolean windowListItemsOnSeparateLines,
        int indentation,
        int lineLength,
        PreparedStatement resultInserter) throws SQLException
    {
        String output;
        // reassemble SQL from input chunks
        String sql = StringChunker.readChunks(sqlChunks);
        // figure out how they'd like it formtted
        SqlFormatOptions options = new SqlFormatOptions(
            alwaysUseParentheses,
            caseClausesOnNewLines,
            clauseStartsLine,
            keywordsLowercase,
            quoteAllIdentifiers,
            selectListItemsOnSeparateLines,
            whereListItemsOnSeparateLines,
            windowDeclarationStartsLine,
            windowListItemsOnSeparateLines,
            indentation,
            lineLength);
        final SqlPrettyWriter pw =
            new SqlPrettyWriter(SqlDialect.EIGENBASE);
        pw.setFormatOptions(options);
        FarragoSession session = FarragoUdrRuntime.getSession();
        FarragoReposTxnContext txn =
            new FarragoReposTxnContext(session.getRepos(), true);
        txn.beginReadTxn();
        FarragoSessionStmtValidator stmtValidator = null;
        try {
            stmtValidator = session.newStmtValidator();
            FarragoSessionParser parser =
                session.getPersonality().newParser(session);
            SqlNode node = (SqlNode) parser.parseSqlText(
                stmtValidator,
                null,
                sql,
                true);
            output = pw.format(node);
            int chunkIdx = 1;
            // split into chunks as necessary for output
            String[] chunks = StringChunker.slice(output);
            for (String chunk : chunks) {
                resultInserter.setInt(1, chunkIdx);
                resultInserter.setString(2, chunk);
                resultInserter.executeUpdate();
                chunkIdx++;
            }
        } finally {
            txn.commit();
            stmtValidator.closeAllocation();
        }
    }
}

// End FarragoSqlAdvisorUDR.java
