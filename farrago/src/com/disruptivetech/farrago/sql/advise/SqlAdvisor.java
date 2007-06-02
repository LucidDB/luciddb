/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2002-2007 Disruptive Tech
// Copyright (C) 2005-2007 The Eigenbase Project
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
package com.disruptivetech.farrago.sql.advise;

import java.io.*;

import java.util.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;


/**
 * An assistant which offers hints and corrections to a partially-formed SQL
 * statement. It is used in the SQL editor user-interface.
 *
 * @author tleung
 * @version $Id$
 * @since Jan 16, 2004
 */
public class SqlAdvisor
{
    //~ Instance fields --------------------------------------------------------

    // Flags indicating precision/scale combinations
    private final SqlValidatorWithHints validator;
    private final String hintToken = "_suggest_";

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a SqlAdvisor with a validator instance
     */
    public SqlAdvisor(
        SqlValidatorWithHints validator)
    {
        this.validator = validator;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Gets completion hints for a partially completed or syntatically incorrect
     * sql statement with cursor pointing to the position where completion hints
     * are requested.
     *
     * @param sql A partial or syntatically incorrect sql statement for which to
     * retrieve completion hints
     * @param cursor to indicate the 0-based cursor position in the query at
     * which completion hints need to be retrieved.
     */
    public SqlMoniker [] getCompletionHints(String sql, int cursor)
        throws SqlParseException
    {
        String simpleSql = simplifySql(sql, cursor);
        int idx = simpleSql.indexOf(hintToken);
        if (idx < 0) {
            SqlMoniker [] emptyMonikers = new SqlMoniker[0];
            return emptyMonikers;
        }
        int idxAdj = adjustTokenPosition(simpleSql, idx);
        if (idxAdj >= 0) {
            idx = idxAdj;
        }
        SqlParserPos pos = new SqlParserPos(1, idx + 1);
        return getCompletionHints(simpleSql, pos);
    }

    /**
     * Gets completion hints for a syntatically correct sql statement with dummy
     * SqlIdentifier
     *
     * @param sql A syntatically correct sql statement for which to retrieve
     * completion hints
     * @param pos to indicate the line and column position in the query at which
     * completion hints need to be retrieved. For example, "select a.ename,
     * b.deptno from sales.emp a join sales.dept b "on a.deptno=b.deptno where
     * empno=1"; setting pos to 'Line 1, Column 17' returns all the possible
     * column names that can be selected from sales.dept table setting pos to
     * 'Line 1, Column 31' returns all the possible table names in 'sales'
     * schema
     *
     * @return an array of hints ({@link SqlMoniker}) that can fill in at the
     * indicated position
     */
    public SqlMoniker [] getCompletionHints(String sql, SqlParserPos pos)
        throws SqlParseException
    {
        SqlNode sqlNode = parseQuery(sql);
        try {
            validator.validate(sqlNode);
        } catch (Exception e) {
            // mask any exception that is thrown during the validation, i.e. try
            // to continue even if the sql is invalid. we are doing a best
            // effort here to try to come up with the requested completion hints
        }
        return validator.lookupHints(sqlNode, pos);
    }

    /**
     * Gets the fully qualified name for a {@link SqlIdentifier} at a given
     * position of a sql statement
     *
     * @param sql A syntatically correct sql statement for which to retrieve a
     * fully qualified SQL identifier name
     * @param cursor to indicate the 0-based cursor position in the query that
     * represents a SQL identifier for which its fully qualified name is to be
     * returned.
     *
     * @return a {@link SqlMoniker} that contains the fully qualified name of
     * the specified SQL identifier, returns null if none is found or the SQL
     * statement is invalid.
     */
    public SqlMoniker getQualifiedName(String sql, int cursor)
    {
        SqlNode sqlNode = null;
        try {
            sqlNode = parseQuery(sql);
            validator.validate(sqlNode);
        } catch (Exception e) {
            return null;
        }
        SqlParserPos pos = new SqlParserPos(1, cursor + 1);
        try {
            return validator.lookupQualifiedName(sqlNode, pos);
        } catch (EigenbaseContextException e) {
            return null;
        } catch (java.lang.AssertionError e) {
            return null;
        }
    }

    /**
     * Attempts to complete and validate a given partially completed sql
     * statement. return whether it's valid.
     *
     * @param sql A partial or syntatically incorrect sql statement to validate
     */
    public boolean isValid(String sql)
    {
        SqlSimpleParser simpleParser = new SqlSimpleParser(hintToken);
        String simpleSql = simpleParser.simplifySql(sql);
        SqlNode sqlNode = null;
        try {
            sqlNode = parseQuery(simpleSql);
        } catch (Exception e) {
            // if the sql can't be parsed we wont' be able to validate it
            return false;
        }
        try {
            validator.validate(sqlNode);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    /**
     * Attempts to parse and validate a SQL statement. Throws the first
     * exception encountered. The error message of this exception is to be
     * displayed on the UI
     *
     * @param sql A user-input sql statement to be validated
     *
     * @return a List of ValidateErrorInfo (null if sql is valid)
     */
    public List<ValidateErrorInfo> validate(String sql)
    {
        SqlNode sqlNode = null;
        List<ValidateErrorInfo> errorList = new ArrayList<ValidateErrorInfo>();

        sqlNode = collectParserError(sql, errorList);
        if (!errorList.isEmpty()) {
            return errorList;
        }
        try {
            validator.validate(sqlNode);
        } catch (EigenbaseContextException e) {
            ValidateErrorInfo errInfo = new ValidateErrorInfo(e);

            // validator only returns 1 exception now
            errorList.add(errInfo);
            return errorList;
        } catch (Exception e) {
            ValidateErrorInfo errInfo =
                new ValidateErrorInfo(
                    1,
                    1,
                    1,
                    sql.length(),
                    e.getMessage());

            // parser only returns 1 exception now
            errorList.add(errInfo);
            return errorList;
        }
        return null;
    }

    /**
     * Turns a partially completed or syntatically incorrect sql statement into
     * a simplified, valid one that can be passed into getCompletionHints()
     *
     * @param sql A partial or syntatically incorrect sql statement
     * @param cursor to indicate column position in the query at which
     * completion hints need to be retrieved.
     *
     * @return a completed, valid (and possibly simplified SQL statement
     */
    public String simplifySql(String sql, int cursor)
    {
        SqlSimpleParser parser = new SqlSimpleParser(hintToken);
        return parser.simplifySql(sql, cursor);
    }

    /**
     * Return an array of SQL reserved and keywords
     *
     * @return an of SQL reserved and keywords
     */
    public String [] getReservedAndKeyWords()
    {
        Collection<String> c = SqlAbstractParserImpl.getSql92ReservedWords();
        List<String> l =
            Arrays.asList(
                getParserImpl().getMetadata().getJdbcKeywords().split(","));
        List<String> al = new ArrayList<String>();
        al.addAll(c);
        al.addAll(l);
        return al.toArray(new String[al.size()]);
    }

    /**
     * Return the underlying Parser implementation class
     *
     * @return a {@link SqlAbstractParserImpl} instance
     */
    protected SqlAbstractParserImpl getParserImpl()
    {
        SqlParser parser = new SqlParser(new StringReader(""));
        return parser.getParserImpl();
    }

    /**
     * Wrapper function to parse a SQL statement, throwing a {@link
     * SqlParseException} if the statement is not syntactically valid
     */
    protected SqlNode parseQuery(String sql)
        throws SqlParseException
    {
        SqlParser parser = new SqlParser(sql);
        return parser.parseQuery();
    }

    /**
     * Attempts to parse a SQL statement and adds to the errorList if any syntax
     * error is found. This implementation uses {@link SqlParser}. Subclass can
     * re-implement this with a different parser implementation
     *
     * @param sql A user-input sql statement to be parsed
     * @param errorList A {@link List} of error to be added to
     *
     * @return {@link SqlNode } that is root of the parse tree, null if the sql
     * is not valid
     */
    protected SqlNode collectParserError(
        String sql,
        List<ValidateErrorInfo> errorList)
    {
        try {
            SqlNode sqlNode = parseQuery(sql);
            return sqlNode;
        } catch (SqlParseException e) {
            ValidateErrorInfo errInfo =
                new ValidateErrorInfo(
                    e.getPos(),
                    e.getMessage());

            // parser only returns 1 exception now
            errorList.add(errInfo);
            return null;
        }
    }

    /**
     * simplifySql takes a 0-based cursor which points to exactly where
     * completion hint is to be requested. getCompletionHints takes a 1-based
     * SqlParserPos which points to the beginning of a SqlIdentifier. For
     * example, the caret in 'where b.^' indicates the cursor position needed
     * for simplifySql, while getCompletionHints will require the same clause to
     * be represented as 'where ^b.$suggest$'
     */
    private int adjustTokenPosition(String sql, int cursor)
    {
        if (sql.charAt(cursor - 1) == '.') {
            int idxLastSpace = sql.lastIndexOf(' ', cursor - 1);
            int idxLastEqual = sql.lastIndexOf('=', cursor - 1);
            return (idxLastSpace < idxLastEqual) ? (idxLastEqual + 1)
                : (idxLastSpace + 1);
        } else {
            return -1;
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * An inner class that represents error message text and position info of a
     * validator or parser exception
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
         * error string
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
         * Creates a new ValidateErrorInfo with an EigenbaseContextException
         */
        public ValidateErrorInfo(
            EigenbaseContextException e)
        {
            this.startLineNum = e.getPosLine();
            this.startColumnNum = e.getPosColumn();
            this.endLineNum = e.getEndPosLine();
            this.endColumnNum = e.getEndPosColumn();
            this.errorMsg = e.getCause().getMessage();
        }

        /**
         * Creates a new ValidateErrorInfo with a SqlParserPos and an error
         * string
         */
        public ValidateErrorInfo(
            SqlParserPos pos,
            String errorMsg)
        {
            this.startLineNum = pos.getLineNum();
            this.startColumnNum = pos.getColumnNum();
            this.endLineNum = pos.getEndLineNum();
            this.endColumnNum = pos.getEndColumnNum();
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
}

// End SqlAdvisor.java
