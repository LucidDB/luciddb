/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
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

import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.SqlIdentifier;
import org.eigenbase.sql.validate.SqlMoniker;
import org.eigenbase.sql.validate.SqlValidatorWithHints;
import org.eigenbase.sql.parser.SqlParseException;
import org.eigenbase.sql.parser.SqlParser;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.util.EigenbaseContextException;

import java.util.List;
import java.util.ArrayList;

/**
 * An assistant which offers hints and corrections to a partially-formed SQL
 * statement.  It is used in the SQL editor user-interface.
 *
 * @author tleung
 * @since Jan 16, 2004
 * @version $Id$
 **/
public class SqlAdvisor
{
    //~ Static fields/initializers --------------------------------------------

    // Flags indicating precision/scale combinations

    //~ Instance fields -------------------------------------------------------
    private final SqlValidatorWithHints validator;
    private final String hintToken = "_suggest_";

    //~ Constructors ----------------------------------------------------------
    /**
     * Creates a SqlAdvisor with a validator instance
     */
    public SqlAdvisor(
        SqlValidatorWithHints validator)
    {
        this.validator = validator;
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Gets completion hints for a partially completed or syntatically incorrect      * sql statement with cursor pointing to the position where completion
     * hints are requested
     *
     * @param sql A partial or syntatically incorrect sql statement for which
     * to retrieve completion hints
     *
     * @param cursor to indicate the 0-based cursor position in the query at
     * which completion hints need to be retrieved.
     */
    public SqlMoniker[] getCompletionHints(String sql, int cursor)
        throws SqlParseException
    {
        String simpleSql = simplifySql(sql, cursor);
        int idx = simpleSql.indexOf(hintToken);
        int idxAdj = adjustTokenPosition(simpleSql, idx);
        if (idxAdj >=0 ) {
            idx = idxAdj;
        }
        SqlParserPos pos = new SqlParserPos(1, idx+1);
        return getCompletionHints(simpleSql, pos);
    }

    /**
     * Gets completion hints for a syntatically correct sql statement with
     * dummy SqlIdentifier
     *
     * @param sql A syntatically correct sql statement for which to retrieve
     * completion hints
     * @param pos to indicate the line and column position in the query at which
     * completion hints need to be retrieved.  For example,
     * "select a.ename, b.deptno from sales.emp a join sales.dept b
     * "on a.deptno=b.deptno where empno=1";
     * setting pos to 'Line 1, Column 17' returns all the possible column names
     * that can be selected from sales.dept table
     * setting pos to 'Line 1, Column 31' returns all the possible table names
     * in 'sales' schema
     *
     * @return an array of hints ({@link SqlMoniker}) that can fill in at
     * the indicated position
     *
     */
    public SqlMoniker[] getCompletionHints(String sql, SqlParserPos pos)
        throws SqlParseException
    {
        SqlNode sqlNode = parseQuery(sql);
        try {
            validator.validate(sqlNode);
        } catch (Exception e) {
            // mask any exception that is thrown during the validation, i.e.
            // try to continue even if the sql is invalid.
            // we are doing a best effort here to try to come up with the
            // requested completion hints
        }
        return validator.lookupHints(sqlNode, pos);
    }

    /**
     * Gets the fully qualified name for a {@link SqlIdentifier} at a given
     * position of a sql statement
     *
     * @param sql A syntatically correct sql statement for which to retrieve
     * a fully qualified SQL identifier name
     * @param cursor to indicate the 0-based cursor position in the query
     * that represents a SQL identifier for which its fully qualified name is
     * to be returned.
     *
     * @return a {@link SqlMoniker} that contains the fully qualified name of the
     * specified SQL identifier, returns null if none is found or the SQL
     * statement is invalid.
     *
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
        SqlParserPos pos = new SqlParserPos(1, cursor+1);
        return validator.lookupQualifiedName(sqlNode, pos);
    }

    /**
     * Attempts to complete and validate a given partially completed
     * sql statement.  return whether it's valid.
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
     * Attempts to parse and validate a SQL statement.  Throws the first
     * exception encountered.  The error message of this exception is to be
     * displayed on the UI
     *
     * @param sql A user-input sql statement to be validated
     *
     * @return a List of ValidateErrorInfo (null if sql is valid)
     */
    public List validate(String sql)
    {
        SqlNode sqlNode = null;
        try {
            sqlNode = parseQuery(sql);
        } catch (SqlParseException e) {
            // parser error does not contain a range info yet.  we set
            // the error to entire line for now
            ValidateErrorInfo errInfo =
                new ValidateErrorInfo(
                    e.getPos(),
                    e.getMessage());

            // parser only returns 1 exception now
            ArrayList errorList = new ArrayList();
            errorList.add(errInfo);
            return errorList;
        }
        try {
            validator.validate(sqlNode);
        } catch (EigenbaseContextException e) {
            ValidateErrorInfo errInfo =
                new ValidateErrorInfo(e);

            // validator only returns 1 exception now
            ArrayList errorList = new ArrayList();
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
            ArrayList errorList = new ArrayList();
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
     *
     */
    public String simplifySql(String sql, int cursor)
    {
        SqlSimpleParser parser = new SqlSimpleParser(hintToken);
        return parser.simplifySql(sql, cursor);
    }

    /**
     * simplifySql takes a 0-based cursor which points to exactly where
     * completion hint is to be requested.
     *
     * getCompletionHints takes a 1-based SqlParserPos which points to the
     * beginning of a SqlIdentifier.
     *
     * For example, the caret in 'where b.^' indicates the cursor position
     * needed for simplifySql, while getCompletionHints will require the
     * same clause to be represented as 'where ^b.$suggest$'
     *
     */
    private int adjustTokenPosition(String sql, int cursor)
    {
        if (sql.charAt(cursor-1) == '.') {
            int idxLastSpace = sql.lastIndexOf(' ', cursor-1);
            int idxLastEqual = sql.lastIndexOf('=', cursor-1);
            return idxLastSpace < idxLastEqual ?
                idxLastEqual+1 : idxLastSpace+1;
        } else {
            return -1;
        }
    }
    
    /**
     * Wrapper function to parse a SQL statement, throwing a 
     * {@link SqlParseException} if the statement is not syntactically valid
     */
    protected SqlNode parseQuery(String sql) throws SqlParseException
    {
        SqlParser parser = new SqlParser(sql);
        SqlNode sqlNode = null;
        return parser.parseQuery();
    }

    /**
     *  An inner class that represents error message text and position info
     *  of a validator or parser exception
     */
    public class ValidateErrorInfo {

        private int startLineNum;
        private int startColumnNum;
        private int endLineNum;
        private int endColumnNum;
        private String errorMsg;

        /**
        *  Creates a new ValidateErrorInfo with the position coordinates
        *  and an error string
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
        *  Creates a new ValidateErrorInfo with an EigenbaseException
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
        *  Creates a new ValidateErrorInfo with a SqlParserPos
        *  and an error string
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
         *  @return 1-based starting line number
         */
        public int getStartLineNum()
        {
            return startLineNum;
        }

        /**
         *  @return 1-based starting column number
         */
        public int getStartColumnNum()
        {
            return startColumnNum;
        }

        /**
         *  @return 1-based end line number
         */
        public int getEndLineNum()
        {
            return endLineNum;
        }

        /**
         *  @return 1-based end column number
         */
        public int getEndColumnNum()
        {
            return endColumnNum;
        }

        /**
         *  @return error message
         */
        public String getMessage()
        {
            return errorMsg;
        }
    }
}


// End SqlAdvisor.java
