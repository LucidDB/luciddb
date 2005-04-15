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
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.validate.SqlValidatorImpl;
import org.eigenbase.sql.parser.SqlParseException;
import org.eigenbase.sql.parser.SqlParser;
import org.eigenbase.sql.parser.SqlParserPos;

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
    private final SqlValidator validator;
    private final String hintToken = "_suggest_";

    //~ Constructors ----------------------------------------------------------
    /**
     * Creates a SqlAdvisor with a validator instance
     */
    public SqlAdvisor(
        SqlValidator validator)
    {
        this.validator = validator;
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Get completion hints for a partially completed or syntatically incorrect      * sql statement with cursor pointing to the position where completion
     * hints are requested
     *
     * @param sql A partial or syntatically incorrect sql statement for which
     * to retrieve completion hints
     *
     * @param cursor to indicate the 0-based cursor position in the query at
     * which completion hints need to be retrieved.
     */
    public String[] getCompletionHints(String sql, int cursor)
        throws SqlParseException
    {
        String simpleSql = simplifySql(sql, cursor);
        int idx = simpleSql.indexOf(hintToken);
        int idxAdj = adjustTokenPosition(simpleSql, idx);
        if (idxAdj >=0 ) {
            idx = idxAdj;
        }
        SqlParserPos pp = new SqlParserPos(1, idx+1);
        return getCompletionHints(simpleSql, pp);
    }

    /**
     * Get completion hints for a syntatically correct sql statement with
     * dummy SqlIdentifier
     *
     * @param sql A syntatically correct sql statement for which to retrieve
     * completion hints
     * @param pp to indicate the line and column position in the query at which
     * completion hints need to be retrieved.  For example,
     * "select a.ename, b.deptno from sales.emp a join sales.dept b
     * "on a.deptno=b.deptno where empno=1";
     * setting pp to 'Line 1, Column 17' returns all the possible column names
     * that can be selected from sales.dept table
     * setting pp to 'Line 1, Column 31' returns all the possible table names
     * in 'sales' schema
     *
     * @return an array of string hints (sql identifiers) that can fill in at
     * the indicated position
     *
     */
    public String[] getCompletionHints(String sql, SqlParserPos pp)
        throws SqlParseException
    {
        SqlParser parser = new SqlParser(sql);
        SqlNode sqlNode = parser.parseQuery();
        try {
            validator.validate(sqlNode);
        } catch (Exception e) {
            // mask any exception that is thrown during the validation, i.e.
            // try to continue even if the sql is invalid.
            // we are doing a best effort here to try to come up with the
            // requested completion hints
        }
        // XXX new interface?
        return ((SqlValidatorImpl) validator).lookupHints(sqlNode, pp);
    }

    /**
     * Attempt to complete and validate a given partially completed 
     * sql statement.  return whether it's valid.  
     *
     * @param sql A partial or syntatically incorrect sql statement to validate
     */
    public boolean isValid(String sql)
    {
        SqlSimpleParser simpleParser = new SqlSimpleParser(hintToken);
        String simpleSql = simpleParser.simplifySql(sql);
        SqlParser parser = new SqlParser(simpleSql);
        SqlNode sqlNode = null;
        try {
            sqlNode = parser.parseQuery();
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
     * Turn a partially completed or syntatically incorrect sql statement into
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
     * Parser does not like the '$suggest$' token used in SqlSimpleParser.
     * Convert it to a 'dummy' token for Parser and Validator. The validator
     * would not really try to interpret this token in context.
     *
     * @param sql The sql containing '$suggest$' token
     *
     * @return a new sql with $suggest$ token replaced by dummy
     *
     */
    private String prepareSqlForParser(String sql)
    {
        return sql.replaceAll("\\$suggest\\$", "dummy");
    }
}


// End SqlAdvisor.java
