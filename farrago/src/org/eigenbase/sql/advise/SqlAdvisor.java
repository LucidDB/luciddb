/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

package org.eigenbase.sql.advise;

import java.util.List;
import org.eigenbase.sql.SqlValidator;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.parser.SqlParser;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.parser.SqlParseException;

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
    final SqlValidator validator;

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
        return validator.lookupHints(sqlNode, pp);
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
        SqlSimpleParser parser = new SqlSimpleParser();
        return parser.simplifySql(sql, cursor);
    }
}


// End SqlAdvisor.java
