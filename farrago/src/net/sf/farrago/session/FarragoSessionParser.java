/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
*/
package net.sf.farrago.session;

import net.sf.farrago.catalog.*;
import net.sf.farrago.util.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;


/**
 * FarragoSessionParser represents an object capable of parsing Farrago
 * SQL statements.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSessionParser
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Parses an SQL expression.  If a DDL statement, implicitly
     * performs uncommitted catalog updates.
     *
     * @param ddlValidator the validator to use for lookup during parsing
     * if this turns out to be a DDL statement
     *
     * @param sql the SQL text to be parsed
     *
     * @param expectStatement if true, expect a statement; if false, 
     * expect a row-expression
     *
     * @return for DDL, a FarragoSessionDdlStmt; for DML or query, top-level
     * SqlNode
     */
    public Object parseSqlText(
        FarragoSessionDdlValidator ddlValidator,
        String sql,
        boolean expectStatement);

    /**
     * @return the current position, or null if done parsing
     */
    public SqlParserPos getCurrentPosition();

    /**
     * @return a comma-separated list of all a database's SQL keywords that are
     * NOT also SQL92 keywords (as defined by JDBC getSQLKeywords)
     */
    public String getJdbcKeywords();

    /**
     * @return validator to use for validating DDL statements as they are
     * parsed
     */
    public FarragoSessionDdlValidator getDdlValidator();

    /**
     * @return validator to use for validating statements as they are parse
     */
    public FarragoSessionStmtValidator getStmtValidator();

    /**
     * Wraps a validation error with the current position information
     * of the parser.
     *
     * @param ex exception to be wrapped
     *
     * @return wrapping exception
     */
    public FarragoException newPositionalError(
        SqlValidatorException ex);

    /**
     * Gets a substring from the text currently being parsed.
     *
     * @param start start position (inclusive) of substring
     *
     * @param end end position (exclusive) of substring
     *
     * @return substring
     */
    public String getSubstring(SqlParserPos start, SqlParserPos end);
}


// End FarragoSessionParser.java
