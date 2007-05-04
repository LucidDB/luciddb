/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
package org.eigenbase.sql.parser;

import java.io.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.impl.*;


/**
 * A <code>SqlParser</code> parses a SQL statement.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 18, 2003
 */
public class SqlParser
{

    //~ Instance fields --------------------------------------------------------

    private final SqlParserImpl parser;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a <code>SqlParser</code> which reads input from a string.
     */
    public SqlParser(String s)
    {
        parser = new SqlParserImpl(new StringReader(s));
        parser.setTabSize(1);
    }

    /**
     * Creates a <code>SqlParser</code> which reads input from a reader.
     */
    public SqlParser(Reader reader)
    {
        parser = new SqlParserImpl(reader);
        parser.setTabSize(1);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Parses a SQL expression.
     *
     * @throws SqlParseException if there is a parse error
     */
    public SqlNode parseExpression()
        throws SqlParseException
    {
        try {
            return parser.SqlExpressionEof();
        } catch (Throwable ex) {
            throw parser.normalizeException(ex);
        }
    }

    /**
     * Parses a <code>SELECT</code> statement.
     *
     * @return A {@link org.eigenbase.sql.SqlSelect} for a regular <code>
     * SELECT</code> statement; a {@link org.eigenbase.sql.SqlBinaryOperator}
     * for a <code>UNION</code>, <code>INTERSECT</code>, or <code>EXCEPT</code>.
     *
     * @throws SqlParseException if there is a parse error
     */
    public SqlNode parseQuery()
        throws SqlParseException
    {
        try {
            return parser.SqlQueryEof();
        } catch (Throwable ex) {
            throw parser.normalizeException(ex);
        }
    }

    /**
     * Parses an SQL statement.
     *
     * @return top-level SqlNode representing stmt
     *
     * @throws SqlParseException if there is a parse error
     */
    public SqlNode parseStmt()
        throws SqlParseException
    {
        try {
            return parser.SqlStmtEof();
        } catch (Throwable ex) {
            throw parser.normalizeException(ex);
        }
    }

    /**
     * Returns the underlying generated parser.
     */
    public SqlParserImpl getParserImpl()
    {
        return parser;
    }
}

// End SqlParser.java
