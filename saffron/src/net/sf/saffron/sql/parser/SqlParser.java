/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.saffron.sql.parser;

import net.sf.saffron.sql.SqlNode;

import java.io.Reader;
import java.io.StringReader;


/**
 * A <code>SqlParser</code> parses a SQL statement.
 *
 * @author jhyde$
 * @version $Id$
 *
 * @since Mar 18, 2003$
 */
public class SqlParser
{
    //~ Instance fields -------------------------------------------------------

    private final Parser parser;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creats a <code>SqlParser</code> which reads input from a string.
     */
    public SqlParser(String s)
    {
        parser = new Parser(new StringReader(s));
    }

    /**
     * Creats a <code>SqlParser</code> which reads input from a reader.
     */
    public SqlParser(Reader reader)
    {
        parser = new Parser(reader);
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Parses a SQL expression.
     *
     * @throws ParseException if there is a parse error
     */
    public SqlNode parseExpression() throws ParseException
    {
        return parser.SqlExpressionEof();
    }

    /**
     * Parses a <code>SELECT</code> statement.
     *
     * @return A {@link net.sf.saffron.sql.SqlSelect} for a regular
     *         <code>SELECT</code> statement; a {@link
     *         net.sf.saffron.sql.SqlBinaryOperator} for a
     *         <code>UNION</code>, <code>INTERSECT</code>, or
     *         <code>EXCEPT</code>.
     *
     * @throws ParseException if there is a parse error
     */
    public SqlNode parseQuery() throws ParseException
    {
        return parser.SqlQueryEof();
    }

    /**
     * Parses an SQL statement.
     *
     * @return top-level SqlNode representing stmt
     *
     * @throws ParseException if there is a parse error
     */
    public SqlNode parseStmt() throws ParseException
    {
        return parser.SqlStmtEof();
    }
}


// End SqlParser.java
