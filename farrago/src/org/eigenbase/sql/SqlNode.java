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

package org.eigenbase.sql;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.eigenbase.sql.parser.ParserPosition;


/**
 * A <code>SqlNode</code> is a SQL parse tree. It may be an {@link SqlOperator
 * operator}, {@link SqlLiteral literal}, {@link SqlIdentifier identifier},
 * and so forth.
 *
 * @author jhyde
 * @version $Id$
 * @since Dec 12, 2003
 */
public abstract class SqlNode
{
    //~ Instance fields -------------------------------------------------------

    private final ParserPosition pos;

    //~ Constructors ----------------------------------------------------------

    SqlNode(ParserPosition pos)
    {
        this.pos = pos;
    }

    //~ Methods ---------------------------------------------------------------

    public abstract Object clone();

    /**
     * Returns whether this node is a particular kind.
     *
     * @param kind a {@link org.eigenbase.sql.SqlKind} value
     */
    public boolean isA(SqlKind kind)
    {
        // REVIEW jvs 6-Feb-2004:  either this should be
        // getKind().isA(kind), or this method should be renamed to
        // avoid confusion
        return getKind() == kind;
    }

    /**
     * Returns the type of node this is, or {@link org.eigenbase.sql.SqlKind#Other} if it's
     * nothing special.
     *
     * @return a {@link SqlKind} value, never null
     */
    public SqlKind getKind()
    {
        return SqlKind.Other;
    }

    public static SqlNode [] cloneArray(SqlNode [] nodes)
    {
        SqlNode [] clones = (SqlNode []) nodes.clone();
        for (int i = 0; i < clones.length; i++) {
            SqlNode node = clones[i];
            if (node != null) {
                clones[i] = (SqlNode) node.clone();
            }
        }
        return clones;
    }

    public String toString()
    {
        return toSqlString(null);
    }

    /**
     * Returns the SQL text of the tree of which this <code>SqlNode</code> is
     * the root.
     *
     * <p>Typical return values are:<ul>
     * <li>'It''s a bird!'</li>
     * <li>NULL</li>
     * <li>12.3</li>
     * <li>DATE '1969-04-29'</li>
     * </ul>
     */
    public String toSqlString(SqlDialect dialect)
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        if (dialect == null) {
            dialect = SqlUtil.dummyDialect;
        }
        SqlWriter writer = new SqlWriter(dialect, pw);
        unparse(writer, 0, 0);
        pw.flush();
        return sw.toString();
    }

    /**
     * Writes a SQL representation of this node to a writer.
     *
     * <p>The <code>leftPrec</code> and <code>rightPrec</code> parameters
     * give us enough context to decide whether we need to enclose the
     * expression in parentheses. For example, we need parentheses around
     * "2 + 3" if preceded by "5 *". This is because the precedence of the "*"
     * operator is greater than the precedence of the "+" operator.
     *
     * <p>The algorithm handles left- and right-associative operators by giving
     * them slightly different left- and right-precedence.
     *
     * <p>If {@link SqlWriter#alwaysUseParentheses} is true, we use parentheses
     * even when they are not required by the precedence rules.
     *
     * <p>For the details of this algorithm, see {@link SqlCall#unparse}.
     *
     * @param writer Target writer
     * @param leftPrec The precedence of the {@link SqlNode} immediately
     *   preceding this node in a depth-first scan of the parse tree
     * @param rightPrec The precedence of the {@link SqlNode} immediately
     *   following this node in a depth-first scan of the parse tree
     */
    public abstract void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec);

    public ParserPosition getParserPosition()
    {
        return pos;
    }
}


// End SqlNode.java
