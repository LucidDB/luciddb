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
package org.eigenbase.sql;

import org.eigenbase.util.EnumeratedValues;

/**
 * A <code>SqlWriter</code> is the target to construct a SQL statement from a
 * parse tree. It deals with dialect differences; for example, Oracle quotes
 * identifiers as <code>"scott"</code>, while SQL Server quotes them as
 * <code>[scott]</code>.
 *
 * @author Julian Hyde
 * @since 2002/8/8
 * @version $Id$
 */
public interface SqlWriter
{
    /**
     * Resets this writer so that it can format another expression.
     * Does not affect formatting preferences (see {@link #resetSettings()}
     */
    void reset();

    /**
     * Resets all properties to their default values.
     */
    void resetSettings();

    SqlDialect getDialect();

    /**
     * Prints a literal, exactly as provided.
     * Does not attempt to indent or convert to upper or lower case.
     * Does not add quotation marks.
     * Adds preceding whitespace if necessary.
     */
    void literal(String s);

    /**
     * Prints a sequence of keywords. Must not start or end with space, but
     * may contain a space. For example,
     * <code>keyword("SELECT")</code>,
     * <code>keyword("CHARACTER SET")</code>.
     */
    void keyword(String s);

    /**
     * Prints a string, preceded by whitespace if necessary.
     */
    void print(String s);

    void print(int x);

    /**
     * Prints an identifier, quoting as necessary.
     */
    void identifier(String name);

    /**
     * Prints a new line, and indents.
     */
    void newlineAndIndent();

    boolean isQuoteAllIdentifiers();

    boolean isClauseStartsLine();

    boolean isSelectListItemsOnSeparateLines();

    boolean isKeywordsLowerCase();

    /**
     * Starts a list which is a call to a function.
     *
     * @see #endFunCall(Frame)
     */
    Frame startFunCall(String funName);

    /**
     * Ends a list which is a call to a function.
     *
     * @see #startFunCall(String)
     * @param frame
     */
    void endFunCall(Frame frame);

    /**
     * Starts a list.
     */
    Frame startList(String open, String close);

    /**
     * Starts a list with no opening string.
     *
     * @param frameType Type of list. For example, a SELECT list will be
     */
    Frame startList(FrameType frameType);

    /**
     * Starts a list.
     *
     * @param frameType Type of list. For example, a SELECT list will be
     *   governed according to SELECT-list formatting preferences.
     * @param open String to start the list; typically "(" or the empty string.
     * @param close
     */
    Frame startList(FrameType frameType, String open, String close);

    /**
     * Ends a list.
     *
     * @param frame The frame which was created by {@link #startList}.
     */
    void endList(Frame frame);

    /**
     * Writes a list separator, unless the separator is "," and this is the
     * first occurrence in the list.
     *
     * @param sep List separator, typically ",".
     */
    void sep(String sep);

    /**
     * Writes a list separator.
     *
     * @param sep List separator, typically ","
     * @param printFirst Whether to print the first occurrence of the separator
     */
    void sep(String sep, boolean printFirst);

    /**
     * Sets whether whitespace is needed before the next token.
     */
    void setNeedWhitespace(boolean needWhitespace);

    /**
     * Returns the offset for each level of indentation. Default 4.
     */
    int getIndentation();

    /**
     * Returns whether to enclose all expressions in parentheses, even if the
     * operator has high enough precedence that the parentheses are not
     * required.
     *
     * <p>For example, the parentheses are required in the expression
     * <code>(a + b) * c</code> because the '*' operator has higher precedence
     * than the '+' operator, and so without the parentheses, the expression
     * would be equivalent to <code>a + (b * c)</code>. The fully-parenthesized
     * expression, <code>((a + b) * c)</code> is unambiguous even if you don't
     * know the precedence of every operator.
     */
    boolean isAlwaysUseParentheses();

    /**
     * Returns whether we are currently in a query context (SELECT, INSERT,
     * UNION, INTERSECT, EXCEPT, and the ORDER BY operator).
     */
    boolean inQuery();

    /**
     * Style of formatting subqueries.
     */
    static class SubqueryStyle extends EnumeratedValues.BasicValue
    {
        private SubqueryStyle(String name, int ordinal)
        {
            super(name, ordinal, null);
        }

        /**
         * Julian's style of subquery nesting. Like this:
         *
         * <pre>SELECT *
         * FROM (
         *     SELECT *
         *     FROM t
         * )
         * WHERE condition</pre>
         */
        public static final SubqueryStyle Hyde = new SubqueryStyle("Hyde", 0);

        /**
         * Damian's style of subquery nesting. Like this:
         *
         * <pre>SELECT *
         * FROM
         * (   SELECT *
         *     FROM t
         * )
         * WHERE condition</pre>
         */
        public static final SubqueryStyle Black = new SubqueryStyle("Black", 1);
    }

    /**
     * Enumerates the types of frame.
     */
    static class FrameType extends EnumeratedValues.BasicValue {
        public static final int Simple_ordinal = 0;
        public static final int Select_ordinal = 1;
        public static final int SelectList_ordinal = 2;
        public static final int FromList_ordinal = 3;
        public static final int OrderBy_ordinal = 4;
        public static final int OrderByList_ordinal = 5;
        public static final int GroupByList_ordinal = 6;
        public static final int WindowDeclList_ordinal = 7;
        public static final int Window_ordinal = 8;
        public static final int UpdateSetList_ordinal = 9;
        public static final int FunDecl_ordinal = 10;
        public static final int FunCall_ordinal = 11;
        public static final int Subquery_ordinal = 12;
        public static final int Setop_ordinal = 13;

        /**
         * SELECT query (or UPDATE or DELETE). The items in the list are
         * the clauses: FROM, WHERE, etc.
         */
        public static final FrameType Select =
            new FrameType("Select", Select_ordinal);

        /**
         * Simple list.
         */
        public static final FrameType Simple = new FrameType("Simple", Simple_ordinal);

        /**
         * The SELECT clause of a SELECT statement.
         */
        public static final FrameType SelectList = new FrameType("SelectList", SelectList_ordinal);

        /**
         * The WINDOW clause of a SELECT statement.
         */
        public static final FrameType WindowDeclList = new FrameType("WindowDeclList", WindowDeclList_ordinal);

        /**
         * The SET clause of an UPDATE statement.
         */
        public static final FrameType UpdateSetList = new FrameType("UpdateSetList", UpdateSetList_ordinal);

        /**
         * Function declaration.
         */
        public static final FrameType FunDecl = new FrameType("FunDecl", FunDecl_ordinal);

        /**
         * Function call or datatype declaration.
         *
         * <p>Examples:
         * <li>SUBSTRING('foobar' FROM 1 + 2 TO 4)</li>
         * <li>DECIMAL(10, 5)</li>
         */
        public static final FrameType FunCall = new FrameType("FunCall", FunCall_ordinal);

        /**
         * Window specification.
         *
         * <p>Examples:
         * <li>SUM(x) OVER (ORDER BY hireDate ROWS 3 PRECEDING)</li>
         * <li>WINDOW w1 AS (ORDER BY hireDate),
         *   w2 AS (w1 PARTITION BY gender
         *            RANGE BETWEEN INTERVAL '1' YEAR PRECEDING
         *            AND '2' MONTH PRECEDING)</li>
         */
        public static final FrameType Window = new FrameType("Window", Window_ordinal);

        /**
         * ORDER BY clause of a SELECT statement. The "list" has only two
         * items: the query and the order by clause, with ORDER BY as the
         * separator.
         */
        public static final FrameType OrderBy = new FrameType("OrderBy", OrderBy_ordinal);

        /**
         * ORDER BY list.
         *
         * <p>Example:
         * <li>ORDER BY x, y DESC, z
         */
        public static final FrameType OrderByList = new FrameType("OrderByList", OrderByList_ordinal);


        /**
         * GROUP BY list.
         *
         * <p>Example:
         * <li>GROUP BY x, FLOOR(y)
         */
        public static final FrameType GroupByList =
            new FrameType("GroupByList", GroupByList_ordinal);

        /**
         * Sub-query list. Encloses a SELECT, UNION, EXCEPT, INTERSECT query
         * with optional ORDER BY.
         *
         * <p>Example:
         * <li>GROUP BY x, FLOOR(y)
         */
        public static final FrameType Subquery =
            new FrameType("Subquery", Subquery_ordinal);

        /**
         * Set operation.
         *
         * <p>Example:
         * <li>SELECT * FROM a UNION SELECT * FROM b
         */
        public static final FrameType Setop =
            new FrameType("Setop", Setop_ordinal);

        /**
         * FROM clause (containing various kinds of JOIN).
         */
        public static final FrameType FromList =
            new FrameType("From", FromList_ordinal);

        /**
         * Creates a list type.
         */
        private FrameType(String name, int ordinal)
        {
            super(name, ordinal, null);
        }

        public static final EnumeratedValues enumeration =
            new EnumeratedValues(
                new EnumeratedValues.Value[] {
                    Select,
                    Simple,
                    SelectList,
                    WindowDeclList,
                    UpdateSetList,
                    FunDecl,
                    FunCall,
                    Window,
                    OrderBy,
                    OrderByList,
                    GroupByList,
                    Setop,
                    FromList,
                }
            );

        private static int nextOrdinal = enumeration.getMax() + 1;

        public static FrameType create(String name)
        {
            return new FrameType(name, FrameType.nextOrdinal++);
        }
    }

    /**
     * A Frame is a piece of generated text which shares a common indentation
     * level.
     *
     * <p>Every frame has a beginning, a series of clauses and separators, and
     * an end. A typical frame is a comma-separated list. It begins with a "(",
     * consists of expressions separated by ",", and ends with a ")".
     *
     * <p>A select statement is also a kind of frame. The beginning and end
     * are are empty strings, but it consists of a sequence of clauses.
     * "SELECT", "FROM", "WHERE" are separators.
     *
     * <p>A frame is current between a call to one of the
     * {@link SqlWriter#startList} methods and the call to
     * {@link SqlWriter#endList(Frame)}. If other code starts a frame in the
     * mean time, the sub-frame is put onto a stack.
     */
    public interface Frame {

    }
}

// End SqlWriter.java
