/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2002-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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
package org.eigenbase.sql.pretty;

import java.io.*;

import java.lang.reflect.*;

import java.util.*;

import org.eigenbase.sql.*;
import org.eigenbase.util.*;


/**
 * Pretty printer for SQL statements.
 *
 * <p>There are several options to control the format.
 *
 * <table>
 * <tr>
 * <th>Option</th>
 * <th>Description</th>
 * <th>Default</th>
 * </tr>
 * <tr>
 * <td>{@link #setSelectListItemsOnSeparateLines SelectListItemsOnSeparateLines}
 * </td>
 * <td>Whether each item in the select clause is on its own line</td>
 * <td>false</td>
 * </tr>
 * <tr>
 * <td>{@link #setCaseClausesOnNewLines CaseClausesOnNewLines}</td>
 * <td>Whether the WHEN, THEN and ELSE clauses of a CASE expression appear at
 * the start of a new line.</td>
 * <td>false</td>
 * </tr>
 * <tr>
 * <td>{@link #setIndentation Indentation}</td>
 * <td>Number of spaces to indent</td>
 * <td>4</td>
 * </tr>
 * <tr>
 * <td>{@link #setKeywordsLowerCase KeywordsLowerCase}</td>
 * <td>Whether to print keywords (SELECT, AS, etc.) in lower-case.</td>
 * <td>false</td>
 * </tr>
 * <tr>
 * <td>{@link #isAlwaysUseParentheses ParenthesizeAllExprs}</td>
 * <td>Whether to enclose all expressions in parentheses, even if the operator
 * has high enough precedence that the parentheses are not required.
 *
 * <p>For example, the parentheses are required in the expression <code>(a + b)
 * c</code> because the '*' operator has higher precedence than the '+'
 * operator, and so without the parentheses, the expression would be equivalent
 * to <code>a + (b * c)</code>. The fully-parenthesized expression, <code>((a +
 * b) * c)</code> is unambiguous even if you don't know the precedence of every
 * operator.</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>{@link #setQuoteAllIdentifiers QuoteAllIdentifiers}</td>
 * <td>Whether to quote all identifiers, even those which would be correct
 * according to the rules of the {@link SqlDialect} if quotation marks were
 * omitted.</td>
 * <td>true</td>
 * </tr>
 * <tr>
 * <td>{@link #setSelectListItemsOnSeparateLines SelectListItemsOnSeparateLines}
 * </td>
 * <td>Whether each item in the select clause is on its own line.</td>
 * <td>false</td>
 * </tr>
 * <tr>
 * <td>{@link #setSubqueryStyle SubqueryStyle}</td>
 * <td>Style for formatting sub-queries. Values are: {@link SubqueryStyle#Hyde
 * Hyde}, {@link SubqueryStyle#Black Black}.</td>
 * <td>{@link SubqueryStyle#Hyde Hyde}</td>
 * </tr>
 * </table>
 *
 * @author Julian Hyde
 * @version $Id$
 * @since 2005/8/24
 */
public class SqlPrettyWriter
    implements SqlWriter
{

    //~ Static fields/initializers ---------------------------------------------

    /**
     * Bean holding the default property values.
     */
    private static final Bean defaultBean =
        new SqlPrettyWriter(SqlUtil.dummyDialect).getBean();
    protected static final String NL = System.getProperty("line.separator");

    private static final String [] spaces =
        {
            "",
            " ",
            "  ",
            "   ",
            "    ",
            "     ",
            "      ",
            "       ",
            "        ",
        };

    //~ Instance fields --------------------------------------------------------

    private final SqlDialect dialect;
    private final StringWriter sw = new StringWriter();
    protected final PrintWriter pw;
    private final Stack<FrameImpl> listStack = new Stack<FrameImpl>();
    protected FrameImpl frame;
    private boolean needWhitespace;
    protected String nextWhitespace;
    protected boolean alwaysUseParentheses;
    private boolean keywordsLowerCase;
    private Bean bean;
    private boolean quoteAllIdentifiers;
    private int indentation;
    private boolean clauseStartsLine;
    private boolean selectListItemsOnSeparateLines;
    private int currentIndent;
    private boolean windowDeclListNewline;
    private boolean updateSetListNewline;
    private boolean windowNewline;
    private SubqueryStyle subqueryStyle;

    private boolean caseClausesOnNewLines;

    //~ Constructors -----------------------------------------------------------

    public SqlPrettyWriter(
        SqlDialect dialect,
        boolean alwaysUseParentheses,
        PrintWriter pw)
    {
        if (pw == null) {
            pw = new PrintWriter(sw);
        }
        this.pw = pw;
        this.dialect = dialect;
        this.alwaysUseParentheses = alwaysUseParentheses;
        resetSettings();
        reset();
    }

    public SqlPrettyWriter(
        SqlDialect dialect,
        boolean alwaysUseParentheses)
    {
        this(dialect, alwaysUseParentheses, null);
    }

    public SqlPrettyWriter(SqlDialect dialect)
    {
        this(dialect, true);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Sets whether the WHEN, THEN and ELSE clauses of a CASE expression appear
     * at the start of a new line. The default is false.
     */
    public void setCaseClausesOnNewLines(boolean caseClausesOnNewLines)
    {
        this.caseClausesOnNewLines = caseClausesOnNewLines;
    }

    /**
     * Sets the subquery style. Default is {@link SubqueryStyle#Hyde}.
     */
    public void setSubqueryStyle(SubqueryStyle subqueryStyle)
    {
        this.subqueryStyle = subqueryStyle;
    }

    public int getIndentation()
    {
        return indentation;
    }

    public boolean isAlwaysUseParentheses()
    {
        return alwaysUseParentheses;
    }

    public boolean inQuery()
    {
        return
            (frame == null) || (frame.frameType == FrameType.OrderBy)
            || (frame.frameType == FrameType.Setop);
    }

    public boolean isQuoteAllIdentifiers()
    {
        return quoteAllIdentifiers;
    }

    public boolean isClauseStartsLine()
    {
        return clauseStartsLine;
    }

    public boolean isSelectListItemsOnSeparateLines()
    {
        return selectListItemsOnSeparateLines;
    }

    public boolean isKeywordsLowerCase()
    {
        return keywordsLowerCase;
    }

    public void resetSettings()
    {
        reset();
        indentation = 4;
        clauseStartsLine = true;
        selectListItemsOnSeparateLines = false;
        keywordsLowerCase = false;
        quoteAllIdentifiers = true;
        windowDeclListNewline = true;
        updateSetListNewline = true;
        windowNewline = false;
        subqueryStyle = SubqueryStyle.Hyde;
        alwaysUseParentheses = false;
    }

    public void reset()
    {
        pw.flush();
        sw.getBuffer().setLength(0);
        needWhitespace = false;
        nextWhitespace = " ";
    }

    /**
     * Returns an object which encapsulates each property as a get/set method.
     */
    private Bean getBean()
    {
        if (bean == null) {
            bean = new Bean(this);
        }
        return bean;
    }

    /**
     * Sets the number of spaces indentation.
     *
     * @see #getIndentation()
     */
    public void setIndentation(int indentation)
    {
        this.indentation = indentation;
    }

    /**
     * Prints the property settings of this pretty-writer to a writer.
     *
     * @param pw Writer
     * @param omitDefaults Whether to omit properties whose value is the same as
     * the default
     */
    public void describe(PrintWriter pw, boolean omitDefaults)
    {
        final Bean properties = getBean();
        final String [] propertyNames = properties.getPropertyNames();
        int count = 0;
        for (int i = 0; i < propertyNames.length; i++) {
            String key = propertyNames[i];
            final Object value = bean.get(key);
            final Object defaultValue = defaultBean.get(key);
            if (Util.equal(value, defaultValue)) {
                continue;
            }
            if (count++ > 0) {
                pw.print(",");
            }
            pw.print(key + "=" + value);
        }
    }

    /**
     * Sets settings from a properties object.
     */
    public void setSettings(Properties properties)
    {
        resetSettings();
        final Bean bean = getBean();
        final String [] propertyNames = bean.getPropertyNames();
        for (int i = 0; i < propertyNames.length; i++) {
            String propertyName = propertyNames[i];
            final String value = properties.getProperty(propertyName);
            if (value != null) {
                bean.set(propertyName, value);
            }
        }
    }

    /**
     * Sets whether a clause (FROM, WHERE, GROUP BY, HAVING, WINDOW, ORDER BY)
     * starts a new line. Default is true. SELECT is always at the start of a
     * line.
     */
    public void setClauseStartsLine(boolean clauseStartsLine)
    {
        this.clauseStartsLine = clauseStartsLine;
    }

    /**
     * Sets whether each item in a SELECT list, GROUP BY list, or ORDER BY list
     * is on its own line. Default false.
     */
    public void setSelectListItemsOnSeparateLines(boolean b)
    {
        this.selectListItemsOnSeparateLines = b;
    }

    /**
     * Sets whether to print keywords (SELECT, AS, etc.) in lower-case. The
     * default is false: keywords are printed in upper-case.
     */
    public void setKeywordsLowerCase(boolean b)
    {
        this.keywordsLowerCase = b;
    }

    public void setAlwaysUseParentheses(boolean b)
    {
        this.alwaysUseParentheses = b;
    }

    public void newlineAndIndent()
    {
        pw.println();
        indent(currentIndent);
        needWhitespace = false; // no further whitespace necessary
    }

    void indent(int indent)
    {
        if (indent < 0) {
            throw new IllegalArgumentException("negative indent " + indent);
        } else if (indent <= 8) {
            pw.print(spaces[indent]);
        } else {
            // Print space in chunks of 8 to amortize cost of calls to print.
            final int rem = indent % 8;
            final int div = indent / 8;
            for (int i = 0; i < div; ++i) {
                pw.print(spaces[8]);
            }
            if (rem > 0) {
                pw.print(spaces[rem]);
            }
        }
    }

    /**
     * Sets whether to quote all identifiers, even those which would be correct
     * according to the rules of the {@link SqlDialect} if quotation marks were
     * omitted.
     *
     * <p>Default true.
     */
    public void setQuoteAllIdentifiers(boolean b)
    {
        this.quoteAllIdentifiers = b;
    }

    /**
     * Creates a list frame.
     *
     * <p>Derived classes should override this method to specify the indentation
     * of the list.
     *
     * @param frameType What type of list
     * @param keyword The keyword to be printed at the start of the list
     * @param open The string to print at the start of the list
     * @param close The string to print at the end of the list
     *
     * @return A frame
     */
    protected FrameImpl createListFrame(
        FrameType frameType,
        String keyword,
        String open,
        String close)
    {
        int indentation = getIndentation();
        switch (frameType.getOrdinal()) {
        case FrameType.WindowDeclList_ordinal:
            return
                new FrameImpl(
                    frameType,
                    keyword,
                    open,
                    close,
                    indentation,
                    false,
                    false,
                    indentation,
                    windowDeclListNewline,
                    false,
                    false);

        case FrameType.UpdateSetList_ordinal:
            return
                new FrameImpl(
                    frameType,
                    keyword,
                    open,
                    close,
                    indentation,
                    false,
                    updateSetListNewline,
                    indentation,
                    false,
                    false,
                    false);

        case FrameType.SelectList_ordinal:
        case FrameType.OrderByList_ordinal:
        case FrameType.GroupByList_ordinal:
            return
                new FrameImpl(
                    frameType,
                    keyword,
                    open,
                    close,
                    indentation,
                    selectListItemsOnSeparateLines,
                    false,
                    indentation,
                    selectListItemsOnSeparateLines,
                    false,
                    false);

        case FrameType.Subquery_ordinal:
            if (subqueryStyle == SubqueryStyle.Black) {
                // Generate, e.g.:
                //
                // WHERE foo = bar IN
                // (   SELECT ...
                open = "(" + spaces(indentation - 1);
                return
                    new FrameImpl(
                        frameType,
                        keyword,
                        open,
                        close,
                        0,
                        false,
                        true,
                        indentation,
                        false,
                        false,
                        false) {
                        protected void _before()
                        {
                            newlineAndIndent();
                        }
                    };
            } else if (subqueryStyle == SubqueryStyle.Hyde) {
                // Generate, e.g.:
                //
                // WHERE foo IN (
                //     SELECT ...
                return
                    new FrameImpl(
                        frameType,
                        keyword,
                        open,
                        close,
                        0,
                        false,
                        true,
                        0,
                        false,
                        false,
                        false) {
                        protected void _before()
                        {
                            nextWhitespace = NL;
                        }
                    };
            } else {
                throw subqueryStyle.unexpected();
            }

        case FrameType.OrderBy_ordinal:
            return
                new FrameImpl(
                    frameType,
                    keyword,
                    open,
                    close,
                    0,
                    false,
                    true,
                    0,
                    false,
                    false,
                    false);

        case FrameType.Select_ordinal:
            return
                new FrameImpl(
                    frameType,
                    keyword,
                    open,
                    close,
                    indentation,
                    false,
                    isClauseStartsLine(), // newline before FROM, WHERE etc.
                    0, // all clauses appear below SELECT
                    false,
                    false,
                    false);

        case FrameType.Setop_ordinal:
            return
                new FrameImpl(
                    frameType,
                    keyword,
                    open,
                    close,
                    indentation,
                    false,
                    isClauseStartsLine(), // newline before UNION, EXCEPT
                    0, // all clauses appear below SELECT
                    isClauseStartsLine(), // newline after UNION, EXCEPT
                    false,
                    false);

        case FrameType.Window_ordinal:
            return
                new FrameImpl(
                    frameType,
                    keyword,
                    open,
                    close,
                    indentation,
                    false,
                    windowNewline,
                    0,
                    false,
                    false,
                    false);

        case FrameType.FunCall_ordinal:
            needWhitespace = false;
            return
                new FrameImpl(
                    frameType,
                    keyword,
                    open,
                    close,
                    indentation,
                    false,
                    false,
                    indentation,
                    false,
                    false,
                    false);

        case FrameType.Identifier_ordinal:
        case FrameType.Simple_ordinal:
            return
                new FrameImpl(
                    frameType,
                    keyword,
                    open,
                    close,
                    indentation,
                    false,
                    false,
                    indentation,
                    false,
                    false,
                    false);

        case FrameType.FromList_ordinal:
            return
                new FrameImpl(
                    frameType,
                    keyword,
                    open,
                    close,
                    indentation,
                    false,
                    isClauseStartsLine(), // newline before UNION, EXCEPT
                    0, // all clauses appear below SELECT
                    isClauseStartsLine(), // newline after UNION, EXCEPT
                    false,
                    false) {
                    protected void sep(boolean printFirst, String sep)
                    {
                        boolean newlineBefore =
                            newlineBeforeSep
                            && !sep.equals(",");
                        boolean newlineAfter =
                            newlineAfterSep
                            && sep.equals(",");
                        if ((itemCount > 0) || printFirst) {
                            if (newlineBefore && (itemCount > 0)) {
                                pw.println();
                                indent(currentIndent + sepIndent);
                                needWhitespace = false;
                            }
                            keyword(sep);
                            nextWhitespace = newlineAfter ? NL : " ";
                        }
                        ++itemCount;
                    }
                };

        default:
            boolean newlineAfterOpen = false;
            boolean newlineBeforeSep = false;
            boolean newlineBeforeClose = false;
            int sepIndent = indentation;
            if (frameType.getName().equals("CASE")) {
                if (caseClausesOnNewLines) {
                    newlineAfterOpen = true;
                    newlineBeforeSep = true;
                    newlineBeforeClose = true;
                    sepIndent = 0;
                }
            }
            return
                new FrameImpl(
                    frameType,
                    keyword,
                    open,
                    close,
                    indentation,
                    newlineAfterOpen,
                    newlineBeforeSep,
                    sepIndent,
                    false,
                    newlineBeforeClose,
                    false);
        }
    }

    /**
     * Returns a string of N spaces.
     */
    private static String spaces(int i)
    {
        if (i <= 8) {
            return spaces[i];
        } else {
            char [] chars = new char[i];
            Arrays.fill(chars, ' ');
            return new String(chars);
        }
    }

    /**
     * Starts a list.
     *
     * @param frameType Type of list. For example, a SELECT list will be
     * governed according to SELECT-list formatting preferences.
     * @param open String to print at the start of the list; typically "(" or
     * the empty string.
     * @param close String to print at the end of the list.
     */
    protected Frame startList(
        FrameType frameType,
        String keyword,
        String open,
        String close)
    {
        assert frameType != null;
        if (frame != null) {
            ++frame.itemCount;

            // REVIEW jvs 9-June-2006:  This is part of the fix for FRG-149
            // (extra frame for identifier was leading to extra indentation,
            // causing select list to come out raggedy with identifiers
            // deeper than literals); are there other frame types
            // for which extra indent should be suppressed?
            if (frameType.needsIndent()) {
                currentIndent += frame.extraIndent;
            }
            assert !listStack.contains(frame);
            listStack.push(frame);
        }
        frame = createListFrame(frameType, keyword, open, close);
        frame.before();
        return frame;
    }

    public void endList(Frame frame)
    {
        FrameImpl endedFrame = (FrameImpl) frame;
        Util.pre(frame == this.frame,
            "Frame " + endedFrame.frameType
            + " does not match current frame " + this.frame.frameType);
        if (this.frame == null) {
            throw new RuntimeException("No list started");
        }
        if (this.frame.open.equals("(")) {
            if (!this.frame.close.equals(")")) {
                throw new RuntimeException("Expected ')'");
            }
        }
        if (this.frame.newlineBeforeClose) {
            newlineAndIndent();
        }
        keyword(this.frame.close);
        if (this.frame.newlineAfterClose) {
            newlineAndIndent();
        }

        // Pop the frame, and move to the previous indentation level.
        if (listStack.isEmpty()) {
            this.frame = null;
            assert currentIndent == 0 : currentIndent;
        } else {
            this.frame = listStack.pop();
            if (endedFrame.frameType.needsIndent()) {
                currentIndent -= this.frame.extraIndent;
            }
        }
    }

    public String format(SqlNode node)
    {
        assert frame == null;
        node.unparse(this, 0, 0);
        assert frame == null;
        return toString();
    }

    public String toString()
    {
        pw.flush();
        return sw.toString();
    }

    public SqlDialect getDialect()
    {
        return dialect;
    }

    public void literal(String s)
    {
        print(s);
        needWhitespace = true;
    }

    public void keyword(String s)
    {
        maybeWhitespace(s);
        pw.print(
            isKeywordsLowerCase() ? s.toLowerCase() : s.toUpperCase());
        if (!s.equals("")) {
            needWhitespace = needWhitespaceAfter(s);
        }
    }

    private void maybeWhitespace(String s)
    {
        if (needWhitespace
            && needWhitespaceBefore(s)) {
            whiteSpace();
        }
    }

    private static boolean needWhitespaceBefore(String s)
    {
        return
            !(
                s.equals(",")
                || s.equals(".")
                || s.equals(")")
                || s.equals("]")
                || s.equals("")
             );
    }

    private static boolean needWhitespaceAfter(String s)
    {
        return !(s.equals("(")
                || s.equals("[")
                || s.equals("."));
    }

    protected void whiteSpace()
    {
        if (needWhitespace) {
            if (nextWhitespace == NL) {
                newlineAndIndent();
            } else {
                pw.print(nextWhitespace);
            }
            nextWhitespace = " ";
            needWhitespace = false;
        }
    }

    public void print(String s)
    {
        if (s.equals("(")) {
            throw new RuntimeException("Use 'startList'");
        }
        if (s.equals(")")) {
            throw new RuntimeException("Use 'endList'");
        }
        maybeWhitespace(s);
        pw.print(s);
    }

    public void print(int x)
    {
        maybeWhitespace("0");
        pw.print(x);
    }

    public void identifier(String name)
    {
        whiteSpace();
        if (isQuoteAllIdentifiers()
            || dialect.identifierNeedsToBeQuoted(name)) {
            pw.print(dialect.quoteIdentifier(name));
        } else {
            pw.print(name);
        }
        needWhitespace = true;
    }

    public Frame startFunCall(String funName)
    {
        keyword(funName);
        setNeedWhitespace(false);
        return startList(FrameType.FunCall, "(", ")");
    }

    public void endFunCall(Frame frame)
    {
        endList(this.frame);
    }

    public Frame startList(String open, String close)
    {
        return startList(FrameType.Simple, null, open, close);
    }

    public Frame startList(FrameType frameType)
    {
        assert frameType != null;
        return startList(frameType, null, "", "");
    }

    public Frame startList(FrameType frameType, String open, String close)
    {
        assert frameType != null;
        return startList(frameType, null, open, close);
    }

    public void sep(String sep)
    {
        sep(sep, !(sep.equals(",") || sep.equals(".")));
    }

    public void sep(String sep, boolean printFirst)
    {
        if (frame == null) {
            throw new RuntimeException("No list started");
        }
        if (sep.startsWith(" ") || sep.endsWith(" ")) {
            throw new RuntimeException("Separator must not contain whitespace");
        }
        frame.sep(printFirst, sep);
    }

    public void setNeedWhitespace(boolean needWhitespace)
    {
        this.needWhitespace = needWhitespace;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Implementation of {@link Frame}.
     */
    protected class FrameImpl
        implements Frame
    {
        final FrameType frameType;
        final String keyword;
        final String open;
        final String close;

        /**
         * Indent of sub-frame with respect to this one.
         */
        final int extraIndent;

        /**
         * Indent of separators with respect to this frame's indent. Typically
         * zero.
         */
        final int sepIndent;

        /**
         * Number of items which have been printed in this list so far.
         */
        int itemCount;

        /**
         * Whether to print a newline before each separator.
         */
        public boolean newlineBeforeSep;

        /**
         * Whether to print a newline after each separator.
         */
        public boolean newlineAfterSep;
        private final boolean newlineBeforeClose;
        private final boolean newlineAfterClose;
        private boolean newlineAfterOpen;

        FrameImpl(
            FrameType frameType,
            String keyword,
            String open,
            String close,
            int extraIndent,
            boolean newlineAfterOpen,
            boolean newlineBeforeSep,
            int sepIndent,
            boolean newlineAfterSep,
            boolean newlineBeforeClose,
            boolean newlineAfterClose)
        {
            this.frameType = frameType;
            this.keyword = keyword;
            this.open = open;
            this.close = close;
            this.extraIndent = extraIndent;
            this.newlineAfterOpen = newlineAfterOpen;
            this.newlineBeforeSep = newlineBeforeSep;
            this.newlineAfterSep = newlineAfterSep;
            this.newlineBeforeClose = newlineBeforeClose;
            this.newlineAfterClose = newlineAfterClose;
            this.sepIndent = sepIndent;
        }

        protected void before()
        {
            if ((open != null) && !open.equals("")) {
                keyword(open);
            }
        }

        protected void after()
        {
        }

        protected void sep(boolean printFirst, String sep)
        {
            if ((newlineBeforeSep && (itemCount > 0))
                || (newlineAfterOpen && (itemCount == 0))) {
                newlineAndIndent();
            }
            if ((itemCount > 0) || printFirst) {
                keyword(sep);
                nextWhitespace = newlineAfterSep ? NL : " ";
            }
            ++itemCount;
        }
    }

    /**
     * Helper class which exposes the get/set methods of an object as
     * properties.
     */
    private static class Bean
    {
        private final SqlPrettyWriter o;
        private final Map<String,Method> getterMethods = new HashMap<String, Method>();
        private final Map<String,Method> setterMethods = new HashMap<String, Method>();

        Bean(SqlPrettyWriter o)
        {
            this.o = o;

            // Figure out the getter/setter methods for each attribute.
            final Method [] methods = o.getClass().getMethods();
            for (int i = 0; i < methods.length; i++) {
                Method method = methods[i];
                if (method.getName().startsWith("set")
                    && (method.getReturnType() == Void.class)
                    && (method.getParameterTypes().length == 1)) {
                    String attributeName = stripPrefix(
                            method.getName(),
                            3);
                    setterMethods.put(attributeName, method);
                }
                if (method.getName().startsWith("get")
                    && (method.getReturnType() != Void.class)
                    && (method.getParameterTypes().length == 0)) {
                    String attributeName = stripPrefix(
                            method.getName(),
                            3);
                    getterMethods.put(attributeName, method);
                }
                if (method.getName().startsWith("is")
                    && (method.getReturnType() == Boolean.class)
                    && (method.getParameterTypes().length == 0)) {
                    String attributeName = stripPrefix(
                            method.getName(),
                            2);
                    getterMethods.put(attributeName, method);
                }
            }
        }

        private String stripPrefix(String name, int offset)
        {
            return name.substring(offset, offset + 1).toLowerCase()
                + name.substring(offset + 1);
        }

        public void set(String name, String value)
        {
            final Method method = setterMethods.get(name);
            try {
                method.invoke(
                    o,
                    value);
            } catch (IllegalAccessException e) {
                throw Util.newInternal(e);
            } catch (InvocationTargetException e) {
                throw Util.newInternal(e);
            }
        }

        public Object get(String name)
        {
            final Method method = getterMethods.get(name);
            try {
                return method.invoke(
                        o,
                        new Object[0]);
            } catch (IllegalAccessException e) {
                throw Util.newInternal(e);
            } catch (InvocationTargetException e) {
                throw Util.newInternal(e);
            }
        }

        public String [] getPropertyNames()
        {
            final Set<String> names = new HashSet<String>();
            names.addAll(getterMethods.keySet());
            names.addAll(setterMethods.keySet());
            return (String []) names.toArray(new String[names.size()]);
        }
    }
}

// End SqlPrettyWriter.java
