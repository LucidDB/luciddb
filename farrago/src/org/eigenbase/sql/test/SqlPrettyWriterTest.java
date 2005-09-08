/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2005-2005 John V. Sichi
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
package org.eigenbase.sql.test;

import junit.framework.AssertionFailedError;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.SqlParseException;
import org.eigenbase.sql.parser.SqlParser;
import org.eigenbase.sql.pretty.SqlPrettyWriter;
import org.eigenbase.test.DiffTestCase;
import org.eigenbase.util.TestUtil;

import java.io.File;
import java.io.PrintWriter;
import java.io.Writer;

/**
 * Unit test for {@link SqlPrettyWriter}.
 *
 * <p>You must provide the system property "source.dir".
 *
 * @author Julian Hyde
 * @since 2005/8/24
 * @version $Id$
 */
public class SqlPrettyWriterTest extends DiffTestCase
{
    public static final String NL = System.getProperty("line.separator");

    public SqlPrettyWriterTest(String testCaseName) throws Exception
    {
        super(testCaseName);
    }

    // ~ Helper methods -------------------------------------------------------

    protected File getTestlogRoot() throws Exception
    {
        return new File(System.getProperty("net.sf.farrago.home"), "src");
    }

    protected static String fold(String[] strings)
    {
        return TestUtil.fold(strings);
    }

    private void assertPrintsTo(boolean newlines, final String sql, String expected)
    {
        final SqlNode node = parse(sql);
        final SqlPrettyWriter prettyWriter = new SqlPrettyWriter(SqlUtil.dummyDialect);
        prettyWriter.setAlwaysUseParentheses(false);
        if (newlines) {
            prettyWriter.setCaseClausesOnNewLines(true);
        }
        String actual = prettyWriter.format(node);
        TestUtil.assertEqualsVerbose(expected, actual);

        // Now parse the result, and make sure it is structurally equivalent
        // to the original.
        final String actual2 = actual.replaceAll("`", "\"");
        final SqlNode node2 = parse(actual2);
        assertTrue(node.equalsDeep(node2));
    }

    private void assertExprPrintsTo(boolean newlines, final String sql, String expected)
    {
        final SqlCall valuesCall = (SqlCall)parse("VALUES (" + sql + ")");
        final SqlCall rowCall = (SqlCall) valuesCall.getOperands()[0];
        final SqlNode node = rowCall.getOperands()[0];
        final SqlPrettyWriter prettyWriter = new SqlPrettyWriter(SqlUtil.dummyDialect);
        prettyWriter.setAlwaysUseParentheses(false);
        if (newlines) {
            prettyWriter.setCaseClausesOnNewLines(true);
        }
        String actual = prettyWriter.format(node);
        TestUtil.assertEqualsVerbose(expected, actual);

        // Now parse the result, and make sure it is structurally equivalent
        // to the original.
        final String actual2 = actual.replaceAll("`", "\"");
        final SqlNode valuesCall2 = parse("VALUES (" + actual2 + ")");
        assertTrue(valuesCall.equalsDeep(valuesCall2));
    }

    // ~ Tests ----------------------------------------------------------------

    public void testDefault() throws Exception
    {
        final Writer writer = openTestLog();
        final SqlNode node = parse("select x as a, b," +
            " 'mixed-Case string'," +
            " unquotedCamelCaseId," +
            " \"quoted id\" " +
            "from" +
            " (select *" +
            " from t" +
            " where x = y and a > 5" +
            " group by z, zz" +
            " window w as (partition by c)," +
            "  w1 as (partition by c,d order by a, b" +
            "   range between interval '2:2' hour to minute preceding" +
            "    and interval '1' day following)) " +
            "order by gg");
        final PrintWriter pw = new PrintWriter(writer);
        final SqlPrettyWriter prettyWriter =
            new SqlPrettyWriter(SqlUtil.dummyDialect);
        String s;

        // Default
        pw.println(":: Default");
        prettyWriter.describe(pw, true);
        s = prettyWriter.format(node);
        pw.println(s);
        pw.println();

        // Indent 8
        pw.println(":: Indent 8");
        prettyWriter.resetSettings();
        prettyWriter.setIndentation(8);
        prettyWriter.describe(pw, true);
        s = prettyWriter.format(node);
        pw.println(s);
        pw.println();

        // Clauses do not start newline
        pw.println(":: Clauses not on new line");
        prettyWriter.resetSettings();
        prettyWriter.setClauseStartsLine(false);
        prettyWriter.describe(pw, true);
        s = prettyWriter.format(node);
        pw.println(s);
        pw.println();

        // Select-list items on separate lines
        pw.println(":: Select list items on separate lines");
        prettyWriter.resetSettings();
        prettyWriter.setSelectListItemsOnSeparateLines(true);
        prettyWriter.describe(pw, true);
        s = prettyWriter.format(node);
        pw.println(s);
        pw.println();

        // Keywords lower-case
        pw.println(":: Keywords in lower-case");
        prettyWriter.resetSettings();
        prettyWriter.setKeywordsLowerCase(true);
        prettyWriter.describe(pw, true);
        s = prettyWriter.format(node);
        pw.println(s);
        pw.println();

        // Parenthesize all exprs
        pw.println(":: Parenthesize all exprs");
        prettyWriter.resetSettings();
        prettyWriter.setAlwaysUseParentheses(true);
        prettyWriter.describe(pw, true);
        s = prettyWriter.format(node);
        pw.println(s);
        pw.println();

        // Only quote identifiers which need it
        pw.println(":: Only quote identifiers which need it");
        prettyWriter.resetSettings();
        prettyWriter.setQuoteAllIdentifiers(false);
        prettyWriter.describe(pw, true);
        s = prettyWriter.format(node);
        pw.println(s);
        pw.println();

        // Damian's subquery style
        pw.println(":: Damian's subquery style. Note that ( is at the indent, SELECT is on the same line, and ) is below it");
        prettyWriter.resetSettings();
        prettyWriter.setSubqueryStyle(SqlWriter.SubqueryStyle.Black);
        prettyWriter.describe(pw, true);
        s = prettyWriter.format(node);
        pw.println(s);
        pw.println();

        // Last of all...
        pw.flush();
        // ...and put the seat down.
    }

    private SqlNode parse(final String sql)
    {
        SqlNode node;
        try {
            node = new SqlParser(sql).parseQuery();
        } catch (SqlParseException e) {
            String message = "Received error while parsing SQL '" + sql +
                    "'; error is:" + NL + e.toString();
            throw new AssertionFailedError(message);
        }
        return node;
    }

    // test disabled because default SQL parser cannot parse DDL
    public void _testExplain()
    {
        assertPrintsTo(false, "explain select * from t", "foo");
    }

    public void testCase()
    {
        // Note that CASE is rewritten to the searched form. Wish it weren't
        // so, but that's beyond the control of the pretty-printer.
        assertExprPrintsTo(true,
            "case 1 when 2 + 3 then 4 when case a when b then c else d end then 6 else 7 end",
            "CASE" + NL +
            "WHEN 1 = 2 + 3" + NL +
            "THEN 4" + NL +
            "WHEN 1 = CASE" + NL +
            "        WHEN `A` = `B`" + NL + // todo: indent should be 4 not 8
            "        THEN `C`" + NL +
            "        ELSE `D`" + NL +
            "        END" + NL +
            "THEN 6" + NL +
            "ELSE 7" + NL +
            "END");

        assertExprPrintsTo(false,
            "case 1 when 2 + 3 then 4 when case a when b then c else d end then 6 else 7 end",
            "CASE WHEN 1 = 2 + 3 THEN 4 WHEN 1 = CASE WHEN `A` = `B` THEN `C` ELSE `D` END THEN 6 ELSE 7 END");
    }

    public void testBetween()
    {
        assertExprPrintsTo(true,
            "x not between symmetric y and z",
            "`X` NOT BETWEEN SYMMETRIC `Y` AND `Z`"); // todo: remove leading space
    }

    public void testCast()
    {
        assertExprPrintsTo(true,
            "cast(x + y as decimal(5, 10))",
            "CAST(`X` + `Y` AS DECIMAL(5, 10))");
    }

    public void testLiteralChain()
    {
        assertExprPrintsTo(true,
            "'x' /* comment */ 'y'" + NL +
            "  'z' ",
            "'x' 'y' 'z'");
    }

    public void testOverlaps()
    {
        assertExprPrintsTo(true,
            "(x,xx) overlaps (y,yy) or x is not null",
            "(`X`, `XX`) OVERLAPS (`Y`, `YY`) OR `X` IS NOT NULL");
    }

    public void testUnion()
    {
        assertPrintsTo(true,
            "select * from t " +
            "union select * from (" +
            "  select * from u " +
            "  union select * from v) " +
            "union select * from w " +
            "order by a, b",

            fold(new String[] {
                "SELECT *",
                "        FROM `T`",
                "    UNION",
                "        SELECT *",
                "        FROM (SELECT *",
                "                        FROM `U`",
                "                    UNION",
                "                        SELECT *",
                "                        FROM `V`)",
                "UNION",
                "    SELECT *", // todo: should not be indented
                "    FROM `W`",
                "ORDER BY `A`, `B`"}));
    }

    public void testMultiset()
    {
        assertPrintsTo(false, "values (multiset (select * from t))",
            fold(new String[] {
                "VALUES ROW(MULTISET ((SELECT *",
                "            FROM `T`)))"}));
    }
}

// End SqlPrettyWriterTest.java
