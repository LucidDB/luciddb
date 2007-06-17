/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
package org.eigenbase.test;

import java.io.*;

import java.text.*;

import java.util.*;

import junit.framework.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;


/**
 * Unit test for SQL limits.
 *
 * @author jhyde
 * @version $Id$
 */
public class SqlLimitsTest
    extends TestCase
{
    //~ Static fields/initializers ---------------------------------------------

    private static final List<BasicSqlType> typeList =
        new ArrayList<BasicSqlType>();

    static {
        typeList.add(new BasicSqlType(SqlTypeName.BOOLEAN));
        typeList.add(new BasicSqlType(SqlTypeName.TINYINT));
        typeList.add(new BasicSqlType(SqlTypeName.SMALLINT));
        typeList.add(new BasicSqlType(SqlTypeName.INTEGER));
        typeList.add(new BasicSqlType(SqlTypeName.BIGINT));
        typeList.add(new BasicSqlType(SqlTypeName.DECIMAL));
        typeList.add(new BasicSqlType(SqlTypeName.DECIMAL, 5));
        typeList.add(new BasicSqlType(SqlTypeName.DECIMAL, 6, 2));
        typeList.add(
            new BasicSqlType(
                SqlTypeName.DECIMAL,
                SqlTypeName.DECIMAL.getMaxPrecision(),
                0));
        typeList.add(
            new BasicSqlType(
                SqlTypeName.DECIMAL,
                SqlTypeName.DECIMAL.getMaxPrecision(),
                5));

        // todo: test Float, Real, Double
        typeList.add(new BasicSqlType(SqlTypeName.CHAR, 5));
        typeList.add(new BasicSqlType(SqlTypeName.VARCHAR, 1));
        typeList.add(new BasicSqlType(SqlTypeName.VARCHAR, 20));
        typeList.add(new BasicSqlType(SqlTypeName.BINARY, 3));
        typeList.add(new BasicSqlType(SqlTypeName.VARBINARY, 4));
        typeList.add(new BasicSqlType(SqlTypeName.DATE));
        typeList.add(new BasicSqlType(SqlTypeName.TIME, 0));
        typeList.add(new BasicSqlType(SqlTypeName.TIMESTAMP, 0));
        // todo: test IntervalDayTime and IntervalYearMonth
    }

    //~ Constructors -----------------------------------------------------------

    public SqlLimitsTest(String name)
    {
        super(name);
    }

    //~ Methods ----------------------------------------------------------------

    protected DiffRepository getDiffRepos()
    {
        return DiffRepository.lookup(SqlLimitsTest.class);
    }

    /**
     * Returns a list of typical types.
     */
    public static List<BasicSqlType> getTypes()
    {
        return typeList;
    }

    public void testPrintLimits()
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        for (BasicSqlType type : typeList) {
            pw.println(type.toString());
            printLimit(
                pw,
                "  min - epsilon:          ",
                type,
                false,
                SqlTypeName.Limit.OVERFLOW,
                true);
            printLimit(
                pw,
                "  min:                    ",
                type,
                false,
                SqlTypeName.Limit.OVERFLOW,
                false);
            printLimit(
                pw,
                "  zero - delta:           ",
                type,
                false,
                SqlTypeName.Limit.UNDERFLOW,
                false);
            printLimit(
                pw,
                "  zero - delta + epsilon: ",
                type,
                false,
                SqlTypeName.Limit.UNDERFLOW,
                true);
            printLimit(
                pw,
                "  zero:                   ",
                type,
                false,
                SqlTypeName.Limit.ZERO,
                false);
            printLimit(
                pw,
                "  zero + delta - epsilon: ",
                type,
                true,
                SqlTypeName.Limit.UNDERFLOW,
                true);
            printLimit(
                pw,
                "  zero + delta:           ",
                type,
                true,
                SqlTypeName.Limit.UNDERFLOW,
                false);
            printLimit(
                pw,
                "  max:                    ",
                type,
                true,
                SqlTypeName.Limit.OVERFLOW,
                false);
            printLimit(
                pw,
                "  max + epsilon:          ",
                type,
                true,
                SqlTypeName.Limit.OVERFLOW,
                true);
            pw.println();
        }
        pw.flush();
        getDiffRepos().assertEquals("output", "${output}", sw.toString());
    }

    private void printLimit(
        PrintWriter pw,
        String desc,
        BasicSqlType type,
        boolean sign,
        SqlTypeName.Limit limit,
        boolean beyond)
    {
        Object o = type.getLimit(sign, limit, beyond);
        if (o == null) {
            return;
        }
        pw.print(desc);
        String s;
        if (o instanceof byte []) {
            int k = 0;
            StringBuilder buf = new StringBuilder("{");
            for (byte b : (byte []) o) {
                if (k++ > 0) {
                    buf.append(", ");
                }
                buf.append(Integer.toHexString(b & 0xff));
            }
            buf.append("}");
            s = buf.toString();
        } else if (o instanceof Calendar) {
            Calendar calendar = (Calendar) o;
            DateFormat dateFormat;
            switch (type.getSqlTypeName()) {
            case DATE:
                dateFormat = DateFormat.getDateInstance();
                break;
            case TIME:
                dateFormat = DateFormat.getTimeInstance();
                break;
            default:
                dateFormat = DateFormat.getDateTimeInstance();
                break;
            }
            dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            s = dateFormat.format(calendar.getTime());
        } else {
            s = o.toString();
        }
        pw.print(s);
        SqlLiteral literal =
            type.getSqlTypeName().createLiteral(o, SqlParserPos.ZERO);
        pw.print("; as SQL: ");
        pw.print(literal.toSqlString(SqlUtil.dummyDialect));
        pw.println();
    }
}

// End SqlLimitsTest.java
