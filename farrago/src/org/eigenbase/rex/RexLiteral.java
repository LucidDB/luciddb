/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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

package org.eigenbase.rex;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.util.Calendar;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.SqlSymbol;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.BitString;
import org.eigenbase.util.EnumeratedValues;
import org.eigenbase.util.NlsString;
import org.eigenbase.util.Util;


/**
 * Constant value in a row-expression.
 *
 * <p>There are several methods for creating literals in {@link RexBuilder}:
 * {@link RexBuilder#makeLiteral(boolean)} and so forth.</p>
 *
 *
 * <p>How is the value stored? In that respect, the class is somewhat of a
 * black box. There is a {@link #getValue} method which returns the value as
 * an object, but the type of that value is implementation detail, and it is
 * best that your code does not depend upon that knowledge. It is better to
 * use task-oriented methods such as {@link #getValue2} and
 * {@link #toJavaString}.</p>
 *
 * <p>The allowable types and combinations are:
 * <table>
 * <tr>
 *   <th>TypeName</th>
 *   <th>Meaing</th>
 *   <th>Value type</th>
 * </tr>
 * <tr>
 *   <td>{@link SqlTypeName#Null}</td>
 *   <td>The null value. It has its own special type.</td>
 *   <td>null</td>
 * </tr>
 * <tr>
 *   <td>{@link SqlTypeName#Boolean}</td>
 *   <td>Boolean, namely <code>TRUE</code>,
 *       <code>FALSE</code> or
 *       <code>UNKNOWN</code>.</td>
 *   <td>{@link Boolean}, or null represents the UNKNOWN value</td>
 * </tr>
 * <tr>
 *   <td>{@link SqlTypeName#Decimal}</td>
 *   <td>Exact number, for example <code>0</code>,
 *       <code>-.5</code>,
 *       <code>12345</code>.</td>
 *   <td>{@link BigDecimal}</td>
 * </tr>
 * <tr>
 *   <td>{@link SqlTypeName#Double}</td>
 *   <td>Approximate number, for example <code>6.023E-23</code>.</td>
 *   <td>{@link BigDecimal}</td>
 * </tr>
 * <tr>
 *   <td>{@link SqlTypeName#Date}</td>
 *   <td>Date, for example <code>DATE '1969-04'29'</code></td>
 *   <td>{@link Calendar}</td>
 * </tr>
 * <tr>
 *   <td>{@link SqlTypeName#Time}</td>
 *   <td>Time, for example <code>TIME '18:37:42.567'</code></td>
 *   <td>{@link Calendar}</td>
 * </tr>
 * <tr>
 *   <td>{@link SqlTypeName#Timestamp}</td>
 *   <td>Timestamp, for example
 *       <code>TIMESTAMP '1969-04-29 18:37:42.567'</code></td>
 *   <td>{@link Calendar}</td>
 * </tr>
 * <tr>
 *   <td>{@link SqlTypeName#Char}</td>
 *   <td>Character constant, for example
 *       <code>'Hello, world!'</code>,
 *       <code>''</code>,
 *       <code>_N'Bonjour'</code>,
 *       <code>_ISO-8859-1'It''s superman!' COLLATE SHIFT_JIS$ja_JP$2</code>.
 *       These are always CHAR, never VARCHAR.</td>
 *   <td>{@link NlsString}</td>
 * </tr>
 * <tr>
 *   <td>{@link SqlTypeName#Binary}</td>
 *   <td>Binary constant, for example <code>X'7F34'</code>. (The number of
 *       hexits must be even; see above.)
 *       These constants are always BINARY, never VARBINARY.</td>
 *   <td><code>byte[]</code> or {@link BitString}</td>
 * </tr>
 * <tr>
 *   <td>{@link SqlTypeName#Symbol}</td>
 *   <td>A symbol is a special type used to make parsing easier; it is not
 *       part of the SQL standard, and is not exposed to end-users.
 *       It is used to hold a flag, such as the LEADING flag in a call to the
 *       function <code>TRIM([LEADING|TRAILING|BOTH] chars FROM string)</code>.
 *   </td>
 *   <td><{@link SqlSymbol} (which conveniently both extends
 *       {@link org.eigenbase.sql.SqlLiteral}
 *       and also implements {@link EnumeratedValues}), or a class derived from
 *       it.</td>
 * </tr>
 * </table>
 *
 * @author jhyde
 * @since Nov 24, 2003
 * @version $Id$
 **/
public class RexLiteral extends RexNode
{
    //~ Instance fields -------------------------------------------------------

    /**
     * The value of this literal. Must be consistent with its type, as per
     * {@link #valueMatchesType}. For example, you can't store an
     * {@link Integer} value here just because you feel like it -- all numbers
     * are represented by a {@link BigDecimal}. But since this field is
     * private, it doesn't really matter how the values are stored.
     */
    private final Object value;

    /**
     * The real type of this literal, as reported by {@link #getType}.
     */
    private final RelDataType type;

    /**
     * An indication of the broad type of this literal -- even if its type
     * isn't a SQL type. Sometimes this will be different than the SQL type;
     * for example, all exact numbers, including integers have typeName
     * {@link SqlTypeName#Decimal}. See {@link #valueMatchesType} for the
     * definitive story.
     */
    public final SqlTypeName typeName;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a <code>RexLiteral</code>.
     *
     * @pre type != null
     * @pre valueMatchesType(value,typeName)
     * @pre (value == null) == type.isNullable()
     */
    RexLiteral(
        Object value,
        RelDataType type,
        SqlTypeName typeName)
    {
        Util.pre(type != null, "type != null");
        Util.pre(
            valueMatchesType(value, typeName),
            "valueMatchesType(value,typeName)");
        Util.pre((value == null) == type.isNullable(),
            "(value == null) == type.isNullable()");
        this.value = value;
        this.type = type;
        this.typeName = typeName;
        this.digest = toJavaString(value, typeName);
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Whether value is appropriate for its type. (We have rules about these
     * things.)
     */
    public static boolean valueMatchesType(
        Object value,
        SqlTypeName typeName)
    {
        switch (typeName.ordinal) {
        case SqlTypeName.Boolean_ordinal:

            // Unlike SqlLiteral, we do not allow boolean null.
            return value instanceof Boolean;
        case SqlTypeName.Null_ordinal:
            return value == null;
        case SqlTypeName.Decimal_ordinal:
        case SqlTypeName.Double_ordinal:
            return value instanceof BigDecimal;
        case SqlTypeName.Date_ordinal:
        case SqlTypeName.Time_ordinal:
        case SqlTypeName.Timestamp_ordinal:
            return value instanceof Calendar;
        case SqlTypeName.Binary_ordinal:
            return value instanceof byte [];
        case SqlTypeName.Char_ordinal:
            return value instanceof NlsString;
        case SqlTypeName.Symbol_ordinal:

            // Unlike SqlLiteral, we DO allow a String value.
            return value instanceof SqlSymbol || value instanceof String;
        case SqlTypeName.Integer_ordinal: // not allowed -- use Decimal
        case SqlTypeName.Varchar_ordinal: // not allowed -- use Char
        case SqlTypeName.Varbinary_ordinal: // not allowed -- use Binary
        default:
            throw typeName.unexpected();
        }
    }

    private static String toJavaString(
        Object value,
        SqlTypeName typeName)
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        printAsJava(value, pw, typeName, false);
        pw.flush();
        return sw.toString();
    }

    /**
     * Prints the value this literal as a Java string constant.
     */
    public void printAsJava(PrintWriter pw)
    {
        printAsJava(value, pw, typeName, true);
    }

    /**
     * Prints a value as a Java string. The value must be consistent with
     * the type, as per {@link #valueMatchesType}.
     *
     * <p>Typical return values:<ul>
     * <li>true</li>
     * <li>null</li>
     * <li>"Hello, world!"</li>
     * <li>1.25</li>
     * <li>1234ABCD</li>
     * </ul>
     *
     * @param value Value
     * @param pw Writer to write to
     * @param typeName Type family
     */
    private static void printAsJava(
        Object value,
        PrintWriter pw,
        SqlTypeName typeName,
        boolean java)
    {
        switch (typeName.ordinal) {
        case SqlTypeName.Char_ordinal:
            NlsString nlsString = (NlsString) value;
            if (java) {
                Util.printJavaString(
                    pw,
                    nlsString.getValue(),
                    true);
            } else {
                pw.print(nlsString.toString());
            }
            break;
        case SqlTypeName.Boolean_ordinal:
            assert value instanceof Boolean;
            pw.print(((Boolean) value).booleanValue() ? "true" : "false");
            break;
        case SqlTypeName.Decimal_ordinal:
            assert value instanceof BigDecimal;
            pw.print(value.toString());
            break;
        case SqlTypeName.Double_ordinal:
            assert value instanceof BigDecimal;
            pw.print(Util.toScientificNotation((BigDecimal) value));
            break;
        case SqlTypeName.Binary_ordinal:
            assert value instanceof byte [];
            pw.print(Util.toStringFromByteArray((byte []) value, 16));
            break;
        case SqlTypeName.Null_ordinal:
            assert value == null;
            pw.print("null");
            break;
        case SqlTypeName.Symbol_ordinal:
            assert value instanceof SqlSymbol;
            pw.print("FLAG(");
            pw.print(value);
            pw.print(")");
            break;
        case SqlTypeName.Date_ordinal:
        case SqlTypeName.Time_ordinal:
        case SqlTypeName.Timestamp_ordinal:
            assert value instanceof Calendar;
            pw.print(value.toString());
            break;
        default:
            Util.pre(
                valueMatchesType(value, typeName),
                "valueMatchesType(value, typeName)");
            throw Util.needToImplement(typeName);
        }
    }

    public RelDataType getType()
    {
        return type;
    }

    public RexKind getKind()
    {
        return RexKind.Literal;
    }

    /**
     * Returns the value of this literal.
     *
     * @post valueMatchesType(return, typeName)
     */
    public Object getValue()
    {
        assert valueMatchesType(value, typeName) : value;
        return value;
    }

    /**
     * Returns the value of this literal, in the form that the calculator
     * program builder wants it.
     */
    public Object getValue2()
    {
        switch (typeName.ordinal) {
        case SqlTypeName.Char_ordinal:
            return ((NlsString) value).getValue();
        case SqlTypeName.Date_ordinal:
        case SqlTypeName.Time_ordinal:
        case SqlTypeName.Timestamp_ordinal:
            return new Long(((Calendar) value).getTimeInMillis());
        default:
            return value;
        }
    }

    public static boolean booleanValue(RexNode node)
    {
        return ((Boolean) ((RexLiteral) node).value).booleanValue();
    }

    public boolean equals(Object obj)
    {
        return (obj instanceof RexLiteral)
            && equals(((RexLiteral) obj).value, value);
    }

    public int hashCode()
    {
        return (value == null) ? 0 : value.hashCode();
    }

    public static int intValue(RexNode node)
    {
        return ((Number) ((RexLiteral) node).value).intValue();
    }

    public static String stringValue(RexNode node)
    {
        return ((NlsString) ((RexLiteral) node).value).getValue();
    }

    public static boolean isNullLiteral(RexNode node)
    {
        return node instanceof RexLiteral
            && (((RexLiteral) node).value == null);
    }

    public Object clone()
    {
        return new RexLiteral(value, type, typeName);
    }

    private static boolean equals(
        Object o1,
        Object o2)
    {
        return (o1 == null) ? (o2 == null) : o1.equals(o2);
    }

    public void accept(RexVisitor visitor)
    {
        visitor.visitLiteral(this);
    }
}


// End RexLiteral.java
