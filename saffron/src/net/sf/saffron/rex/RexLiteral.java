/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2003-2003 Disruptive Technologies, Inc.
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
package net.sf.saffron.rex;

import net.sf.saffron.core.SaffronType;
import net.sf.saffron.sql.SqlLiteral;
import net.sf.saffron.sql.SqlFunctionTable;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigInteger;

/**
 * Constant value in a row-expression.
 *
 * <p>There are several methods for creating literals in {@link RexBuilder}:
 * {@link RexBuilder#makeLiteral(boolean)} and so forth.</p>
 *
 * @author jhyde
 * @since Nov 24, 2003
 * @version $Id$
 **/
public class RexLiteral extends RexNode
{
    //~ Static fields/initializers --------------------------------------------

    //~ Instance fields -------------------------------------------------------

    public final Object value;
    private final SaffronType type;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a <code>RexLiteral</code>.
     *
     * @pre value instanceof String ||
     *   value instanceof BigInteger ||
     *   value instanceof Boolean ||
     *   value instanceof Double ||
     *   value instanceof byte[] ||
     *   value instanceof SqlLiteral.BitString ||
     *   value instanceof SqlLiteral.StringLiteral ||
     *   value instanceof SqlFunctionTable.FunctionFlagType ||
     *   value == null
     */
    RexLiteral(Object value,SaffronType type)
    {
        assert supportedType(value) :
                value;
        assert type != null;
        this.value = value;
        this.type = type;
        this.digest = format(value, type);
    }

    /**
     * Returns true if this literal is a supported type.
     *
     * @post return true if value is
     *   instanceof String ||
     *    instanceof BigInteger ||
     *    instanceof Boolean ||
     *    value instanceof Double ||
     *    value instanceof byte[] ||
     *    value instanceof SqlLiteral.BitString ||
     *    value instanceof SqlLiteral.StringLiteral ||
     *    value instanceof SqlFunctionTable.FunctionFlagType ||
     *    value instanceof java.sql.Date ||
     *    value instanceof java.sql.Time ||
     *    value instanceof java.sql.Timestamp ||
     *    value == null
     */
    private boolean supportedType(Object value) {
        return value instanceof String ||
                value instanceof BigInteger ||
                value instanceof Boolean ||
                value instanceof Double ||
                value instanceof byte[] ||
                value instanceof SqlLiteral.BitString ||
                value instanceof SqlLiteral.StringLiteral ||
                value instanceof SqlFunctionTable.FunctionFlagType ||
                value instanceof java.sql.Date ||
                value instanceof java.sql.Time ||
                value instanceof java.sql.Timestamp ||
                value == null;
    }

    private static String format(Object value,SaffronType type) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        type.format(value, pw);
        pw.flush();
        return sw.toString();
    }

    //~ Methods ---------------------------------------------------------------

    public SaffronType getType() {
        return type;
    }

    public RexKind getKind() {
        return RexKind.Literal;
    }

    /**
     * Returns the value of this literal if its a supported type.
     *
     * @post return instanceof String ||
     *   return instanceof BigInteger ||
     *   return instanceof Boolean ||
     *   return value instanceof Double ||
     *   return value instanceof byte[] ||
     *   return value instanceof SqlLiteral.BitString ||
     *   return value instanceof SqlLiteral.StringLiteral ||
     *   return value instanceof SqlFunctionTable.FunctionFlagType ||
     *   return value instance
     *   return == null
     */
    public Object getValue()
    {
        assert supportedType(value) : value;
        return value;
    }

    public static boolean booleanValue(RexNode node)
    {
        return ((Boolean) ((RexLiteral) node).value).booleanValue();
    }

    public boolean equals(Object obj)
    {
        return (obj instanceof RexLiteral)
            && equals(((RexLiteral) obj).value,value);
    }

    public int hashCode()
    {
        return (value == null) ? 0 : value.hashCode();
    }

    public static int intValue(RexNode node)
    {
        return ((Integer) ((RexLiteral) node).value).intValue();
    }

    public static boolean isNullLiteral(RexNode node) {
        return node instanceof RexLiteral && ((RexLiteral) node).getValue()==null;
    }

    public Object clone()
    {
        return new RexLiteral(value,type);
    }

    private static boolean equals(Object o1,Object o2)
    {
        return (o1 == null) ? (o2 == null) : o1.equals(o2);
    }
}

// End RexLiteral.java
