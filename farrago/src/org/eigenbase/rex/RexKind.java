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

package org.eigenbase.rex;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.eigenbase.util.EnumeratedValues;


/**
 * Enumeration of some important types of row-expression.
 *
 * <p>The values are immutable, canonical constants, so you can use Kinds to
 * find particular types of expressions quickly. To identity a call to a
 * common operator such as '=', use {@link RexNode#isA}:<blockquote>
 *
 * <pre>exp.{@link RexNode#isA isA}({@link RexKind#Equals RexKind.Equals})</pre>
 *
 * </blockquote>To identify a category of expressions, you can use
 * {@link RexNode#isA} with an aggregate RexKind. The following expression will
 * return <code>true</code> for calls to '=' and '&gt;=', but <code>false</code>
 * for the constant '5', or a call to '+':<blockquote>
 *
 * <pre>exp.{@link RexNode#isA isA}({@link RexKind#Comparison RexKind.Comparison})</pre>
 *
 * </blockquote>To quickly choose between a number of options, use the
 * {@link #getOrdinal() ordinal} property:<blockquote>
 *
 * <pre>switch (exp.getKind().getOrdinal()) {
 * case {@link RexKind#EqualsORDINAL RexKind.Equals_ORDINAL}:
 *     ...;
 * case {@link RexKind#NotEqualsORDINAL RexKind.NotEquals_ORDINAL}:
 *     ...;
 * default:
 *     throw exp.getKind().{@link #unexpected unexpected}();
 * }</pre>
 *
 * </blockquote></p>
 *
 * @author jhyde
 * @since Nov 24, 2003
 * @version $Id$
 **/
public class RexKind extends EnumeratedValues.BasicValue
{
    //~ Static fields/initializers --------------------------------------------

    public static final int OtherORDINAL = 0;

    /** No operator in particular. This is the default kind. */
    public static final RexKind Other = new RexKind("Other", OtherORDINAL);
    public static final int EqualsORDINAL = 1;

    /** The equals operator, "=". */
    public static final RexKind Equals = new RexKind("Equals", EqualsORDINAL);
    public static final int NotEqualsORDINAL = 2;

    /** The not-equals operator, "&#33;=" or "&lt;&gt;". */
    public static final RexKind NotEquals =
        new RexKind("NotEquals", NotEqualsORDINAL);
    public static final int GreaterThanORDINAL = 3;

    /** The greater-than operator, "&gt;". */
    public static final RexKind GreaterThan =
        new RexKind("GreaterThan", GreaterThanORDINAL);
    public static final int GreaterEqualORDINAL = 4;

    /** The greater-than-or-equal operator, "&gt;=". */
    public static final RexKind GreaterThanOrEqual =
        new RexKind("GreaterThanOrEqual", GreaterEqualORDINAL);
    public static final int LessThanORDINAL = 5;

    /** The less-than operator, "&lt;". */
    public static final RexKind LessThan =
        new RexKind("LessThan", LessThanORDINAL);
    public static final int LessThanOrEqualORDINAL = 6;

    /** The less-than-or-equal operator, "&lt;=". */
    public static final RexKind LessThanOrEqual =
        new RexKind("LessThanOrEqual", LessThanOrEqualORDINAL);
    public static final int ComparisonORDINAL = 7;

    /** A comparison operator ({@link #Equals}, {@link #GreaterThan}, etc.).
     * Comparisons are always a {@link RexCall} with 2 arguments. */
    public static final RexKind Comparison =
        new RexKind("Comparison", ComparisonORDINAL,
            new RexKind [] {
                Equals, NotEquals, GreaterThan, GreaterThanOrEqual, LessThan,
                LessThanOrEqual
            });
    public static final int AndORDINAL = 10;

    /** The logical "AND" operator. */
    public static final RexKind And = new RexKind("And", AndORDINAL);
    public static final int OrORDINAL = 11;

    /** The logical "OR" operator. */
    public static final RexKind Or = new RexKind("Or", OrORDINAL);
    public static final int NotORDINAL = 12;

    /** The logical "NOT" operator. */
    public static final RexKind Not = new RexKind("Not", NotORDINAL);
    public static final int LogicalORDINAL = 13;

    /** A logical operator ({@link #And}, {@link #Or}, {@link #Not}). */
    public static final RexKind Logical =
        new RexKind("Logical", LogicalORDINAL, new RexKind [] { And, Or, Not });
    public static final int DivideORDINAL = 20;

    /** The arithmetic division operator, "/". */
    public static final RexKind Divide = new RexKind("Divide", DivideORDINAL);
    public static final int MinusORDINAL = 21;

    /** The arithmetic minus operator, "-".
     * @see #MinusPrefix */
    public static final RexKind Minus = new RexKind("Minus", MinusORDINAL);
    public static final int PlusORDINAL = 22;

    /** The arithmetic plus operator, "+". */
    public static final RexKind Plus = new RexKind("Plus", PlusORDINAL);
    public static final int MinusPrefixORDINAL = 23;

    /** The unary minus operator, as in "-1".
     * @see #Minus */
    public static final RexKind MinusPrefix =
        new RexKind("MinusPrefix", MinusPrefixORDINAL);
    public static final int TimesORDINAL = 24;

    /** The arithmetic multiplication operator, "*". */
    public static final RexKind Times = new RexKind("Times", TimesORDINAL);
    public static final int ArithmeticORDINAL = 25;

    /** An arithmetic operator ({@link #Divide}, {@link #Minus},
     * {@link #MinusPrefix}, {@link #Plus}, {@link #Times}). */
    public static final RexKind Arithmetic =
        new RexKind("Arithmetic", ArithmeticORDINAL,
            new RexKind [] { Divide, Minus, MinusPrefix, Plus, Times });
    public static final int FieldAccessORDINAL = 30;

    /** The arithmetic multiplication operator, "*". */
    public static final RexKind FieldAccess =
        new RexKind("FieldAccess", FieldAccessORDINAL);
    public static final int ConcatORDINAL = 31;

    /** The string concatenation operator, "||". */
    public static final RexKind Concat = new RexKind("Concat", ConcatORDINAL);
    public static final int SubstrORDINAL = 32;

    /** The substring function. */

    // REVIEW (jhyde, 2004/1/26) We should obsolete Substr. RexKind values are
    // so that the validator and optimizer can quickly recognize special
    // syntactic cetegories, and there's nothing particularly special about
    // Substr. For the mapping of sql->rex, and rex->calc, just use its name
    // or signature.
    public static final RexKind Substr = new RexKind("SUBSTR", SubstrORDINAL);
    public static final int RowORDINAL = 33;

    /** The row constructor operator. */
    public static final RexKind Row = new RexKind("Row", RowORDINAL);
    public static final int IsNullORDINAL = 34;

    /** The IS NULL operator. */
    public static final RexKind IsNull = new RexKind("IsNull", IsNullORDINAL);
    public static final int IdentifierORDINAL = 35;

    /** An identifier. */
    public static final RexKind Identifier =
        new RexKind("Identifier", IdentifierORDINAL);
    public static final int LiteralORDINAL = 36;

    /** A literal. */
    public static final RexKind Literal =
        new RexKind("Literal", LiteralORDINAL);
    public static final int ValuesORDINAL = 37;

    /** The VALUES operator. */
    public static final RexKind Values = new RexKind("Values", ValuesORDINAL);
    public static final int IsTrueORDINAL = 38;

    /** The IS TRUE operator. */
    public static final RexKind IsTrue = new RexKind("IsTrue", IsTrueORDINAL);
    public static final int IsFalseORDINAL = 39;

    /** The IS FALSE operator. */
    public static final RexKind IsFalse =
        new RexKind("IsFalse", IsFalseORDINAL);
    public static final int DynamicParamORDINAL = 40;

    /** A dynamic parameter. */
    public static final RexKind DynamicParam =
        new RexKind("DynamicParam", DynamicParamORDINAL);
    public static final int CastOrdinal = 41;
    public static final RexKind Cast = new RexKind("Cast", CastOrdinal);
    public static final int TrimOrdinal = 42;
    public static final RexKind Trim = new RexKind("Trim", TrimOrdinal);

    /** The LIKE operator */
    public static final int LikeOrdinal = 43;
    public static final RexKind Like = new RexKind("LIKE", LikeOrdinal);

    /** The SIMILAR operator */
    public static final int SimilarOrdinal = 44;
    public static final RexKind Similar =
        new RexKind("SIMILAR TO", SimilarOrdinal);

    /** The MULTISET operator */
    public static final int MultisetOrdinal = 45;
    public static final RexKind Multiset =
        new RexKind("MULTISET", MultisetOrdinal);

    /**
     * Set of all {@link RexKind} instances.
     */
    public static final EnumeratedValues enumeration =
        new EnumeratedValues(new RexKind [] {
                Equals, NotEquals, GreaterThan, GreaterThanOrEqual, LessThan,
                LessThanOrEqual, Comparison, IsNull, IsTrue, IsFalse, // comparisons
            And, Or, Not, Logical, // logical
            Divide, Minus, Plus, MinusPrefix, Times, Arithmetic, // arithmetic
            FieldAccess, Concat, Substr, Row, Identifier, Literal,
                Values, DynamicParam, Cast, Trim, Multiset
            });

    //~ Instance fields -------------------------------------------------------

    private Set otherKinds;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a kind.
     */
    private RexKind(
        String name,
        int ordinal)
    {
        super(name, ordinal, null);
        otherKinds = Collections.EMPTY_SET;
    }

    /**
     * Creates a kind which includes other kinds.
     */
    private RexKind(
        String name,
        int ordinal,
        RexKind [] others)
    {
        super(name, ordinal, null);
        otherKinds = new HashSet();
        for (int i = 0; i < others.length; i++) {
            RexKind other = others[i];
            otherKinds.add(other);
            otherKinds.add(other.otherKinds);
        }
    }

    //~ Methods ---------------------------------------------------------------

    public boolean includes(RexKind kind)
    {
        return (kind == this) || otherKinds.contains(kind);
    }

    /**
     * Looks up a kind from its ordinal.
     */
    public static RexKind get(int ordinal)
    {
        return (RexKind) enumeration.getValue(ordinal);
    }

    /**
     * Looks up a kind from its name.
     */
    public static RexKind get(String name)
    {
        return (RexKind) enumeration.getValue(name);
    }
}


// End RexKind.java
