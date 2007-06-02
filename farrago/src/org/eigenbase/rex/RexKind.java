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
package org.eigenbase.rex;

import java.util.*;


/**
 * Enumeration of some important types of row-expression.
 *
 * <p>The values are immutable, canonical constants, so you can use Kinds to
 * find particular types of expressions quickly. To identity a call to a common
 * operator such as '=', use {@link RexNode#isA}:
 *
 * <blockquote>
 * <pre>exp.{@link RexNode#isA isA}({@link RexKind#Equals RexKind.Equals})</pre>
 * </blockquote>
 *
 * To identify a category of expressions, you can use {@link RexNode#isA} with
 * an aggregate RexKind. The following expression will return <code>true</code>
 * for calls to '=' and '&gt;=', but <code>false</code> for the constant '5', or
 * a call to '+':
 *
 * <blockquote>
 * <pre>exp.{@link RexNode#isA isA}({@link RexKind#Comparison RexKind.Comparison})</pre>
 * </blockquote>
 *
 * To quickly choose between a number of options, use a switch statement:
 *
 * <blockquote>
 * <pre>switch (exp.getKind()) {
 * case {@link #Equals}:
 *     ...;
 * case {@link #NotEquals}:
 *     ...;
 * default:
 *     throw {@link org.eigenbase.util.Util#unexpected Util.unexpected}(exp.getKind());
 * }</pre>
 * </blockquote>
 * </p>
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 24, 2003
 */
public enum RexKind
{
    /**
     * No operator in particular. This is the default kind.
     */
    Other,

    /**
     * The equals operator, "=".
     */
    Equals,

    /**
     * The not-equals operator, "&#33;=" or "&lt;&gt;".
     */
    NotEquals,

    /**
     * The greater-than operator, "&gt;".
     */
    GreaterThan,

    /**
     * The greater-than-or-equal operator, "&gt;=".
     */
    GreaterThanOrEqual,

    /**
     * The less-than operator, "&lt;".
     */
    LessThan,

    /**
     * The less-than-or-equal operator, "&lt;=".
     */
    LessThanOrEqual,

    /**
     * A comparison operator ({@link #Equals}, {@link #GreaterThan}, etc.).
     * Comparisons are always a {@link RexCall} with 2 arguments.
     */
    Comparison(
        new RexKind[] {
            Equals, NotEquals, GreaterThan, GreaterThanOrEqual, LessThan,
            LessThanOrEqual
        }),

    /**
     * The logical "AND" operator.
     */
    And,

    /**
     * The logical "OR" operator.
     */
    Or,

    /**
     * The logical "NOT" operator.
     */
    Not,

    /**
     * A logical operator ({@link #And}, {@link #Or}, {@link #Not}).
     */
    Logical(new RexKind[] { And, Or, Not }),

    /**
     * The arithmetic division operator, "/".
     */
    Divide,

    /**
     * The arithmetic minus operator, "-".
     *
     * @see #MinusPrefix
     */
    Minus,

    /**
     * The arithmetic plus operator, "+".
     */
    Plus,

    /**
     * The unary minus operator, as in "-1".
     *
     * @see #Minus
     */
    MinusPrefix,

    /**
     * The arithmetic multiplication operator, "*".
     */
    Times,

    /**
     * An arithmetic operator ({@link #Divide}, {@link #Minus},
     * {@link #MinusPrefix}, {@link #Plus}, {@link #Times}).
     */
    Arithmetic(new RexKind[] { Divide, Minus, MinusPrefix, Plus, Times }),

    /**
     * The field access operator, ".".
     */
    FieldAccess,

    /**
     * The string concatenation operator, "||".
     */
    Concat,

    /**
     * The substring function.
     */

    // REVIEW (jhyde, 2004/1/26) We should obsolete Substr. RexKind values are
    // so that the validator and optimizer can quickly recognize special
    // syntactic cetegories, and there's nothing particularly special about
    // Substr. For the mapping of sql->rex, and rex->calc, just use its name or
    // signature.
    Substr,

    /**
     * The row constructor operator.
     */
    Row,

    /**
     * The IS NULL operator.
     */
    IsNull,

    /**
     * An identifier.
     */
    Identifier,

    /**
     * A literal.
     */
    Literal,

    /**
     * The VALUES operator.
     */
    Values,

    /**
     * The IS TRUE operator.
     */
    IsTrue,

    /**
     * The IS FALSE operator.
     */
    IsFalse,

    /**
     * A dynamic parameter.
     */
    DynamicParam, Cast, Trim,

    /**
     * The LIKE operator.
     */
    Like,

    /**
     * The SIMILAR operator.
     */
    Similar,

    /**
     * The MULTISET Query Constructor
     */
    MultisetQueryConstructor,

    /**
     * NEW invocation
     */
    NewSpecification,

    /**
     * The internal REINTERPRET operator
     */
    Reinterpret;

    private final Set<RexKind> otherKinds;

    /**
     * Creates a kind.
     */
    RexKind()
    {
        otherKinds = Collections.emptySet();
    }

    /**
     * Creates a kind which includes other kinds.
     */
    RexKind(RexKind [] others)
    {
        otherKinds = new HashSet<RexKind>();
        for (int i = 0; i < others.length; i++) {
            RexKind other = others[i];
            otherKinds.add(other);
            otherKinds.addAll(other.otherKinds);
        }
    }

    public boolean includes(RexKind kind)
    {
        return (kind == this) || otherKinds.contains(kind);
    }
}

// End RexKind.java
