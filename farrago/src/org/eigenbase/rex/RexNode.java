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
package org.eigenbase.rex;

import org.eigenbase.reltype.*;


/**
 * Row expression.
 *
 * <p>Every row-expression has a type. (Compare with {@link
 * org.eigenbase.sql.SqlNode}, which is created before validation, and therefore
 * types may not be available.)</p>
 *
 * <p>Some common row-expressions are: {@link RexLiteral} (constant value),
 * {@link RexVariable} (variable), {@link RexCall} (call to operator with
 * operands). Expressions are generally created using a {@link RexBuilder}
 * factory.</p>
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 22, 2003
 */
public abstract class RexNode
{
    //~ Static fields/initializers ---------------------------------------------

    public static final RexNode [] EMPTY_ARRAY = new RexNode[0];

    //~ Instance fields --------------------------------------------------------

    protected String digest;

    //~ Methods ----------------------------------------------------------------

    public abstract RelDataType getType();

    public abstract RexNode clone();

    /**
     * Returns whether this expression always returns true. (Such as if this
     * expression is equal to the literal <code>TRUE</code>.)
     */
    public boolean isAlwaysTrue()
    {
        return false;
    }

    public boolean isA(RexKind kind)
    {
        return (getKind() == kind) || kind.includes(getKind());
    }

    /**
     * Returns the kind of node this is.
     *
     * @return A {@link RexKind} value, never null
     *
     * @post return != null
     */
    public RexKind getKind()
    {
        return RexKind.Other;
    }

    public String toString()
    {
        return digest;
    }

    /**
     * Accepts a visitor, dispatching to the right overloaded {@link
     * RexVisitor#visitInputRef visitXxx} method.
     *
     * <p>Also see {@link RexProgram#apply(RexVisitor, RexNode[], RexNode)},
     * which applies a visitor to several expressions simultaneously.
     */
    public abstract <R> R accept(RexVisitor<R> visitor);
}

// End RexNode.java
