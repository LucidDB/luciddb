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

/**
 * Row expression.
 *
 * <p>Every row-expression has a type. (Compare with
 * {@link net.sf.saffron.sql.SqlNode}, which is created before validation, and
 * therefore types may not be available.)</p>
 *
 * <p>Some common row-expressions are:
 * {@link RexLiteral} (constant value),
 * {@link RexVariable} (variable),
 * {@link RexCall} (call to operator with operands). Expressions are generally
 * created using a {@link RexBuilder} factory.</p>
 * 
 * @author jhyde
 * @since Nov 22, 2003
 * @version $Id$
 **/
public abstract class RexNode {
    protected String digest;

    public abstract SaffronType getType();

    public abstract Object clone();

    /**
     * Returns whether this expression always returns true. (Such as if this
     * expression is equal to the literal <code>TRUE</code>.)
     */
    public boolean isAlwaysTrue() {
        return false;
    }

    public boolean isA(RexKind kind) {
        return getKind() == kind ||
                kind.includes(getKind());
    }

    /**
     * Returns the kind of node this is.
     *
     * @return A {@link RexKind} value, never null
     * @post return != null
     */
    public RexKind getKind() {
        return RexKind.Other;
    }

    public String toString() {
        return digest;
    }
}

// End RexNode.java
