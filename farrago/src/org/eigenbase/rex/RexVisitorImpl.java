/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2002 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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

/**
 * Default implementation of {@link RexVisitor}, which visits each node but does
 * nothing while it's there.
 */
public class RexVisitorImpl<R>
    implements RexVisitor<R>
{
    //~ Instance fields --------------------------------------------------------

    protected final boolean deep;

    //~ Constructors -----------------------------------------------------------

    protected RexVisitorImpl(boolean deep)
    {
        this.deep = deep;
    }

    //~ Methods ----------------------------------------------------------------

    public R visitInputRef(RexInputRef inputRef)
    {
        return null;
    }

    public R visitLocalRef(RexLocalRef localRef)
    {
        return null;
    }

    public R visitLiteral(RexLiteral literal)
    {
        return null;
    }

    public R visitOver(RexOver over)
    {
        R r = visitCall(over);
        if (!deep) {
            return null;
        }
        final RexWindow window = over.getWindow();
        for (int i = 0; i < window.orderKeys.length; i++) {
            window.orderKeys[i].accept(this);
        }
        for (int i = 0; i < window.partitionKeys.length; i++) {
            window.partitionKeys[i].accept(this);
        }
        return r;
    }

    public R visitCorrelVariable(RexCorrelVariable correlVariable)
    {
        return null;
    }

    public R visitCall(RexCall call)
    {
        if (!deep) {
            return null;
        }

        final RexNode [] operands = call.getOperands();
        R r = null;
        for (int i = 0; i < operands.length; i++) {
            RexNode operand = operands[i];
            r = operand.accept(this);
        }
        return r;
    }

    public R visitDynamicParam(RexDynamicParam dynamicParam)
    {
        return null;
    }

    public R visitRangeRef(RexRangeRef rangeRef)
    {
        return null;
    }

    public R visitFieldAccess(RexFieldAccess fieldAccess)
    {
        if (!deep) {
            return null;
        }
        final RexNode expr = fieldAccess.getReferenceExpr();
        return expr.accept(this);
    }

    /**
     * <p>Visits an array of expressions, returning the logical 'and' of their
     * results.
     *
     * <p>If any of them returns false, returns false immediately; if they all
     * return true, returns true.
     *
     * @see #visitArrayOr
     * @see RexShuttle#visitArray
     */
    public static boolean visitArrayAnd(
        RexVisitor<Boolean> visitor,
        RexNode [] exprs)
    {
        for (int i = 0; i < exprs.length; i++) {
            RexNode expr = exprs[i];
            final boolean b = expr.accept(visitor);
            if (!b) {
                return false;
            }
        }
        return true;
    }

    /**
     * <p>Visits an array of expressions, returning the logical 'or' of their
     * results.
     *
     * <p>If any of them returns true, returns true immediately; if they all
     * return false, returns false.
     *
     * @see #visitArrayAnd
     * @see RexShuttle#visitArray
     */
    public static boolean visitArrayOr(
        RexVisitor<Boolean> visitor,
        RexNode [] exprs)
    {
        for (int i = 0; i < exprs.length; i++) {
            RexNode expr = exprs[i];
            final boolean b = expr.accept(visitor);
            if (b) {
                return true;
            }
        }
        return false;
    }
}

// End RexVisitorImpl.java
