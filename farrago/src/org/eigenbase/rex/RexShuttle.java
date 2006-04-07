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

import java.util.List;


/**
 * Passes over a row-expression, calling a handler method for each node,
 * appropriate to the type of the node.
 *
 * <p>Like {@link RexVisitor}, this is an instance of the
 * {@link org.eigenbase.util.Glossary#VisitorPattern Visitor Pattern}.
 * Use <code>RexShuttle</code> if you would like your methods to return a
 * value.</p>
 *
 * @author jhyde
 * @since Nov 26, 2003
 * @version $Id$
 **/
public class RexShuttle
{
    //~ Methods ---------------------------------------------------------------

    public RexNode visitOver(RexOver over)
    {
        boolean[] update = {false};
        RexNode[] clonedOperands = visitArray(over.operands, update);
        RexWindow window = visitWindow(over.getWindow());
        if (update[0] || window != over.getWindow()) {
            // REVIEW jvs 8-Mar-2005:  This doesn't take into account
            // the fact that a rewrite may have changed the result type.
            // To do that, we would need to take a RexBuilder and
            // watch out for special operators like CAST and NEW where
            // the type is embedded in the original call.
            return new RexOver(
                over.getType(),
                over.getAggOperator(),
                clonedOperands,
                window);
        } else {
            return over;
        }
    }

    public RexWindow visitWindow(RexWindow window)
    {
        boolean[] update = {false};
        RexNode[] clonedOrderKeys = visitArray(window.orderKeys, update);
        RexNode[] clonedPartitionKeys = visitArray(window.partitionKeys, update);
        if (update[0]) {
            return new RexWindow(
                clonedPartitionKeys,
                clonedOrderKeys,
                window.getLowerBound(),
                window.getUpperBound(),
                window.isRows());
        } else {
            return window;
        }
    }

    public RexNode visitCall(final RexCall call)
    {
        boolean[] update = {false};
        RexNode[] clonedOperands = visitArray(call.operands, update);
        if (update[0]) {
            // REVIEW jvs 8-Mar-2005:  This doesn't take into account
            // the fact that a rewrite may have changed the result type.
            // To do that, we would need to take a RexBuilder and
            // watch out for special operators like CAST and NEW where
            // the type is embedded in the original call.
            return new RexCall(
                call.getType(),
                call.getOperator(),
                clonedOperands);
        } else {
            return call;
        }
    }

    protected RexNode[] visitArray(RexNode[] exprs, boolean[] update)
    {
        RexNode[] clonedOperands = new RexNode[exprs.length];
        for (int i = 0; i < exprs.length; i++) {
            RexNode operand = exprs[i];
            RexNode clonedOperand = operand.accept(this);
            if (clonedOperand != operand) {
                update[0] = true;
            }
            clonedOperands[i] = clonedOperand;
        }
        return clonedOperands;
    }

    public RexNode visitCorrelVariable(RexCorrelVariable variable)
    {
        return variable;
    }

    public RexNode visitFieldAccess(RexFieldAccess fieldAccess)
    {
        RexNode before = fieldAccess.getReferenceExpr();
        RexNode after = before.accept(this);

        if (before == after) {
            return fieldAccess;
        } else {
            return new RexFieldAccess(
                after,
                fieldAccess.getField());
        }
    }

    public RexNode visitInputRef(RexInputRef inputRef)
    {
        return inputRef;
    }

    public RexNode visitLocalRef(RexLocalRef localRef)
    {
        return localRef;
    }

    public RexNode visitLiteral(RexLiteral literal)
    {
        return literal;
    }

    public RexNode visitDynamicParam(RexDynamicParam dynamicParam)
    {
        return dynamicParam;
    }

    public RexNode visitRangeRef(RexRangeRef rangeRef)
    {
        return rangeRef;
    }

    /**
     * Applies this shuttle to each expression in a list.
     *
     * @return whether any of the expressions changed
     */
    public final <T extends RexNode> boolean apply(List<T> exprList)
    {
        int changeCount = 0;
        for (int i = 0; i < exprList.size(); i++) {
            T expr = exprList.get(i);
            T expr2 = (T) expr.accept(this);
            if (expr != expr2) {
                ++changeCount;
                exprList.set(i, expr2);
            }
        }
        return changeCount > 0;
    }

    /**
     * Applies this shuttle to an expression, or returns null if the expression
     * is null.
     */
    public final RexNode apply(RexNode expr)
    {
        return expr == null ?
            null :
            expr.accept(this);
    }
}

// End RexShuttle.java
