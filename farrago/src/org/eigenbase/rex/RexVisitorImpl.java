/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
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
 * Default implementation of {@link RexVisitor}, which visits each node
 * but does nothing while it's there.
 */
public class RexVisitorImpl implements RexVisitor
{
    protected final boolean deep;

    protected RexVisitorImpl(boolean deep)
    {
        this.deep = deep;
    }

    public void visitInputRef(RexInputRef inputRef)
    {
    }

    public void visitLiteral(RexLiteral literal)
    {
    }

    public void visitOver(RexOver over)
    {
        visitCall(over);
    }

    public void visitCorrelVariable(RexCorrelVariable correlVariable)
    {
    }

    public void visitCall(RexCall call)
    {
        if (!deep) {
            return;
        }

        final RexNode [] operands = call.getOperands();
        for (int i = 0; i < operands.length; i++) {
            RexNode operand = operands[i];
            operand.accept(this);
        }
    }

    public void visitDynamicParam(RexDynamicParam dynamicParam)
    {
    }

    public void visitRangeRef(RexRangeRef rangeRef)
    {
    }

    public void visitFieldAccess(RexFieldAccess fieldAccess)
    {
        if (!deep) {
            return;
        }
        final RexNode expr = fieldAccess.getReferenceExpr();
        expr.accept(this);
    }
}

// End RexVisitorImpl.java
