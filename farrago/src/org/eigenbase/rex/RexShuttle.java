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


/**
 * Passes over a row-expression, calling a handler method for each node,
 * appropriate to the type of the node.
 *
 * <p>This is an instance of the
 * {@link org.eigenbase.util.Glossary#VisitorPattern Visitor Pattern}.</p>
 *
 * @author jhyde
 * @since Nov 26, 2003
 * @version $Id$
 **/
public class RexShuttle
{
    //~ Methods ---------------------------------------------------------------

    public RexNode visit(RexNode rex)
    {
        if (rex instanceof RexCall) {
            return visit((RexCall) rex);
        } else if (rex instanceof RexCorrelVariable) {
            return visit((RexCorrelVariable) rex);
        } else if (rex instanceof RexInputRef) {
            return visit((RexInputRef) rex);
        } else if (rex instanceof RexFieldAccess) {
            return visit((RexFieldAccess) rex);
        } else if (rex instanceof RexLiteral) {
            return visit((RexLiteral) rex);
        } else if (rex instanceof RexDynamicParam) {
            return visit((RexDynamicParam) rex);
        } else {
            return rex;
        }
    }

    public RexNode visit(final RexCall call)
    {
        RexNode [] operands = call.operands;
        for (int i = 0; i < operands.length; i++) {
            RexNode operand = operands[i];
            operands[i] = visit(operand);
        }
        return call;
    }

    public RexNode visit(RexCorrelVariable variable)
    {
        return variable;
    }

    public RexNode visit(RexFieldAccess fieldAccess)
    {
        RexNode before = fieldAccess.getReferenceExpr();
        RexNode after = visit(before);

        if (before == after) {
            return fieldAccess;
        } else {
            return new RexFieldAccess(
                after,
                fieldAccess.getField());
        }
    }

    public RexNode visit(RexInputRef input)
    {
        return input;
    }

    public RexNode visit(RexLiteral literal)
    {
        return literal;
    }

    public RexNode visit(RexDynamicParam dynamicParam)
    {
        return dynamicParam;
    }
}


// End RexShuttle.java
