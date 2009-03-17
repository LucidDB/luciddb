/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2007-2007 The Eigenbase Project
// Copyright (C) 2007-2007 Disruptive Tech
// Copyright (C) 2007-2007 LucidEra, Inc.
// Portions Copyright (C) 2007-2007 John V. Sichi
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
 * Shuttle which creates a deep copy of a Rex expression.
 *
 * <p>This is useful when copying objects from one type factory or builder to
 * another.
 *
 * <p>Due to the laziness of the author, not all Rex types are supported at
 * present.
 *
 * @author jhyde
 * @version $Id$
 * @see RexBuilder#copy(RexNode)
 */
class RexCopier
    extends RexShuttle
{
    //~ Instance fields --------------------------------------------------------

    private final RexBuilder builder;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a RexCopier.
     *
     * @param builder Builder
     */
    RexCopier(RexBuilder builder)
    {
        this.builder = builder;
    }

    //~ Methods ----------------------------------------------------------------

    public RexNode visitOver(RexOver over)
    {
        throw new UnsupportedOperationException();
    }

    public RexWindow visitWindow(RexWindow window)
    {
        throw new UnsupportedOperationException();
    }

    public RexNode visitCall(final RexCall call)
    {
        return builder.makeCall(
            builder.getTypeFactory().copyType(call.getType()),
            call.getOperator(),
            visitArray(call.getOperands(), null));
    }

    public RexNode visitCorrelVariable(RexCorrelVariable variable)
    {
        throw new UnsupportedOperationException();
    }

    public RexNode visitFieldAccess(RexFieldAccess fieldAccess)
    {
        return builder.makeFieldAccess(
            fieldAccess.getReferenceExpr().accept(this),
            fieldAccess.getField().getIndex());
    }

    public RexNode visitInputRef(RexInputRef inputRef)
    {
        throw new UnsupportedOperationException();
    }

    public RexNode visitLocalRef(RexLocalRef localRef)
    {
        throw new UnsupportedOperationException();
    }

    public RexNode visitLiteral(RexLiteral literal)
    {
        return new RexLiteral(
            literal.getValue(),
            builder.getTypeFactory().copyType(literal.getType()),
            literal.getTypeName());
    }

    public RexNode visitDynamicParam(RexDynamicParam dynamicParam)
    {
        throw new UnsupportedOperationException();
    }

    public RexNode visitRangeRef(RexRangeRef rangeRef)
    {
        throw new UnsupportedOperationException();
    }
}

// End RexCopier.java
