/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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
import org.eigenbase.sql.*;
import org.eigenbase.util.*;


/**
 * <code>RexCallBinding</code> implements {@link SqlOperatorBinding} by
 * referring to an underlying collection of {@link RexNode} operands.
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class RexCallBinding
    extends SqlOperatorBinding
{
    //~ Instance fields --------------------------------------------------------

    private final RexNode [] operands;

    //~ Constructors -----------------------------------------------------------

    public RexCallBinding(
        RelDataTypeFactory typeFactory,
        SqlOperator sqlOperator,
        RexNode [] operands)
    {
        super(typeFactory, sqlOperator);
        this.operands = operands;
    }

    //~ Methods ----------------------------------------------------------------

    // implement SqlOperatorBinding
    public String getStringLiteralOperand(int ordinal)
    {
        return RexLiteral.stringValue(operands[ordinal]);
    }

    // implement SqlOperatorBinding
    public int getIntLiteralOperand(int ordinal)
    {
        return RexLiteral.intValue(operands[ordinal]);
    }

    // implement SqlOperatorBinding
    public boolean isOperandNull(int ordinal, boolean allowCast)
    {
        return RexUtil.isNullLiteral(operands[ordinal], allowCast);
    }

    // implement SqlOperatorBinding
    public int getOperandCount()
    {
        return operands.length;
    }

    // implement SqlOperatorBinding
    public RelDataType getOperandType(int ordinal)
    {
        return operands[ordinal].getType();
    }
}

// End RexCallBinding.java
