/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 SQLstream, Inc.
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
package org.eigenbase.sql;

import org.eigenbase.sql.parser.*;


/**
 * A <code>SqlJoin</code> is ...
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 29, 2003
 */
public class SqlJoin
    extends SqlCall
{
    //~ Static fields/initializers ---------------------------------------------

    public static final int LEFT_OPERAND = 0;

    /**
     * Operand says whether this is a natural join. Must be constant TRUE or
     * FALSE.
     */
    public static final int IS_NATURAL_OPERAND = 1;

    /**
     * Value must be a {@link SqlLiteral}, one of the integer codes for {@link
     * SqlJoinOperator.JoinType}.
     */
    public static final int TYPE_OPERAND = 2;
    public static final int RIGHT_OPERAND = 3;

    /**
     * Value must be a {@link SqlLiteral}, one of the integer codes for {@link
     * SqlJoinOperator.ConditionType}.
     */
    public static final int CONDITION_TYPE_OPERAND = 4;
    public static final int CONDITION_OPERAND = 5;

    //~ Constructors -----------------------------------------------------------

    public SqlJoin(
        SqlJoinOperator operator,
        SqlNode [] operands,
        SqlParserPos pos)
    {
        super(operator, operands, pos);
    }

    //~ Methods ----------------------------------------------------------------

    public final SqlNode getCondition()
    {
        return operands[CONDITION_OPERAND];
    }

    /**
     * Returns a {@link SqlJoinOperator.ConditionType}
     *
     * @post return != null
     */
    public final SqlJoinOperator.ConditionType getConditionType()
    {
        return (SqlJoinOperator.ConditionType) SqlLiteral.symbolValue(
            operands[CONDITION_TYPE_OPERAND]);
    }

    /**
     * Returns a {@link SqlJoinOperator.JoinType}
     *
     * @post return != null
     */
    public final SqlJoinOperator.JoinType getJoinType()
    {
        return (SqlJoinOperator.JoinType) SqlLiteral.symbolValue(
            operands[TYPE_OPERAND]);
    }

    public final SqlNode getLeft()
    {
        return operands[LEFT_OPERAND];
    }

    public final boolean isNatural()
    {
        return SqlLiteral.booleanValue(operands[IS_NATURAL_OPERAND]);
    }

    public final SqlNode getRight()
    {
        return operands[RIGHT_OPERAND];
    }
}

// End SqlJoin.java
