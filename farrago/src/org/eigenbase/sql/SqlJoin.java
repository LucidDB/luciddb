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

package org.eigenbase.sql;

import org.eigenbase.sql.parser.ParserPosition;


/**
 * A <code>SqlJoin</code> is ...
 *
 * @author jhyde
 * @version $Id$
 *
 * @since Mar 29, 2003
 */
public class SqlJoin extends SqlCall
{
    //~ Constructors ----------------------------------------------------------

    public SqlJoin(
        SqlJoinOperator operator,
        SqlNode [] operands,
        ParserPosition pos)
    {
        super(operator, operands, pos);
    }

    //~ Methods ---------------------------------------------------------------

    public final SqlNode getCondition()
    {
        return operands[SqlJoinOperator.CONDITION_OPERAND];
    }

    /**
     * Returns a {@link SqlJoinOperator.ConditionType}
     *
     * @post return != null
     */
    public final SqlJoinOperator.ConditionType getConditionType()
    {
        final SqlLiteral literal =
            (SqlLiteral) operands[SqlJoinOperator.CONDITION_TYPE_OPERAND];
        return (SqlJoinOperator.ConditionType) literal.getValue();
    }

    /**
     * Returns a {@link SqlJoinOperator.JoinType}
     *
     * @post return != null
     */
    public final SqlJoinOperator.JoinType getJoinType()
    {
        final SqlLiteral literal =
            (SqlLiteral) operands[SqlJoinOperator.TYPE_OPERAND];
        return (SqlJoinOperator.JoinType) literal.getValue();
    }

    public final SqlNode getLeft()
    {
        return operands[SqlJoinOperator.LEFT_OPERAND];
    }

    public final boolean isNatural()
    {
        return SqlLiteral.booleanValue(operands[SqlJoinOperator.IS_NATURAL_OPERAND]);
    }

    public final SqlNode getRight()
    {
        return operands[SqlJoinOperator.RIGHT_OPERAND];
    }
}


// End SqlJoin.java
