/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2004-2004 Disruptive Tech
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
import org.eigenbase.sql.fun.SqlWindowOperator;

/**
 * SQL window specifcation.
 *
 * <p>For example, the query
 *
 * <blockquote><pre>SELECT sum(a) OVER (w ROWS 3 PRECEDING)
 * FROM t
 * WINDOW w AS (PARTITION BY x, y ORDER BY z),
 *     w1 AS (w ROWS 5 PRECEDING UNBOUNDED FOLLOWING)</pre></blockquote>
 *
 * declares windows w and w1, and uses a window in an OVER clause.
 * It thus contains 3 {@link SqlWindow} objects.</p>
 */
public class SqlWindow extends SqlCall
{
    public static final int RefName_OPERAND = 0;
    public static final int PartitionList_OPERAND = 1;
    public static final int OrderList_OPERAND = 2;
    public static final int IsRows_OPERAND = 3;
    public static final int LowerBound_OPERAND = 4;
    public static final int UpperBound_OPERAND = 5;

    public SqlWindow(SqlWindowOperator operator, SqlNode[] operands,
            ParserPosition pos)
    {
        super(operator,operands,pos);
    }

    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        // Override, so we don't print extra parentheses. 
        operator.unparse(writer, operands, 0, 0);
    }

    SqlNode getLowerBound() {
        return operands[LowerBound_OPERAND];
    }

    SqlNode getUpperBound() {
        return operands[UpperBound_OPERAND];
    }

    boolean isRows() {
        return SqlLiteral.booleanValue(operands[IsRows_OPERAND]);
    }

    SqlNodeList getOrderList() {
        return (SqlNodeList) operands[OrderList_OPERAND];
    }

    SqlNodeList getPartitionList() {
        return (SqlNodeList) operands[PartitionList_OPERAND];
    }

    SqlIdentifier getRefName() {
        return (SqlIdentifier) operands[RefName_OPERAND];
    }

}

// End SqlWindow.java
