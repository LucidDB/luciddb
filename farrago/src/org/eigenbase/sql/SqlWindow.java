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
import org.eigenbase.util.Util;

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
    /**
     * Ordinal of the operand which holds the name of the window being
     * declared.
     */
    public static final int DeclName_OPERAND = 0;
    /**
     * Ordinal of operand which holds the name of the window being referenced,
     * or null.
     */
    public static final int RefName_OPERAND = 1;
    /**
     * Ordinal of the operand which holds the list of partitioning columns.
     */
    public static final int PartitionList_OPERAND = 2;
    /**
     * Ordinal of the operand which holds the list of ordering columns.
     */
    public static final int OrderList_OPERAND = 3;
    /**
     * Ordinal of the operand which declares whether it is a physical (rows)
     * or logical (values) range.
     */
    public static final int IsRows_OPERAND = 4;
    /**
     * Ordinal of the operand which holds the lower bound of the window.
     */
    public static final int LowerBound_OPERAND = 5;
    /**
     * Ordinal of the operand which holds the upper bound of the window.
     */
    public static final int UpperBound_OPERAND = 6;

    public SqlWindow(SqlWindowOperator operator, SqlNode[] operands,
            ParserPosition pos)
    {
        super(operator,operands,pos);
        final SqlIdentifier declId = (SqlIdentifier) operands[DeclName_OPERAND];
        Util.pre(declId == null || declId.isSimple(), "declId.isSimple()");
    }

    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        // Override, so we don't print extra parentheses. 
        operator.unparse(writer, operands, 0, 0);
    }

    public SqlIdentifier getDeclName() {
        return (SqlIdentifier) operands[DeclName_OPERAND];
    }

    public void setDeclName(SqlIdentifier name) {
        Util.pre(name.isSimple(), "name.isSimple()");
        operands[DeclName_OPERAND] = name;
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
