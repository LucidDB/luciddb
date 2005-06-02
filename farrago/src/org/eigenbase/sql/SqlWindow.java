/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.util.Util;
import org.eigenbase.resource.EigenbaseResource;

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

    /**
     * Creates a window.
    public SqlWindow(
        SqlWindowOperator operator,
        SqlNode[] operands,
        SqlParserPos pos)
    {
        super(operator,operands,pos);
        final SqlIdentifier declId = (SqlIdentifier) operands[DeclName_OPERAND];
        Util.pre(declId == null || declId.isSimple(), "declId.isSimple()");
        Util.pre(getPartitionList() != null, "getPartitionList() != null");
    }
     */

    /**
     * Creates a window.
     *
     * @pre operands[DeclName_OPERAND] == null ||
            operands[DeclName_OPERAND].isSimple()
     * @pre operands[OrderList_OPERAND] != null
     * @pre operands[PartitionList_OPERAND] != null
     */
    public SqlWindow(
        SqlWindowOperator operator,
        SqlNode[] operands,
        SqlParserPos pos)
    {
        super(operator,operands,pos);
        final SqlIdentifier declId =
            (SqlIdentifier) operands[DeclName_OPERAND];
        Util.pre(declId == null || declId.isSimple(),
            "operands[DeclName_OPERAND] == null || " +
            "operands[DeclName_OPERAND].isSimple()");
        Util.pre(operands[PartitionList_OPERAND] != null,
            "operands[PartitionList_OPERAND] != null");
        Util.pre(operands[OrderList_OPERAND] != null,
            "operands[OrderList_OPERAND] != null");
    }


    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        // Override, so we don't print extra parentheses.
        getOperator().unparse(writer, operands, 0, 0);
    }

    public SqlIdentifier getDeclName() {
        return (SqlIdentifier) operands[DeclName_OPERAND];
    }

    public void setDeclName(SqlIdentifier name) {
        Util.pre(name.isSimple(), "name.isSimple()");
        operands[DeclName_OPERAND] = name;
    }

    public SqlNode getLowerBound() {
        return operands[LowerBound_OPERAND];
    }

    public SqlNode getUpperBound() {
        return operands[UpperBound_OPERAND];
    }

    public boolean isRows() {
        return SqlLiteral.booleanValue(operands[IsRows_OPERAND]);
    }

    public SqlNodeList getOrderList() {
        return (SqlNodeList) operands[OrderList_OPERAND];
    }

    public SqlNodeList getPartitionList() {
        return (SqlNodeList) operands[PartitionList_OPERAND];
    }

    public SqlIdentifier getRefName() {
        return (SqlIdentifier) operands[RefName_OPERAND];
    }


    /**
     * Creates a new window by combining this one with another.
     *
     * <p>For example,
     *
     * <pre>WINDOW (w PARTITION BY x ORDER BY y)
     *   overlay
     *   WINDOW w AS (PARTITION BY z)</pre>
     *
     * yields
     *
     * <pre>WINDOW (PARTITION BY z ORDER BY y)</pre>
     *
     * <p>Does not alter this or the other window.
     *
     * @return A new window
     */
    public SqlWindow overlay(SqlWindow that, SqlValidator validator) {
        final SqlNode[] newOperands = (SqlNode[]) operands.clone();
        // Clear the reference window, because the reference is now resolved.
        // The overlaying window may have its own reference, of course.
        newOperands[RefName_OPERAND] = null;
        // Overlay other parameters.
        setOperand(newOperands, that.operands, PartitionList_OPERAND, validator);
        setOperand(newOperands, that.operands, OrderList_OPERAND, validator);
        setOperand(newOperands, that.operands, LowerBound_OPERAND, validator);
        setOperand(newOperands, that.operands, UpperBound_OPERAND, validator);
        return new SqlWindow((SqlWindowOperator) getOperator(), newOperands,
            SqlParserPos.ZERO);
    }

    private static void setOperand(
        final SqlNode[] destOperands,
        SqlNode[] srcOperands,
        int i,
        SqlValidator validator)
    {
        final SqlNode thatOperand = srcOperands[i];
        if (thatOperand != null && !SqlNodeList.isEmptyList(thatOperand)) {
            final SqlNode clonedOperand = destOperands[i];
            if (clonedOperand == null || SqlNodeList.isEmptyList(clonedOperand)) {
                destOperands[i] = thatOperand;
            } else {
                throw validator.newValidationError(clonedOperand,
                    EigenbaseResource.instance()
                    .newCannotOverrideWindowAttribute());
            }
        }
    }
}

// End SqlWindow.java
