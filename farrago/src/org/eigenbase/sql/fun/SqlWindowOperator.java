/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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
package org.eigenbase.sql.fun;

import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.sql.test.SqlOperatorTests;
import org.eigenbase.sql.*;
import org.eigenbase.util.EnumeratedValues;

/**
 * An operator describing a window specification.
 *
 * <p>
 * Operands are as follows:
 *
 * <ul>
 * <li>
 * 0: name of referenced window ({@link SqlIdentifier})
 * </li>
 * <li>
 * 1: partition clause ({@link SqlNodeList})
 * </li>
 * <li>
 * 2: order clause ({@link SqlNodeList})
 * </li>
 * <li>
 * 3: isRows ({@link SqlLiteral})
 * </li>
 * <li>
 * 4: lowerBound ({@link SqlNode})
 * </li>
 * <li>
 * 5: upperBound ({@link SqlNode})
 * </li>
 * </ul>
 *
 * All operands are optional.
 * </p>
 *
 * @author jhyde
 * @since Oct 19, 2004
 * @version $Id$
 **/
public class SqlWindowOperator extends SqlOperator {
    /**
     * The FOLLOWING operator used exclusively in a window specification.
     */
    private final SqlPostfixOperator followingOperator =
            new SqlPostfixOperator("FOLLOWING", SqlKind.Other, 10, null, null, null);

    /**
     * The PRECEDING operator used exclusively in a window specification.
     */
    private final SqlPostfixOperator precedingOperator =
            new SqlPostfixOperator("PRECEDING", SqlKind.Other, 10, null, null, null);


    public SqlWindowOperator() {
        super("WINDOW", SqlKind.Window, 1, true, null, null, null);
    }

    public SqlSyntax getSyntax() {
        return SqlSyntax.Special;
    }

    public SqlCall createCall(
        SqlNode[] operands,
        SqlParserPos pos)
    {
        return new SqlWindow(this, operands, pos);
    }

    public SqlWindow createCall(
        SqlIdentifier declName,
        SqlIdentifier refName,
        SqlNodeList partitionList,
        SqlNodeList orderList,
        boolean isRows,
        SqlNode lowerBound,
        SqlNode upperBound,
        SqlParserPos pos)
    {
        return (SqlWindow) createCall(
            new SqlNode[] {
                declName, refName, partitionList, orderList,
                SqlLiteral.createBoolean(isRows, pos),
                lowerBound, upperBound
            },
            pos);
    }

    public void unparse(
            SqlWriter writer,
            SqlNode[] operands,
            int leftPrec,
            int rightPrec) {
        writer.print("(");
        SqlIdentifier refName =
                (SqlIdentifier) operands[SqlWindow.RefName_OPERAND];
        int clauseCount = 0;
        if (refName != null) {
            refName.unparse(writer, 0, 0);
            ++clauseCount;
        }
        SqlNodeList partitionList =
                (SqlNodeList) operands[SqlWindow.PartitionList_OPERAND];
        if (partitionList != null) {
            if (clauseCount++ > 0) {
                writer.println();
            }
            writer.print("PARTITION BY ");
            partitionList.unparse(writer, 0, 0);
        }
        SqlNodeList orderList =
                (SqlNodeList) operands[SqlWindow.OrderList_OPERAND];
        if (orderList != null) {
            if (clauseCount++ > 0) {
                writer.println();
            }
            writer.print("ORDER BY ");
            orderList.unparse(writer, 0, 0);
        }
        boolean isRows =
                SqlLiteral.booleanValue(operands[SqlWindow.IsRows_OPERAND]);
        SqlNode lowerBound = operands[SqlWindow.LowerBound_OPERAND],
                upperBound = operands[SqlWindow.UpperBound_OPERAND];
        if (lowerBound == null) {
            // No ROWS or RANGE clause
        } else if (upperBound == null) {
            if (clauseCount++ > 0) {
                writer.println();
            }
            if (isRows) {
                writer.print("ROWS ");
            } else {
                writer.print("RANGE ");
            }
            lowerBound.unparse(writer, 0, 0);
        } else {
            if (clauseCount++ > 0) {
                writer.println();
            }
            if (isRows) {
                writer.print("ROWS BETWEEN ");
            } else {
                writer.print("RANGE BETWEEN ");
            }
            lowerBound.unparse(writer, 0, 0);
            writer.print(" AND ");
            upperBound.unparse(writer, 0, 0);
        }
        writer.print(")");
    }

    public void validateCall(
        SqlCall call,
        SqlValidator validator,
        SqlValidator.Scope scope,
        SqlValidator.Scope operandScope)
    {
        // TODO: validate
        assert call.operator == this;
        final SqlNode [] operands = call.operands;
        SqlIdentifier refName =
                (SqlIdentifier) operands[SqlWindow.RefName_OPERAND];
        SqlNodeList partitionList =
                (SqlNodeList) operands[SqlWindow.PartitionList_OPERAND];
        SqlNodeList orderList =
                (SqlNodeList) operands[SqlWindow.OrderList_OPERAND];
        if (orderList != null) {
            for (int i = 0; i < orderList.size(); i++) {
                SqlNode orderItem = orderList.get(i);
                orderItem.validate(validator, scope);
            }
        }
        boolean isRows =
                SqlLiteral.booleanValue(operands[SqlWindow.IsRows_OPERAND]);
        SqlNode lowerBound = operands[SqlWindow.LowerBound_OPERAND],
                upperBound = operands[SqlWindow.UpperBound_OPERAND];
    }

    public void test(SqlTester tester) {
        SqlOperatorTests.testWindow(tester);
    }

    /**
     * An enumeration of types of bounds in a window: <code>CURRENT ROW</code>, 
     * <code>UNBOUNDED PRECEDING</code>, and <code>UNBOUNDED FOLLOWING</code>.
     */
    static class Bound extends EnumeratedValues.BasicValue {
        private Bound(String name, int ordinal) {
            super(name, ordinal, null);
        }
        public static final Bound CurrentRow = new Bound("CURRENT ROW", 0);
        public static final Bound UnboundedPreceding = new Bound("UNBOUNDED PRECEDING", 1);
        public static final Bound UnboundedFollowing = new Bound("UNBOUNDED FOLLOWING", 2);
    }

    public SqlNode createCurrentRow(SqlParserPos pos) {
        return SqlLiteral.createFlag(Bound.CurrentRow, pos);
    }

    public SqlNode createUnboundedFollowing(SqlParserPos pos) {
        return SqlLiteral.createFlag(Bound.UnboundedFollowing, pos);
    }

    public SqlNode createUnboundedPreceding(SqlParserPos pos) {
        return SqlLiteral.createFlag(Bound.UnboundedPreceding, pos);
    }

    public SqlNode createFollowing(SqlLiteral literal, SqlParserPos pos) {
        return followingOperator.createCall(literal, pos);
    }

    public SqlNode createPreceding(SqlLiteral literal, SqlParserPos pos) {
        return precedingOperator.createCall(literal, pos);
    }

    public SqlNode createBound(SqlLiteral range) {
        return range;
    }
}

// End SqlWindowOperator.java
