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
import org.eigenbase.sql.test.SqlOperatorTests;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.sql.type.ReturnTypeInference;


/**
 * An operator describing a query. (Not a query itself.)
 *
 * <p>
 * Operands are:
 *
 * <ul>
 * <li>
 * 0: distinct ({@link SqlLiteral})
 * </li>
 * <li>
 * 1: selectClause ({@link SqlNodeList})
 * </li>
 * <li>
 * 2: fromClause ({@link SqlCall} to "join" operator)
 * </li>
 * <li>
 * 3: whereClause ({@link SqlNode})
 * </li>
 * <li>
 * 4: havingClause ({@link SqlNode})
 * </li>
 * <li>
 * 5: groupClause ({@link SqlNode})
 * </li>
 * <li>
 * 6: orderClause ({@link SqlNode})
 * </li>
 * </ul>
 * </p>
 */
public class SqlSelectOperator extends SqlOperator
{
    //~ Constructors ----------------------------------------------------------

    public SqlSelectOperator()
    {
        super("SELECT", SqlKind.Select, 1, true, ReturnTypeInference.useScope,
            null, null);
    }

    //~ Methods ---------------------------------------------------------------

    public SqlSyntax getSyntax()
    {
        return SqlSyntax.Special;
    }

    public SqlCall createCall(
        SqlNode [] operands,
        ParserPosition pos)
    {
        return new SqlSelect(this, operands, pos);
    }

    public SqlSelect createCall(
        boolean isDistinct,
        SqlNodeList selectList,
        SqlNode fromClause,
        SqlNode whereClause,
        SqlNode groupBy,
        SqlNode having,
        SqlNode orderBy,
        ParserPosition pos)
    {
        return (SqlSelect) createCall(
            new SqlNode [] {
                SqlLiteral.createBoolean(isDistinct, pos),
                selectList, fromClause, whereClause, groupBy, having, orderBy
            },
            pos);
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        writer.print("SELECT ");
        if (SqlLiteral.booleanValue(operands[SqlSelect.DISTINCT_OPERAND])) {
            writer.print("DISTINCT ");
        }
        SqlNode selectClause = operands[SqlSelect.SELECT_OPERAND];
        if (selectClause == null) {
            selectClause =
                new SqlIdentifier(
                    "*",
                    selectClause.getParserPosition());
        }
        selectClause.unparse(writer, 0, 0);
        writer.println();
        writer.print("FROM ");
        SqlNode fromClause = operands[SqlSelect.FROM_OPERAND];

        // for FROM clause, use precedence just below join operator to make
        // sure that an unjoined nested select will be properly
        // parenthesized
        fromClause.unparse(writer,
            SqlOperatorTable.std().joinOperator.leftPrec - 1,
            SqlOperatorTable.std().joinOperator.rightPrec - 1);
        SqlNode whereClause = operands[SqlSelect.WHERE_OPERAND];
        if (whereClause != null) {
            writer.println();
            writer.print("WHERE ");
            whereClause.unparse(writer, 0, 0);
        }
        SqlNode groupClause = operands[SqlSelect.GROUP_OPERAND];
        if (groupClause != null) {
            writer.println();
            writer.print("GROUP BY ");
            groupClause.unparse(writer, 0, 0);
        }
        SqlNode havingClause = operands[SqlSelect.HAVING_OPERAND];
        if (havingClause != null) {
            writer.println();
            writer.print("HAVING ");
            havingClause.unparse(writer, 0, 0);
        }
        SqlNode orderClause = operands[SqlSelect.ORDER_OPERAND];
        if (orderClause != null) {
            writer.println();
            writer.print("ORDER BY ");
            orderClause.unparse(writer, 0, 0);
        }
    }

    public void test(SqlTester tester)
    {
        SqlOperatorTests.testSelect(tester);
    }
}


// End SqlSelectOperator.java
