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

import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.ParserPosition;
import org.eigenbase.sql.test.SqlOperatorTests;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.sql.type.ReturnTypeInference;
import org.eigenbase.sql.type.ReturnTypeInferenceImpl;


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
 * 6: widnowClause ({@link SqlNodeList})
 * </li>
 * <li>
 * 7: orderClause ({@link SqlNode})
 * </li>
 * </ul>
 * </p>
 */
public class SqlSelectOperator extends SqlOperator
{
    //~ Constructors ----------------------------------------------------------

    public SqlSelectOperator()
    {
        super("SELECT", SqlKind.Select, 1, true,
            ReturnTypeInferenceImpl.useScope, null, null);
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

    /**
     * Creates a call to the <code>SELECT</code> operator.
     *
     * @param keywordList  List of keywords such DISTINCT and ALL, or null
     * @param selectList   The SELECT clause, or null if empty
     * @param fromClause   The FROM clause
     * @param whereClause  The WHERE clause, or null if not present
     * @param groupBy      The GROUP BY clause, or null if not present
     * @param having       The HAVING clause, or null if not present
     * @param windowDecls  The WINDOW clause, or null if not present
     * @param orderBy      The ORDER BY clause, or null if not present
     * @param pos          The parser position, or {@link ParserPosition#ZERO}
     *                     if not specified; must not be null.
     * @return A {@link SqlSelect}, never null
     */
    public SqlSelect createCall(
        SqlNodeList keywordList,
        SqlNodeList selectList,
        SqlNode fromClause,
        SqlNode whereClause,
        SqlNode groupBy,
        SqlNode having,
        SqlNodeList windowDecls,
        SqlNode orderBy,
            ParserPosition pos)
    {
        if (keywordList == null) {
            keywordList = new SqlNodeList(pos);
        }
        if (windowDecls == null) {
            windowDecls = new SqlNodeList(pos);
        }
        return (SqlSelect) createCall(
            new SqlNode [] {
                keywordList, selectList, fromClause, whereClause, groupBy,
                having, windowDecls, orderBy
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
        final SqlNodeList keywords =
            (SqlNodeList) operands[SqlSelect.KEYWORDS_OPERAND];
        for (int i = 0; i < keywords.size(); i++) {
            SqlSymbol keyword = (SqlSymbol) keywords.get(i);
            writer.print(keyword.getName());
            writer.print(" ");
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
            SqlStdOperatorTable.instance().joinOperator.leftPrec - 1,
            SqlStdOperatorTable.instance().joinOperator.rightPrec - 1);
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
        SqlNodeList windowDecls = (SqlNodeList)
                operands[SqlSelect.WINDOW_OPERAND];
        for (int i = 0; i < windowDecls.size(); i++) {
            SqlNode windowDecl = windowDecls.get(i);
            if (i == 0) {
                writer.println();
                writer.print("WINDOW ");
            } else {
                writer.print(",");
                writer.println();
            }
            windowDecl.unparse(writer, 0, 0);
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
