/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.saffron.sql;

import net.sf.saffron.sql.test.SqlTester;
import net.sf.saffron.sql.type.SqlTypeName;

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
        super("SELECT",SqlKind.Select,1,true, SqlOperatorTable.useScope,null, null);
    }

    //~ Methods ---------------------------------------------------------------

    public int getSyntax()
    {
        return Syntax.Special;
    }

    public SqlCall createCall(SqlNode [] operands)
    {
        return new SqlSelect(this,operands);
    }

    public SqlSelect createCall(
        boolean isDistinct,
        SqlNodeList selectList,
        SqlNode fromClause,
        SqlNode whereClause,
        SqlNode groupBy,
        SqlNode having,
        SqlNode orderBy)
    {
        return (SqlSelect) createCall(
            new SqlNode [] {
                SqlLiteral.createBoolean(isDistinct),selectList,fromClause,
                whereClause,groupBy,having,orderBy
            });
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
            selectClause = new SqlIdentifier("*");
        }
        selectClause.unparse(writer,0,0);
        writer.println();
        writer.print("FROM ");
        SqlNode fromClause = operands[SqlSelect.FROM_OPERAND];
        // for FROM clause, use precedence just below join operator to make
        // sure that an unjoined nested select will be properly
        // parenthesized
        fromClause.unparse(
            writer,
            SqlOperatorTable.std().joinOperator.leftPrec - 1,
            SqlOperatorTable.std().joinOperator.rightPrec - 1);
        SqlNode whereClause = operands[SqlSelect.WHERE_OPERAND];
        if (whereClause != null) {
            writer.println();
            writer.print("WHERE ");
            whereClause.unparse(writer,0,0);
        }
        SqlNode groupClause = operands[SqlSelect.GROUP_OPERAND];
        if (groupClause != null) {
            writer.println();
            writer.print("GROUP BY ");
            groupClause.unparse(writer,0,0);
        }
        SqlNode havingClause = operands[SqlSelect.HAVING_OPERAND];
        if (havingClause != null) {
            writer.println();
            writer.print("HAVING ");
            havingClause.unparse(writer,0,0);
        }
        SqlNode orderClause = operands[SqlSelect.ORDER_OPERAND];
        if (orderClause != null) {
            writer.println();
            writer.print("ORDER BY ");
            orderClause.unparse(writer,0,0);
        }
    }

    public void test(SqlTester tester) {
        tester.check("select * from values(1)","1",SqlTypeName.Integer);
    }
}


// End SqlSelectOperator.java
