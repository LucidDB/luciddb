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

/**
 * A <code>SqlSelect</code> is a node of a parse tree which represents a
 * select statement. It warrants its own node type just because we have a lot
 * of methods to put somewhere.
 */
public class SqlSelect extends SqlCall
{
    //~ Static fields/initializers --------------------------------------------

    // constants representing operand positions
    public static final int DISTINCT_OPERAND = 0;
    public static final int SELECT_OPERAND = 1;
    public static final int FROM_OPERAND = 2;
    public static final int WHERE_OPERAND = 3;
    public static final int GROUP_OPERAND = 4;
    public static final int HAVING_OPERAND = 5;
    public static final int ORDER_OPERAND = 6;
    public static final int OPERAND_COUNT = 7;

    //~ Constructors ----------------------------------------------------------

    SqlSelect(SqlSelectOperator operator,SqlNode [] operands)
    {
        super(operator,operands);
    }

    //~ Methods ---------------------------------------------------------------

    public final boolean isDistinct()
    {
        return SqlLiteral.booleanValue(operands[SqlSelect.DISTINCT_OPERAND]);
    }

    public final SqlNode getFrom()
    {
        return operands[SqlSelect.FROM_OPERAND];
    }

    public final SqlNodeList getGroup()
    {
        return (SqlNodeList) operands[SqlSelect.GROUP_OPERAND];
    }

    public final SqlNode getHaving()
    {
        return operands[SqlSelect.HAVING_OPERAND];
    }

    public final SqlNodeList getSelectList()
    {
        return (SqlNodeList) operands[SqlSelect.SELECT_OPERAND];
    }

    public final SqlNode getWhere()
    {
        return operands[SqlSelect.WHERE_OPERAND];
    }

    public final SqlNodeList getOrderList()
    {
        return (SqlNodeList) operands[SqlSelect.ORDER_OPERAND];
    }

    public void addFrom(SqlIdentifier tableId)
    {
        SqlNode fromClause = getFrom();
        if (fromClause == null) {
            fromClause = tableId;
        } else {
            fromClause =
                SqlOperatorTable.instance().joinOperator.createCall(
                    fromClause,
                    tableId);
        }
        operands[FROM_OPERAND] = fromClause;
    }

    public void addWhere(SqlNode condition)
    {
        assert (operands[SELECT_OPERAND] == null) :
            "cannot add a filter if there is already a select list";
        operands[WHERE_OPERAND] =
            SqlUtil.andExpressions(operands[WHERE_OPERAND],condition);
    }

    public void unparse(SqlWriter writer,int leftPrec,int rightPrec)
    {
        writer.pushQuery(this);
        super.unparse(writer,leftPrec,rightPrec);
        writer.popQuery(this);
    }
}


// End SqlSelect.java
