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
import org.eigenbase.util.Util;


/**
 * A <code>SqlSelect</code> is a node of a parse tree which represents a
 * select statement. It warrants its own node type just because we have a lot
 * of methods to put somewhere.
 */
public class SqlSelect extends SqlCall
{
    //~ Static fields/initializers --------------------------------------------

    // constants representing operand positions
    public static final int KEYWORDS_OPERAND = 0;
    public static final int SELECT_OPERAND = 1;
    public static final int FROM_OPERAND = 2;
    public static final int WHERE_OPERAND = 3;
    public static final int GROUP_OPERAND = 4;
    public static final int HAVING_OPERAND = 5;
    public static final int WINDOW_OPERAND = 6;
    public static final int ORDER_OPERAND = 7;
    public static final int OPERAND_COUNT = 8;

    //~ Constructors ----------------------------------------------------------

    SqlSelect(
        SqlSelectOperator operator,
        SqlNode [] operands,
        ParserPosition pos)
    {
        super(operator, operands, pos);
        Util.pre(operands.length == OPERAND_COUNT,
            "operands.length == OPERAND_COUNT");
        Util.pre(operands[KEYWORDS_OPERAND] != null,
            "operands[KEYWORDS_OPERAND] != null");
        Util.pre(operands[KEYWORDS_OPERAND] instanceof SqlNodeList,
            "operands[KEYWORDS_OPERAND] instanceof SqlNodeList");
        Util.pre(operands[WINDOW_OPERAND] != null,
            "operands[WINDOW_OPERAND] != null");
        Util.pre(operands[WINDOW_OPERAND] instanceof SqlNodeList,
                "operands[WINDOW_OPERAND] instanceof SqlNodeList");
        Util.pre(pos != null, "pos != null");
    }

    //~ Methods ---------------------------------------------------------------

    public final boolean isDistinct()
    {
        final SqlNodeList keywords =
            (SqlNodeList) operands[SqlSelect.KEYWORDS_OPERAND];
        for (int i = 0; i < keywords.size(); i++) {
            SqlSymbol keyword = (SqlSymbol) keywords.get(i);
            if (keyword.getName().equals("DISTINCT")) {
                return true;
            }
        }
        return false;
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

    public final SqlNodeList getWindowList()
    {
        return (SqlNodeList) operands[SqlSelect.WINDOW_OPERAND];
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
                SqlOperatorTable.std().joinOperator.createCall(fromClause,
                    tableId, null);
        }
        operands[FROM_OPERAND] = fromClause;
    }

    public void addWhere(SqlNode condition)
    {
        assert (operands[SELECT_OPERAND] == null) :
                "cannot add a filter if there is already a select list";
        operands[WHERE_OPERAND] =
            SqlUtil.andExpressions(operands[WHERE_OPERAND], condition);
    }

    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        writer.pushQuery(this);
        super.unparse(writer, leftPrec, rightPrec);
        writer.popQuery(this);
    }

    public void validate(SqlValidator validator, SqlValidator.Scope scope)
    {
        validator.validateQuery(this);
    }
}

// End SqlSelect.java
