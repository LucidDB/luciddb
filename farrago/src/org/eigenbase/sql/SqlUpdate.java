/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

import java.util.Iterator;


/**
 * A <code>SqlUpdate</code> is a node of a parse tree which represents
 * an UPDATE statement.
 */
public class SqlUpdate extends SqlCall
{
    //~ Static fields/initializers --------------------------------------------

    // constants representing operand positions
    public static final int TARGET_TABLE_OPERAND = 0;
    public static final int SOURCE_EXPRESSION_LIST_OPERAND = 1;
    public static final int TARGET_COLUMN_LIST_OPERAND = 2;
    public static final int CONDITION_OPERAND = 3;
    public static final int SOURCE_SELECT_OPERAND = 4;
    public static final int ALIAS_OPERAND = 5;
    public static final int OPERAND_COUNT = 6;

    //~ Constructors ----------------------------------------------------------

    public SqlUpdate(
        SqlSpecialOperator operator,
        SqlIdentifier targetTable,
        SqlNodeList targetColumnList,
        SqlNodeList sourceExpressionList,
        SqlNode condition,
        SqlIdentifier alias,
        SqlParserPos pos)
    {
        super(operator, new SqlNode[OPERAND_COUNT], pos);
        operands[TARGET_TABLE_OPERAND] = targetTable;
        operands[SOURCE_EXPRESSION_LIST_OPERAND] = sourceExpressionList;
        operands[TARGET_COLUMN_LIST_OPERAND] = targetColumnList;
        operands[CONDITION_OPERAND] = condition;
        operands[ALIAS_OPERAND] = alias;
        assert (sourceExpressionList.size() == targetColumnList.size());
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * .
     *
     * @return the identifier for the target table of the insertion
     */
    public SqlIdentifier getTargetTable()
    {
        return (SqlIdentifier) operands[TARGET_TABLE_OPERAND];
    }

    /**
     * @return the alias for the target table of the deletion
     */
    public SqlIdentifier getAlias()
    {
        return (SqlIdentifier) operands[ALIAS_OPERAND];
    }

    /**
     * .
     *
     * @return the list of target column names
     */
    public SqlNodeList getTargetColumnList()
    {
        return (SqlNodeList) operands[TARGET_COLUMN_LIST_OPERAND];
    }

    /**
     * .
     *
     * @return the list of source expressions
     */
    public SqlNodeList getSourceExpressionList()
    {
        return (SqlNodeList) operands[SOURCE_EXPRESSION_LIST_OPERAND];
    }

    /**
     * Get the filter condition for rows to be updated.
     *
     * @return the condition expression for the data to be updated, or null for
     * all rows in the table
     */
    public SqlCall getCondition()
    {
        return (SqlCall) operands[CONDITION_OPERAND];
    }

    /**
     * Get the source SELECT expression for the data to be updated.  This
     * returns null before the statement
     * has been expanded by SqlValidator.performUnconditionalRewrites.
     *
     * @return the source SELECT for the data to be updated
     */
    public SqlSelect getSourceSelect()
    {
        return (SqlSelect) operands[SOURCE_SELECT_OPERAND];
    }

    // implement SqlNode
    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        writer.print("UPDATE ");
        getTargetTable().unparse(writer, operator.leftPrec, operator.rightPrec);
        if (getTargetColumnList() != null) {
            getTargetColumnList().unparse(writer, operator.leftPrec,
                operator.rightPrec);
        }
        if (getAlias() != null) {
            writer.print(" AS ");
            getAlias().unparse(writer, operator.leftPrec, operator.rightPrec);
        }
        writer.print("SET ");
        Iterator targetColumnIter = getTargetColumnList().getList().iterator();
        Iterator sourceExpressionIter =
            getSourceExpressionList().getList().iterator();
        while (targetColumnIter.hasNext()) {
            writer.println();
            SqlIdentifier id = (SqlIdentifier) targetColumnIter.next();
            id.unparse(writer, operator.leftPrec, operator.rightPrec);
            writer.print(" = ");
            SqlNode sourceExp = (SqlNode) sourceExpressionIter.next();
            sourceExp.unparse(writer, operator.leftPrec, operator.rightPrec);
            if (targetColumnIter.hasNext()) {
                writer.print(",");
            }
        }
        if (getCondition() != null) {
            writer.println();
            writer.print("WHERE ");
            getCondition().unparse(writer, operator.leftPrec,
                operator.rightPrec);
        }
    }

    public void validate(SqlValidator validator, SqlValidator.Scope scope)
    {
        validator.validateUpdate(this);
    }
}


// End SqlUpdate.java
