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

import org.eigenbase.sql.parser.SqlParserPos;


/**
 * A <code>SqlDelete</code> is a node of a parse tree which represents
 * a DELETE statement.
 */
public class SqlDelete extends SqlCall
{
    //~ Static fields/initializers --------------------------------------------

    // constants representing operand positions
    public static final int TARGET_TABLE_OPERAND = 0;
    public static final int CONDITION_OPERAND = 1;
    public static final int SOURCE_SELECT_OPERAND = 2;
    public static final int OPERAND_COUNT = 3;

    //~ Constructors ----------------------------------------------------------

    public SqlDelete(
        SqlSpecialOperator operator,
        SqlIdentifier targetTable,
        SqlNode condition,
        SqlParserPos pos)
    {
        super(operator, new SqlNode[OPERAND_COUNT], pos);
        operands[TARGET_TABLE_OPERAND] = targetTable;
        operands[CONDITION_OPERAND] = condition;
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
     * Get the filter condition for rows to be deleted.
     *
     * @return the condition expression for the data to be deleted, or null for
     * all rows in the table
     */
    public SqlCall getCondition()
    {
        return (SqlCall) operands[CONDITION_OPERAND];
    }

    /**
     * Get the source SELECT expression for the data to be inserted.  This
     * returns null before the condition has been expanded
     * by SqlValidator.createInternalSelect.
     *
     * @return the source SELECT for the data to be inserted
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
        writer.print("DELETE FROM ");
        getTargetTable().unparse(writer, operator.leftPrec, operator.rightPrec);
        if (getCondition() != null) {
            writer.println();
            writer.print("WHERE ");
            getCondition().unparse(writer, operator.leftPrec,
                operator.rightPrec);
        }
    }

    public void validate(SqlValidator validator, SqlValidator.Scope scope)
    {
        validator.validateDelete(this);
    }
}


// End SqlDelete.java
