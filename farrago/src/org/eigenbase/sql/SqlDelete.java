/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
import org.eigenbase.sql.validate.SqlValidatorScope;
import org.eigenbase.sql.validate.SqlValidator;


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
    public static final int ALIAS_OPERAND = 3;
    public static final int OPERAND_COUNT = 4;

    //~ Constructors ----------------------------------------------------------

    public SqlDelete(
        SqlSpecialOperator operator,
        SqlIdentifier targetTable,
        SqlNode condition,
        SqlIdentifier alias,
        SqlParserPos pos)
    {
        super(operator, new SqlNode[OPERAND_COUNT], pos);
        operands[TARGET_TABLE_OPERAND] = targetTable;
        operands[CONDITION_OPERAND] = condition;
        operands[ALIAS_OPERAND] = alias;
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * @return the identifier for the target table of the deletion
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
     * Gets the filter condition for rows to be deleted.
     *
     * @return the condition expression for the data to be deleted, or null for
     * all rows in the table
     */
    public SqlCall getCondition()
    {
        return (SqlCall) operands[CONDITION_OPERAND];
    }

    /**
     * Gets the source SELECT expression for the data to be deleted.  This
     * returns null before the condition has been expanded
     * by SqlValidator.performUnconditionRewrites.
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
        final SqlWriter.Frame frame =
            writer.startList(SqlWriter.FrameType.Select, "DELETE FROM", "");
        getTargetTable().unparse(
            writer, getOperator().getLeftPrec(), getOperator().getRightPrec());
        if (getAlias() != null) {
            writer.keyword("AS");
            getAlias().unparse(
                writer, getOperator().getLeftPrec(),
                getOperator().getRightPrec());
        }
        if (getCondition() != null) {
            writer.sep("WHERE");
            getCondition().unparse(
                writer, getOperator().getLeftPrec(),
                getOperator().getRightPrec());
        }
        writer.endList(frame);
    }

    public void validate(SqlValidator validator, SqlValidatorScope scope)
    {
        validator.validateDelete(this);
    }
}


// End SqlDelete.java
