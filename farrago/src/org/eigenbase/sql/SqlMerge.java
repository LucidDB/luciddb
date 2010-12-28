/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2002 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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

import java.util.*;

import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.validate.*;


/**
 * A <code>SqlMerge</code> is a node of a parse tree which represents a MERGE
 * statement.
 */
public class SqlMerge
    extends SqlCall
{
    //~ Static fields/initializers ---------------------------------------------

    // constants representing operand positions
    public static final int TARGET_TABLE_OPERAND = 0;
    public static final int SOURCE_TABLEREF_OPERAND = 1;
    public static final int CONDITION_OPERAND = 2;
    public static final int UPDATE_OPERAND = 3;
    public static final int INSERT_OPERAND = 4;
    public static final int SOURCE_SELECT_OPERAND = 5;
    public static final int ALIAS_OPERAND = 6;
    public static final int OPERAND_COUNT = 7;

    //~ Constructors -----------------------------------------------------------

    public SqlMerge(
        SqlSpecialOperator operator,
        SqlIdentifier targetTable,
        SqlNode condition,
        SqlNode source,
        SqlNode updateCall,
        SqlNode insertCall,
        SqlIdentifier alias,
        SqlParserPos pos)
    {
        super(
            operator,
            new SqlNode[OPERAND_COUNT],
            pos);
        operands[TARGET_TABLE_OPERAND] = targetTable;
        operands[CONDITION_OPERAND] = condition;
        operands[SOURCE_TABLEREF_OPERAND] = source;
        operands[UPDATE_OPERAND] = updateCall;
        operands[INSERT_OPERAND] = insertCall;
        operands[ALIAS_OPERAND] = alias;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return the identifier for the target table of the merge
     */
    public SqlIdentifier getTargetTable()
    {
        return (SqlIdentifier) operands[TARGET_TABLE_OPERAND];
    }

    /**
     * @return the alias for the target table of the merge
     */
    public SqlIdentifier getAlias()
    {
        return (SqlIdentifier) operands[ALIAS_OPERAND];
    }

    /**
     * @return the source for the merge
     */
    public SqlNode getSourceTableRef()
    {
        return (SqlNode) operands[SOURCE_TABLEREF_OPERAND];
    }

    public void setSourceTableRef(SqlNode tableRef)
    {
        operands[SOURCE_TABLEREF_OPERAND] = tableRef;
    }

    /**
     * @return the update statement for the merge
     */
    public SqlUpdate getUpdateCall()
    {
        return (SqlUpdate) operands[UPDATE_OPERAND];
    }

    /**
     * @return the insert statement for the merge
     */
    public SqlInsert getInsertCall()
    {
        return (SqlInsert) operands[INSERT_OPERAND];
    }

    /**
     * @return the condition expression to determine whether to update or insert
     */
    public SqlNode getCondition()
    {
        return operands[CONDITION_OPERAND];
    }

    /**
     * Gets the source SELECT expression for the data to be updated/inserted.
     * Returns null before the statement has been expanded by
     * SqlValidator.performUnconditionalRewrites.
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
        final SqlWriter.Frame frame =
            writer.startList(SqlWriter.FrameTypeEnum.Select, "MERGE INTO", "");
        getTargetTable().unparse(
            writer,
            getOperator().getLeftPrec(),
            getOperator().getRightPrec());
        if (getAlias() != null) {
            writer.keyword("AS");
            getAlias().unparse(
                writer,
                getOperator().getLeftPrec(),
                getOperator().getRightPrec());
        }

        writer.newlineAndIndent();
        writer.keyword("USING");
        getSourceTableRef().unparse(
            writer,
            getOperator().getLeftPrec(),
            getOperator().getRightPrec());

        writer.newlineAndIndent();
        writer.keyword("ON");
        getCondition().unparse(
            writer,
            getOperator().getLeftPrec(),
            getOperator().getRightPrec());

        SqlUpdate updateCall = (SqlUpdate) getUpdateCall();
        if (updateCall != null) {
            writer.newlineAndIndent();
            writer.keyword("WHEN MATCHED THEN UPDATE");
            final SqlWriter.Frame setFrame =
                writer.startList(
                    SqlWriter.FrameTypeEnum.UpdateSetList,
                    "SET",
                    "");

            Iterator targetColumnIter =
                updateCall.getTargetColumnList().getList().iterator();
            Iterator sourceExpressionIter =
                updateCall.getSourceExpressionList().getList().iterator();
            while (targetColumnIter.hasNext()) {
                writer.sep(",");
                SqlIdentifier id = (SqlIdentifier) targetColumnIter.next();
                id.unparse(
                    writer,
                    getOperator().getLeftPrec(),
                    getOperator().getRightPrec());
                writer.keyword("=");
                SqlNode sourceExp = (SqlNode) sourceExpressionIter.next();
                sourceExp.unparse(
                    writer,
                    getOperator().getLeftPrec(),
                    getOperator().getRightPrec());
            }
            writer.endList(setFrame);
        }

        SqlInsert insertCall = (SqlInsert) getInsertCall();
        if (insertCall != null) {
            writer.newlineAndIndent();
            writer.keyword("WHEN NOT MATCHED THEN INSERT");
            if (insertCall.getTargetColumnList() != null) {
                insertCall.getTargetColumnList().unparse(
                    writer,
                    getOperator().getLeftPrec(),
                    getOperator().getRightPrec());
            }
            insertCall.getSource().unparse(
                writer,
                getOperator().getLeftPrec(),
                getOperator().getRightPrec());

            writer.endList(frame);
        }
    }

    public void validate(SqlValidator validator, SqlValidatorScope scope)
    {
        validator.validateMerge(this);
    }
}

// End SqlMerge.java
