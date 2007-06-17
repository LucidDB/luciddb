/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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

import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;


/**
 * A <code>SqlInsert</code> is a node of a parse tree which represents an INSERT
 * statement.
 */
public class SqlInsert
    extends SqlCall
{
    //~ Static fields/initializers ---------------------------------------------

    // constants representing operand positions
    public static final int KEYWORDS_OPERAND = 0;
    public static final int TARGET_TABLE_OPERAND = 1;
    public static final int SOURCE_OPERAND = 2;
    public static final int TARGET_COLUMN_LIST_OPERAND = 3;
    public static final int OPERAND_COUNT = 4;

    //~ Constructors -----------------------------------------------------------

    public SqlInsert(
        SqlSpecialOperator operator,
        SqlNodeList keywords,
        SqlIdentifier targetTable,
        SqlNode source,
        SqlNodeList columnList,
        SqlParserPos pos)
    {
        super(
            operator,
            new SqlNode[OPERAND_COUNT],
            pos);

        Util.pre(keywords != null, "keywords != null");

        operands[KEYWORDS_OPERAND] = keywords;
        operands[TARGET_TABLE_OPERAND] = targetTable;
        operands[SOURCE_OPERAND] = source;
        operands[TARGET_COLUMN_LIST_OPERAND] = columnList;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return the identifier for the target table of the insertion
     */
    public SqlIdentifier getTargetTable()
    {
        return (SqlIdentifier) operands[TARGET_TABLE_OPERAND];
    }

    /**
     * @return the source expression for the data to be inserted
     */
    public SqlNode getSource()
    {
        return operands[SOURCE_OPERAND];
    }

    /**
     * @return the list of target column names, or null for all columns in the
     * target table
     */
    public SqlNodeList getTargetColumnList()
    {
        return (SqlNodeList) operands[TARGET_COLUMN_LIST_OPERAND];
    }

    public final SqlNode getModifierNode(SqlInsertKeyword modifier)
    {
        final SqlNodeList keywords = (SqlNodeList) operands[KEYWORDS_OPERAND];
        for (int i = 0; i < keywords.size(); i++) {
            SqlInsertKeyword keyword =
                (SqlInsertKeyword) SqlLiteral.symbolValue(keywords.get(i));
            if (keyword == modifier) {
                return keywords.get(i);
            }
        }
        return null;
    }

    // implement SqlNode
    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        writer.startList(SqlWriter.FrameTypeEnum.Select);
        writer.sep("INSERT INTO");
        getTargetTable().unparse(
            writer,
            getOperator().getLeftPrec(),
            getOperator().getRightPrec());
        if (getTargetColumnList() != null) {
            getTargetColumnList().unparse(
                writer,
                getOperator().getLeftPrec(),
                getOperator().getRightPrec());
        }
        writer.newlineAndIndent();
        getSource().unparse(
            writer,
            getOperator().getLeftPrec(),
            getOperator().getRightPrec());
    }

    public void validate(SqlValidator validator, SqlValidatorScope scope)
    {
        validator.validateInsert(this);
    }
}

// End SqlInsert.java
