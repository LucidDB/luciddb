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

package org.eigenbase.sql;

import org.eigenbase.util.Util;
import org.eigenbase.sql.parser.ParserPosition;


/**
 * A <code>SqlInsert</code> is a node of a parse tree which represents
 * an INSERT statement.
 */
public class SqlInsert extends SqlCall
{

    // constants representing operand positions
    public static final int TARGET_TABLE_OPERAND = 0;
    public static final int SOURCE_OPERAND = 1;
    public static final int TARGET_COLUMN_LIST_OPERAND = 2;
    public static final int SOURCE_SELECT_OPERAND = 3;
    public static final int OPERAND_COUNT = 4;

    //~ Constructors ----------------------------------------------------------

    public SqlInsert(
        SqlSpecialOperator operator,
        SqlIdentifier targetTable,
        SqlNode source,
        SqlNodeList columnList,
        ParserPosition parserPosition)
    {
        super(operator,new SqlNode[OPERAND_COUNT], parserPosition);
        operands[TARGET_TABLE_OPERAND] = targetTable;
        operands[SOURCE_OPERAND] = source;
        operands[TARGET_COLUMN_LIST_OPERAND] = columnList;
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
     * Get the source SELECT expression for the data to be inserted.  This
     * returns null before the statement
     * has been expanded by SqlValidator.createInternalSelect
     *
     * @return the source SELECT for the data to be inserted
     */
    public SqlSelect getSourceSelect()
    {
        return (SqlSelect) operands[SOURCE_SELECT_OPERAND];
    }

    /**
     * .
     *
     * @return the source expression for the data to be inserted
     */
    public SqlNode getSource()
    {
        return operands[SOURCE_OPERAND];
    }

    /**
     * .
     *
     * @return the list of target column names, or null for all columns in the
     * target table
     */
    public SqlNodeList getTargetColumnList()
    {
        return (SqlNodeList) operands[TARGET_COLUMN_LIST_OPERAND];
    }

    // implement SqlNode
    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        writer.print("INSERT INTO ");
        getTargetTable().unparse(writer,operator.leftPrec,operator.rightPrec);
        if (getTargetColumnList() != null) {
            getTargetColumnList().unparse(
                writer,operator.leftPrec,operator.rightPrec);
        }
        writer.println();
        getSource().unparse(writer,operator.leftPrec,operator.rightPrec);
    }
}

// End SqlInsert.java
