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

import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.type.SqlTypeStrategies;
import org.eigenbase.sql.util.SqlVisitor;
import org.eigenbase.sql.util.SqlBasicVisitor;

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
 * 6: windowClause ({@link SqlNodeList})
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
            SqlTypeStrategies.rtiScope, null, null);
    }

    //~ Methods ---------------------------------------------------------------

    public SqlSyntax getSyntax()
    {
        return SqlSyntax.Special;
    }

    public SqlCall createCall(
        SqlNode [] operands,
        SqlParserPos pos,
        SqlLiteral functionQualifier)
    {
        assert functionQualifier == null;
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
     * @param pos          The parser position, or {@link SqlParserPos#ZERO}
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
            SqlParserPos pos)
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

    public <R> void acceptCall(
        SqlVisitor<R> visitor,
        SqlCall call,
        boolean onlyExpressions,
        SqlBasicVisitor.ArgHandler<R> argHandler)
    {
        if (onlyExpressions) {
            // None of the arguments to the SELECT operator are expressions.
            return;
        } else {
            super.acceptCall(visitor, call, onlyExpressions, argHandler);
        }
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        final SqlWriter.Frame selectFrame =
            writer.startList(SqlWriter.FrameType.Select);
        writer.sep("SELECT");
        final SqlNodeList keywords =
            (SqlNodeList) operands[SqlSelect.KEYWORDS_OPERAND];
        for (int i = 0; i < keywords.size(); i++) {
            final SqlNode keyword = keywords.get(i);
            keyword.unparse(writer, 0, 0);
        }
        SqlNode selectClause = operands[SqlSelect.SELECT_OPERAND];
        if (selectClause == null) {
            selectClause =
                new SqlIdentifier(
                    "*",
                    selectClause.getParserPosition());
        }
        final SqlWriter.Frame selectListFrame =
            writer.startList(SqlWriter.FrameType.SelectList);
        selectClause.unparse(writer, 0, 0);
        writer.endList(selectListFrame);

        writer.sep("FROM");
        SqlNode fromClause = operands[SqlSelect.FROM_OPERAND];

        // for FROM clause, use precedence just below join operator to make
        // sure that an unjoined nested select will be properly
        // parenthesized
        final SqlWriter.Frame fromFrame =
            writer.startList(SqlWriter.FrameType.FromList);
        fromClause.unparse(writer,
            SqlStdOperatorTable.joinOperator.getLeftPrec() - 1,
            SqlStdOperatorTable.joinOperator.getRightPrec() - 1);
        writer.endList(fromFrame);

        SqlNode whereClause = operands[SqlSelect.WHERE_OPERAND];
        if (whereClause != null) {
            writer.sep("WHERE");
            whereClause.unparse(writer, 0, 0);
        }
        SqlNodeList groupClause =
            (SqlNodeList) operands[SqlSelect.GROUP_OPERAND];
        if (groupClause != null) {
            writer.sep("GROUP BY");
            final SqlWriter.Frame groupFrame =
                writer.startList(SqlWriter.FrameType.GroupByList);
            if (groupClause.getList().isEmpty()) {
                final SqlWriter.Frame frame =
                    writer.startList(SqlWriter.FrameType.Simple, "(", ")");
                writer.endList(frame);
            } else {
                groupClause.unparse(writer, 0, 0);
            }
            writer.endList(groupFrame);
        }
        SqlNode havingClause = operands[SqlSelect.HAVING_OPERAND];
        if (havingClause != null) {
            writer.sep("HAVING");
            havingClause.unparse(writer, 0, 0);
        }
        SqlNodeList windowDecls = (SqlNodeList)
            operands[SqlSelect.WINDOW_OPERAND];
        if (windowDecls.size() > 0) {
            writer.sep("WINDOW");
            final SqlWriter.Frame windowFrame =
                writer.startList(SqlWriter.FrameType.WindowDeclList);
            for (int i = 0; i < windowDecls.size(); i++) {
                SqlNode windowDecl = windowDecls.get(i);
                writer.sep(",");
                windowDecl.unparse(writer, 0, 0);
            }
            writer.endList(windowFrame);
        }
        SqlNode orderClause = operands[SqlSelect.ORDER_OPERAND];
        if (orderClause != null) {
            writer.sep("ORDER BY");
            orderClause.unparse(writer, 0, 0);
        }
        writer.endList(selectFrame);
    }
}


// End SqlSelectOperator.java
