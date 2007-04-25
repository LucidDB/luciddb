/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2007-2007 The Eigenbase Project
// Copyright (C) 2007-2007 Disruptive Tech
// Copyright (C) 2007-2007 LucidEra, Inc.
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
package org.eigenbase.rex;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.SqlTypeFamily;
import org.eigenbase.util.*;

/**
 * Standard implementation of {@link RexSqlConvertletTable}.
 */
public class RexSqlStandardConvertletTable
    extends RexSqlReflectiveConvertletTable
{

    //~ Constructors -----------------------------------------------------------

    public RexSqlStandardConvertletTable()
    {
        super();

        // Register convertlets

        registerOp(
            SqlStdOperatorTable.greaterThanOrEqualOperator,
            new RexSqlConvertlet() {
                public SqlNode convertCall(RelNode relNode, RexCall call)
                {
                    return convertBinaryOp(
                        SqlStdOperatorTable.greaterThanOrEqualOperator,
                        relNode,
                        (RexCall) call);
                }
            });

        registerOp(
            SqlStdOperatorTable.greaterThanOperator,
            new RexSqlConvertlet() {
                public SqlNode convertCall(RelNode relNode, RexCall call)
                {
                    return convertBinaryOp(
                        SqlStdOperatorTable.greaterThanOperator,
                        relNode,
                        (RexCall) call);
                }
            });

        registerOp(
            SqlStdOperatorTable.lessThanOrEqualOperator,
            new RexSqlConvertlet() {
                public SqlNode convertCall(RelNode relNode, RexCall call)
                {
                    return convertBinaryOp(
                        SqlStdOperatorTable.lessThanOrEqualOperator,
                        relNode,
                        (RexCall) call);
                }
            });

        registerOp(
            SqlStdOperatorTable.lessThanOperator,
            new RexSqlConvertlet() {
                public SqlNode convertCall(RelNode relNode, RexCall call)
                {
                    return convertBinaryOp(
                        SqlStdOperatorTable.lessThanOperator,
                        relNode,
                        (RexCall) call);
                }
            });

        registerOp(
            SqlStdOperatorTable.equalsOperator,
            new RexSqlConvertlet() {
                public SqlNode convertCall(RelNode relNode, RexCall call)
                {
                    return convertBinaryOp(
                        SqlStdOperatorTable.equalsOperator,
                        relNode,
                        (RexCall) call);
                }
            });

        registerOp(
            SqlStdOperatorTable.notEqualsOperator,
            new RexSqlConvertlet() {
                public SqlNode convertCall(RelNode relNode, RexCall call)
                {
                    return convertBinaryOp(
                        SqlStdOperatorTable.notEqualsOperator,
                        relNode,
                        (RexCall) call);
                }
            });

        registerOp(
            SqlStdOperatorTable.inOperator,
            new RexSqlConvertlet() {
                public SqlNode convertCall(RelNode relNode, RexCall call)
                {
                    return convertBinaryOp(
                        SqlStdOperatorTable.inOperator,
                        relNode,
                        (RexCall) call);
                }
            });

        registerOp(
            SqlStdOperatorTable.andOperator,
            new RexSqlConvertlet() {
                public SqlNode convertCall(RelNode relNode, RexCall call)
                {
                    return convertBinaryOp(
                        SqlStdOperatorTable.andOperator,
                        relNode,
                        (RexCall) call);
                }
            });

        registerOp(
            SqlStdOperatorTable.orOperator,
            new RexSqlConvertlet() {
                public SqlNode convertCall(RelNode relNode, RexCall call)
                {
                    return convertBinaryOp(
                        SqlStdOperatorTable.orOperator,
                        relNode,
                        (RexCall) call);
                }
            });

        registerOp(
            SqlStdOperatorTable.likeOperator,
            new RexSqlConvertlet() {
                public SqlNode convertCall(RelNode relNode, RexCall call)
                {
                    return convertBinaryOp(
                        SqlStdOperatorTable.likeOperator,
                        relNode,
                        (RexCall) call);
                }
            });

        registerOp(
            SqlStdOperatorTable.notLikeOperator,
            new RexSqlConvertlet() {
                public SqlNode convertCall(RelNode relNode, RexCall call)
                {
                    return convertBinaryOp(
                        SqlStdOperatorTable.notLikeOperator,
                        relNode,
                        (RexCall) call);
                }
            });

        registerOp(
            SqlStdOperatorTable.notOperator,
            new RexSqlConvertlet() {
                public SqlNode convertCall(RelNode relNode, RexCall call)
                {
                    return convertPrefixOp(
                        SqlStdOperatorTable.notOperator,
                        relNode,
                        (RexCall) call);
                }
            });

        registerOp(
            SqlStdOperatorTable.isNotNullOperator,
            new RexSqlConvertlet() {
                public SqlNode convertCall(RelNode relNode, RexCall call)
                {
                    return convertPostfixOp(
                        SqlStdOperatorTable.isNotNullOperator,
                        relNode,
                        (RexCall) call);
                }
            });

        registerOp(
            SqlStdOperatorTable.isNullOperator,
            new RexSqlConvertlet() {
                public SqlNode convertCall(RelNode relNode, RexCall call)
                {
                    return convertPostfixOp(
                        SqlStdOperatorTable.isNullOperator,
                        relNode,
                        (RexCall) call);
                }
            });

    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Converts a call to an operator into a {@link SqlCall} to the same
     * operator.
     *
     * <p>Called automatically via reflection.
     *
     * @param cx Context
     * @param call Call
     *
     * @return Sql call
     */
    public SqlNode convertCall(
        RelNode relNode,
        RexCall call)
    {
        if (get(call) == null) {
            // No convertlet was suitable.
            throw Util.needToImplement(call);
        }
        
        final SqlOperator op = call.getOperator();
        final RexNode [] operands = call.getOperands();

        final SqlNode [] exprs = convertExpressionList(relNode, operands);
        return new SqlCall(
            op,
            exprs,
            SqlParserPos.ZERO);
    }

    /**
     * Converts an expression from {@link RexNode} to {@link SqlNode} format.
     *
     * @param expr Expression to translate
     *
     * @return Converted expression
     */
    private SqlNode convertExpression(RelNode relNode, RexNode expr)
    {
        if (expr instanceof RexLiteral) {
            // Numeric
            if (SqlTypeFamily.ExactNumeric.getTypeNames().contains(
                    ((RexLiteral)expr).getTypeName())) {
                return SqlLiteral.createExactNumeric(
                    ((RexLiteral)expr).getValue().toString(),
                    SqlParserPos.ZERO);
            }

            if (SqlTypeFamily.ApproximateNumeric.getTypeNames().contains(
                    ((RexLiteral)expr).getTypeName())) {
                return SqlLiteral.createApproxNumeric(
                    ((RexLiteral)expr).getValue().toString(),
                    SqlParserPos.ZERO);
            }
            // Timestamp
            if (SqlTypeFamily.Timestamp.getTypeNames().contains(
                    ((RexLiteral)expr).getTypeName())) {
                return SqlLiteral.createTimestamp(
                    (Calendar)((RexLiteral)expr).getValue(),
                    0,
                    SqlParserPos.ZERO);
            }
            // Date
            if (SqlTypeFamily.Date.getTypeNames().contains(
                    ((RexLiteral)expr).getTypeName())) {
                return SqlLiteral.createDate(
                    (Calendar)((RexLiteral)expr).getValue(),
                    SqlParserPos.ZERO);
            }
            // String
            if (SqlTypeFamily.Character.getTypeNames().contains(
                    ((RexLiteral)expr).getTypeName())) {
                return SqlLiteral.createCharString(
                    ((NlsString)(((RexLiteral)expr).getValue())).getValue(),
                    SqlParserPos.ZERO);
            }
        }

        if (expr instanceof RexInputRef) {
            RelDataType fields = relNode.getRowType();
            String fieldName =
                fields.getFieldList().get(((RexInputRef)expr).index).getName();
            return new SqlIdentifier(fieldName, SqlParserPos.ZERO);
        }

        if (expr instanceof RexCall) {
            return convertCall(relNode, (RexCall)expr);
        }

        throw Util.needToImplement(expr);
    }

    private SqlNode [] convertExpressionList(
        RelNode relNode,
        RexNode [] nodes)
    {
        final SqlNode [] exprs = new SqlNode[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            RexNode node = nodes[i];
            exprs[i] = convertExpression(relNode, node);
        }
        return exprs;
    }

    private SqlNode convertBinaryOp(
        SqlOperator operator,
        RelNode relNode,
        RexCall call)
    {
        SqlNode op0 = convertExpression(relNode, call.operands[0]);
        SqlNode op1 = convertExpression(relNode, call.operands[1]);
        return new SqlCall(
            operator,
            new SqlNode[] {op0, op1},
            SqlParserPos.ZERO);
    }

    private SqlNode convertPrefixOp(
        SqlOperator operator,
        RelNode relNode,
        RexCall call)
    {
        SqlNode op0 = convertExpression(relNode, call.operands[0]);
        return new SqlCall(
            operator,
            new SqlNode[] {op0},
            SqlParserPos.ZERO);
    }

    private SqlNode convertPostfixOp(
        SqlOperator operator,
        RelNode relNode,
        RexCall call)
    {
        SqlNode op0 = convertExpression(relNode, call.operands[0]);
        return new SqlCall(
            operator,
            new SqlNode[] {op0},
            SqlParserPos.ZERO);
    }
}

// End RexSqlStandardConvertletTable.java
