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

import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.*;
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
                public SqlNode convertCall(
                    RexToSqlNodeConverter converter,
                    RexCall call)
                {
                    return convertBinaryOp(
                        SqlStdOperatorTable.greaterThanOrEqualOperator,
                        converter,
                        (RexCall) call);
                }
            });

        registerOp(
            SqlStdOperatorTable.greaterThanOperator,
            new RexSqlConvertlet() {
                public SqlNode convertCall(
                    RexToSqlNodeConverter converter,
                    RexCall call)
                {
                    return convertBinaryOp(
                        SqlStdOperatorTable.greaterThanOperator,
                        converter,
                        (RexCall) call);
                }
            });

        registerOp(
            SqlStdOperatorTable.lessThanOrEqualOperator,
            new RexSqlConvertlet() {
                public SqlNode convertCall(
                    RexToSqlNodeConverter converter,
                    RexCall call)
                {
                    return convertBinaryOp(
                        SqlStdOperatorTable.lessThanOrEqualOperator,
                        converter,
                        (RexCall) call);
                }
            });

        registerOp(
            SqlStdOperatorTable.lessThanOperator,
            new RexSqlConvertlet() {
                public SqlNode convertCall(
                    RexToSqlNodeConverter converter,
                    RexCall call)
                {
                    return convertBinaryOp(
                        SqlStdOperatorTable.lessThanOperator,
                        converter,
                        (RexCall) call);
                }
            });

        registerOp(
            SqlStdOperatorTable.equalsOperator,
            new RexSqlConvertlet() {
                public SqlNode convertCall(
                    RexToSqlNodeConverter converter,
                    RexCall call)
                {
                    return convertBinaryOp(
                        SqlStdOperatorTable.equalsOperator,
                        converter,
                        (RexCall) call);
                }
            });

        registerOp(
            SqlStdOperatorTable.notEqualsOperator,
            new RexSqlConvertlet() {
                public SqlNode convertCall(
                    RexToSqlNodeConverter converter,
                    RexCall call)
                {
                    return convertBinaryOp(
                        SqlStdOperatorTable.notEqualsOperator,
                        converter,
                        (RexCall) call);
                }
            });

        registerOp(
            SqlStdOperatorTable.inOperator,
            new RexSqlConvertlet() {
                public SqlNode convertCall(
                    RexToSqlNodeConverter converter,
                    RexCall call)
                {
                    return convertBinaryOp(
                        SqlStdOperatorTable.inOperator,
                        converter,
                        (RexCall) call);
                }
            });

        registerOp(
            SqlStdOperatorTable.andOperator,
            new RexSqlConvertlet() {
                public SqlNode convertCall(
                    RexToSqlNodeConverter converter,
                    RexCall call)
                {
                    return convertBinaryOp(
                        SqlStdOperatorTable.andOperator,
                        converter,
                        (RexCall) call);
                }
            });

        registerOp(
            SqlStdOperatorTable.orOperator,
            new RexSqlConvertlet() {
                public SqlNode convertCall(
                    RexToSqlNodeConverter converter,
                    RexCall call)
                {
                    return convertBinaryOp(
                        SqlStdOperatorTable.orOperator,
                        converter,
                        (RexCall) call);
                }
            });

        registerOp(
            SqlStdOperatorTable.likeOperator,
            new RexSqlConvertlet() {
                public SqlNode convertCall(
                    RexToSqlNodeConverter converter,
                    RexCall call)
                {
                    return convertBinaryOp(
                        SqlStdOperatorTable.likeOperator,
                        converter,
                        (RexCall) call);
                }
            });

        registerOp(
            SqlStdOperatorTable.notLikeOperator,
            new RexSqlConvertlet() {
                public SqlNode convertCall(
                    RexToSqlNodeConverter converter,
                    RexCall call)
                {
                    return convertBinaryOp(
                        SqlStdOperatorTable.notLikeOperator,
                        converter,
                        (RexCall) call);
                }
            });

        registerOp(
            SqlStdOperatorTable.notOperator,
            new RexSqlConvertlet() {
                public SqlNode convertCall(
                    RexToSqlNodeConverter converter,
                    RexCall call)
                {
                    return convertPrefixOp(
                        SqlStdOperatorTable.notOperator,
                        converter,
                        (RexCall) call);
                }
            });

        registerOp(
            SqlStdOperatorTable.isNotNullOperator,
            new RexSqlConvertlet() {
                public SqlNode convertCall(
                    RexToSqlNodeConverter converter,
                    RexCall call)
                {
                    return convertPostfixOp(
                        SqlStdOperatorTable.isNotNullOperator,
                        converter,
                        (RexCall) call);
                }
            });

        registerOp(
            SqlStdOperatorTable.isNullOperator,
            new RexSqlConvertlet() {
                public SqlNode convertCall(
                    RexToSqlNodeConverter converter,
                    RexCall call)
                {
                    return convertPostfixOp(
                        SqlStdOperatorTable.isNullOperator,
                        converter,
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
     * @param converter Converter
     * @param call Call
     *
     * @return Sql call
     */
    public SqlNode convertCall(
        RexToSqlNodeConverter converter,
        RexCall call)
    {
        if (get(call) == null) {
            // No convertlet was suitable.
            throw Util.needToImplement(call);
        }

        final SqlOperator op = call.getOperator();
        final RexNode [] operands = call.getOperands();

        final SqlNode [] exprs = convertExpressionList(converter, operands);
        return new SqlCall(
            op,
            exprs,
            SqlParserPos.ZERO);
    }

    /**
     * Converts an expression from {@link RexNode} to {@link SqlNode} format.
     *
     * @param converter Converter
     * @param expr Expression to translate
     *
     * @return Converted expression
     */
    private SqlNode convertExpression(
        RexToSqlNodeConverter converter,
        RexNode expr)
    {
        if (expr instanceof RexLiteral) {
            return converter.convertLiteral((RexLiteral)expr);
        }

        if (expr instanceof RexInputRef) {
            return converter.convertInputRef((RexInputRef)expr);
        }

        if (expr instanceof RexCall) {
            return convertCall(converter, (RexCall)expr);
        }

        throw Util.needToImplement(expr);
    }

    private SqlNode [] convertExpressionList(
        RexToSqlNodeConverter converter,
        RexNode [] nodes)
    {
        final SqlNode [] exprs = new SqlNode[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            RexNode node = nodes[i];
            exprs[i] = convertExpression(converter, node);
        }
        return exprs;
    }

    private SqlNode convertBinaryOp(
        SqlOperator operator,
        RexToSqlNodeConverter converter,
        RexCall call)
    {
        SqlNode op0 = convertExpression(converter, call.operands[0]);
        SqlNode op1 = convertExpression(converter, call.operands[1]);
        return new SqlCall(
            operator,
            new SqlNode[] {op0, op1},
            SqlParserPos.ZERO);
    }

    private SqlNode convertPrefixOp(
        SqlOperator operator,
        RexToSqlNodeConverter converter,
        RexCall call)
    {
        SqlNode op0 = convertExpression(converter, call.operands[0]);
        return new SqlCall(
            operator,
            new SqlNode[] {op0},
            SqlParserPos.ZERO);
    }

    private SqlNode convertPostfixOp(
        SqlOperator operator,
        RexToSqlNodeConverter converter,
        RexCall call)
    {
        SqlNode op0 = convertExpression(converter, call.operands[0]);
        return new SqlCall(
            operator,
            new SqlNode[] {op0},
            SqlParserPos.ZERO);
    }
}

// End RexSqlStandardConvertletTable.java
