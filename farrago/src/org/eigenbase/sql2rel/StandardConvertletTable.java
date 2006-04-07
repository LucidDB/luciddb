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

package org.eigenbase.sql2rel;

import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.util.Util;

import java.util.Arrays;

/**
 * Standard implementation of {@link SqlRexConvertletTable}.
 */
public class StandardConvertletTable extends ReflectiveConvertletTable
{

    public StandardConvertletTable()
    {
        super();

        // Register aliases (operators which have a different name but
        // identical behavior to other operators).
        addAlias(
            SqlStdOperatorTable.characterLengthFunc,
            SqlStdOperatorTable.charLengthFunc);
        addAlias(
            SqlStdOperatorTable.isUnknownOperator,
            SqlStdOperatorTable.isNullOperator);
        addAlias(
            SqlStdOperatorTable.isNotUnknownOperator,
            SqlStdOperatorTable.isNotNullOperator);

        // Register convertlets for specific objects.
        registerOp(
            SqlStdOperatorTable.castFunc,
            new SqlRexConvertlet() {
                public RexNode convertCall(SqlRexContext cx, SqlCall call)
                {
                    return convertCast(cx, (SqlCall) call);
                }
            }
        );
        registerOp(
            SqlStdOperatorTable.isDistinctFromOperator,
            new SqlRexConvertlet() {
                public RexNode convertCall(SqlRexContext cx, SqlCall call)
                {
                    return convertIsDistinctFrom(cx, (SqlCall) call, false);
                }
            }
        );
        registerOp(
            SqlStdOperatorTable.isNotDistinctFromOperator,
            new SqlRexConvertlet() {
                public RexNode convertCall(SqlRexContext cx, SqlCall call)
                {
                    return convertIsDistinctFrom(cx, (SqlCall) call, true);
                }
            }
        );

        // Expand "x NOT LIKE y" into "NOT (x LIKE y)"
        registerOp(
            SqlStdOperatorTable.notLikeOperator,
            new SqlRexConvertlet()
            {
                public RexNode convertCall(SqlRexContext cx, SqlCall call)
                {
                    final SqlCall expanded =
                        SqlStdOperatorTable.notOperator.createCall(
                            SqlStdOperatorTable.likeOperator.createCall(
                                call.getOperands(),
                                SqlParserPos.ZERO),
                            SqlParserPos.ZERO);
                    return cx.convertExpression(expanded);
                }
            }
        );

        // Unary "+" has no effect, so expand "+ x" into "x".
        registerOp(
            SqlStdOperatorTable.prefixPlusOperator,
            new SqlRexConvertlet()
            {
                public RexNode convertCall(SqlRexContext cx, SqlCall call)
                {
                    SqlNode expanded = ((SqlCall) call).getOperands()[0];
                    return cx.convertExpression(expanded);
                }
            }
        );

        // "AS" has no effect, so expand "x AS id" into "x".
        registerOp(
            SqlStdOperatorTable.asOperator,
            new SqlRexConvertlet()
            {
                public RexNode convertCall(SqlRexContext cx, SqlCall call)
                {
                    SqlNode expanded = ((SqlCall) call).getOperands()[0];
                    return cx.convertExpression(expanded);
                }
            }
        );

        // Convert "avg(<expr>)" to "case count(<expr>) when 0 then
        // null else sum(<expr>) / count(<expr>) end"
        registerOp(
            SqlStdOperatorTable.avgOperator,
            new SqlRexConvertlet()
            {
                public RexNode convertCall(SqlRexContext cx, SqlCall call)
                {
                    final SqlNode[] operands = call.getOperands();
                    Util.permAssert(operands.length == 1, "operands.length == 1");
                    final SqlNode arg = operands[0];
                    final SqlNode kase = expandAvg(arg, cx, call);
                    return cx.convertExpression(kase);
                }
            }
        );

        // Convert "element(<expr>)" to "$element_slice(<expr>)", if the
        // expression is a multiset of scalars.
        if (false)
        registerOp(
            SqlStdOperatorTable.elementFunc,
            new SqlRexConvertlet()
            {
                public RexNode convertCall(SqlRexContext cx, SqlCall call)
                {
                    final SqlNode[] operands = call.getOperands();
                    Util.permAssert(operands.length == 1, "operands.length == 1");
                    final SqlNode operand = operands[0];
                    final RelDataType type = cx.getValidator().getValidatedNodeType(operand);
                    if (!type.getComponentType().isStruct()) {
                        return
                            cx.convertExpression(
                                SqlStdOperatorTable.elementSlicefunc.createCall(
                                    operand,
                                    SqlParserPos.ZERO));
                    }
                    // fallback on default behavior
                    return StandardConvertletTable.this.convertCall(cx, call);
                }
            }
        );

        // Convert "$element_slice(<expr>)" to "element(<expr>).field#0"
        if (false)
        registerOp(
            SqlStdOperatorTable.elementSlicefunc,
            new SqlRexConvertlet()
            {
                public RexNode convertCall(SqlRexContext cx, SqlCall call)
                {
                    final SqlNode[] operands = call.getOperands();
                    Util.permAssert(operands.length == 1, "operands.length == 1");
                    final SqlNode operand = operands[0];
                    final RexNode expr =
                        cx.convertExpression(
                            SqlStdOperatorTable.elementFunc.createCall(
                                operand,
                                SqlParserPos.ZERO));
                    return cx.getRexBuilder().makeFieldAccess(
                        expr,
                        0);
                }
            }
        );
    }

    private SqlNode expandAvg(
        final SqlNode arg, SqlRexContext cx, SqlCall call)
    {
        final SqlParserPos pos = SqlParserPos.ZERO;
        final SqlNode sum =
            SqlStdOperatorTable.sumOperator.createCall(
                arg,
                pos);
        final SqlNode count =
            SqlStdOperatorTable.countOperator.createCall(
                arg,
                pos);
        final SqlLiteral nullLiteral = SqlLiteral.createNull(pos);
        final SqlValidator validator = cx.getValidator();
        // Need to set the type of the NULL literal, since it can only be
        // deduced from the context.
        RelDataType type = validator.getValidatedNodeType(call);
        type = cx.getTypeFactory().createTypeWithNullability(type, true);
        validator.setValidatedNodeType(nullLiteral, type);
        final SqlNode kase =
            SqlStdOperatorTable.caseOperator.createCall(
                count,
                new SqlNodeList(
                    Arrays.asList(
                        new SqlNode[] {
                            SqlLiteral.createExactNumeric(
                                "0",
                                pos)}),
                        pos),
                new SqlNodeList(
                    Arrays.asList(
                        new SqlNode[] {
                            nullLiteral
                        }),
                    pos),
                SqlStdOperatorTable.divideOperator.createCall(
                        new SqlNode[] {
                            sum,
                            count},
                        pos),
                pos);
        return kase;
    }

    /**
     * Converts a CASE expression.
     */
    public RexNode convertCase(
        SqlRexContext cx,
        SqlCase call)
    {
        SqlNodeList whenList = call.getWhenOperands();
        SqlNodeList thenList = call.getThenOperands();
        RexNode [] whenThenElseExprs = new RexNode[(whenList.size() * 2) + 1];
        assert (whenList.size() == thenList.size());

        for (int i = 0; i < whenList.size(); i++) {
            whenThenElseExprs[i * 2] =
                cx.convertExpression(whenList.get(i));
            whenThenElseExprs[(i * 2) + 1] =
                cx.convertExpression(thenList.get(i));
        }
        whenThenElseExprs[whenThenElseExprs.length - 1] =
            cx.convertExpression(call.getElseOperand());
        return cx.getRexBuilder().makeCall(
            SqlStdOperatorTable.caseOperator, whenThenElseExprs);
    }

    public RexNode convertMultiset(
        SqlRexContext cx,
        SqlMultisetOperator op,
        SqlCall call)
    {
        final RelDataType originalType =
            cx.getValidator().getValidatedNodeType(call);
        RexRangeRef rr = cx.getSubqueryExpr(call);
        assert rr != null;
        RelDataType msType = rr.getType().getFields()[0].getType();
        RexNode expr = cx.getRexBuilder().makeInputRef(msType, rr.getOffset());
        assert msType.getComponentType().isStruct();
        if (!originalType.getComponentType().isStruct()) {
            // If the type is not a struct, the multiset operator will have
            // wrapped the type as a record. Add a call to the $SLICE operator
            // to compensate. For example,
            // if '<ms>' has type 'RECORD (INTEGER x) MULTISET',
            // then '$SLICE(<ms>) has type 'INTEGER MULTISET'.
            // This will be removed as the expression is translated.
            expr = cx.getRexBuilder().makeCall(SqlStdOperatorTable.sliceOp, expr);
        }
        return expr;
    }

    public RexNode convertJdbc(
        SqlRexContext cx,
        SqlJdbcFunctionCall op,
        SqlCall call)
    {
        // Yuck!! The function definition contains arguments!
        // TODO: adopt a more conventional definition/instance structure
        final SqlCall convertedCall = op.getLookupCall();
        return cx.convertExpression(convertedCall);
    }

    protected RexNode convertCast(
        SqlRexContext cx,
        SqlCall call)
    {
        RelDataTypeFactory typeFactory = cx.getTypeFactory();
        assert SqlKind.Cast.equals(call.getOperator().getKind());
        SqlDataTypeSpec dataType = (SqlDataTypeSpec) call.operands[1];
        if (SqlUtil.isNullLiteral(call.operands[0], false)) {
            return cx.convertExpression(call.operands[0]);
        }
        RexNode arg = cx.convertExpression(call.operands[0]);
        RelDataType type = dataType.deriveType(typeFactory);
        if (arg.getType().isNullable()) {
            type = typeFactory.createTypeWithNullability(type, true);
        }
        if (null != dataType.getCollectionsTypeName()) {
            final RelDataType argComponentType =
                arg.getType().getComponentType();
            final RelDataType componentType = type.getComponentType();
            if (argComponentType.isStruct() &&
                !componentType.isStruct()) {
                RelDataType tt = typeFactory.createStructType(
                    new RelDataType[]{componentType},
                    new String[]{argComponentType.getFields()[0].getName()});
                tt = typeFactory.createTypeWithNullability(
                    tt, componentType.isNullable());
                boolean isn = type.isNullable();
                type = typeFactory.createMultisetType(tt, -1);
                type = typeFactory.createTypeWithNullability(type, isn);
            }
        }
        return cx.getRexBuilder().makeCast(type,arg);
    }

    public RexNode convertFunction(
        SqlRexContext cx,
        SqlFunction fun,
        SqlCall call)
    {
        final SqlNode[] operands = call.getOperands();
        final RexNode [] exprs = convertExpressionList(cx, operands);
        if (fun.getFunctionType() ==
            SqlFunctionCategory.UserDefinedConstructor) {
            return makeConstructorCall(cx, fun, exprs);
        }
        return cx.getRexBuilder().makeCall(fun, exprs);
    }

    public RexNode convertAggregateFunction(
        SqlRexContext cx,
        SqlAggFunction fun,
        SqlCall call)
    {
        final SqlNode[] operands = call.getOperands();
        final RexNode [] exprs;
        if (call.isCountStar()) {
            exprs = RexNode.EMPTY_ARRAY;
        } else {
            exprs = convertExpressionList(cx, operands);
        }
        return cx.getRexBuilder().makeCall(fun, exprs);
    }

    private RexNode makeConstructorCall(
        SqlRexContext cx,
        SqlFunction constructor,
        RexNode [] exprs)
    {
        final RexBuilder rexBuilder = cx.getRexBuilder();
        final RelDataTypeFactory typeFactory = cx.getTypeFactory();
        RelDataType type = rexBuilder.deriveReturnType(
            constructor, typeFactory, exprs);

        int n = type.getFieldCount();
        RexNode [] initializationExprs = new RexNode[n];
        for (int i = 0; i < n; ++i) {
            initializationExprs[i] =
                cx.getDefaultValueFactory().newAttributeInitializer(
                    type, constructor, i, exprs);
        }

        RexNode [] defaultCasts = RexUtil.generateCastExpressions(
            rexBuilder,
            type,
            initializationExprs);

        return rexBuilder.makeNewInvocation(type, defaultCasts);
    }

    /**
     * Converts a call to an operator into a {@link RexCall} to the same
     * operator.
     *
     * <p>Called automatically via reflection.
     *
     * @param cx Context
     * @param call Call
     * @return Rex call
     */
    public RexNode convertCall(
        SqlRexContext cx,
        SqlCall call)
    {
        final SqlOperator op = call.getOperator();
        final SqlNode[] operands = call.getOperands();
        final RexNode [] exprs = convertExpressionList(cx, operands);
        return cx.getRexBuilder().makeCall(op, exprs);
    }

    private RexNode [] convertExpressionList(
        SqlRexContext cx,
        SqlNode [] nodes)
    {
        final RexNode [] exprs = new RexNode[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            SqlNode node = nodes[i];
            exprs[i] = cx.convertExpression(node);
        }
        return exprs;
    }

    private RexNode convertIsDistinctFrom(
        SqlRexContext cx,
        SqlCall call,
        boolean neg)
    {
        RexNode op0 = cx.convertExpression(call.operands[0]);
        RexNode op1 = cx.convertExpression(call.operands[1]);
        return RelOptUtil.isDistinctFrom(cx.getRexBuilder(), op0, op1, neg);
    }

    /**
     * Converts a BETWEEN expression.
     *
     * <p>Called automatically via reflection.
     */
    public RexNode convertBetween(
        SqlRexContext cx,
        SqlBetweenOperator op,
        SqlCall call)
    {
        final SqlNode value = call.operands[SqlBetweenOperator.VALUE_OPERAND];
        RexNode x = cx.convertExpression(value);
        final SqlBetweenOperator.Flag symmetric = (SqlBetweenOperator.Flag)
            SqlLiteral.symbolValue(call.operands[SqlBetweenOperator.SYMFLAG_OPERAND]);
        final SqlNode lower = call.operands[SqlBetweenOperator.LOWER_OPERAND];
        RexNode y = cx.convertExpression(lower);
        final SqlNode upper = call.operands[SqlBetweenOperator.UPPER_OPERAND];
        RexNode z = cx.convertExpression(upper);

        RexNode res;

        final RexBuilder rexBuilder = cx.getRexBuilder();
        RexNode ge1 = rexBuilder.makeCall(
            SqlStdOperatorTable.greaterThanOrEqualOperator, x, y);
        RexNode le1 = rexBuilder.makeCall(
            SqlStdOperatorTable.lessThanOrEqualOperator, x, z);
        RexNode and1 = rexBuilder.makeCall(
            SqlStdOperatorTable.andOperator, ge1, le1);

        switch (symmetric.getOrdinal()) {
        case SqlBetweenOperator.Flag.Asymmetric_ordinal:
            res = and1;
            break;
        case SqlBetweenOperator.Flag.Symmetric_ordinal:
            RexNode ge2 =
                rexBuilder.makeCall(
                    SqlStdOperatorTable.greaterThanOrEqualOperator, x, z);
            RexNode le2 =
                rexBuilder.makeCall(
                    SqlStdOperatorTable.lessThanOrEqualOperator, x, y);
            RexNode and2 = rexBuilder.makeCall(
                SqlStdOperatorTable.andOperator, ge2, le2);
            res = rexBuilder.makeCall(
                SqlStdOperatorTable.orOperator, and1, and2);
            break;
        default:
            throw symmetric.unexpected();
        }
        final SqlBetweenOperator betweenOp =
            (SqlBetweenOperator) call.getOperator();
        if (betweenOp.isNegated()) {
            res = rexBuilder.makeCall(SqlStdOperatorTable.notOperator, res);
        }
        return res;
    }

    /**
     * Converts a LiteralChain expression: that is, concatenates the operands
     * immediately, to produce a single literal string.
     *
     * <p>Called automatically via reflection.
     */
    public RexNode convertLiteralChain(
        SqlRexContext cx,
        SqlLiteralChainOperator op,
        SqlCall call)
    {
        Util.discard(cx);

        SqlLiteral sum = SqlLiteralChainOperator.concatenateOperands(call);
        return cx.convertLiteral(sum);
    }


}

// End StandardConvertletTable.java
