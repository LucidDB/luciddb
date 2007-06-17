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
package org.eigenbase.rex;

import org.eigenbase.sql.*;
import org.eigenbase.util.*;


/**
 * Translates a {@link RexNode row-expression} to a {@link SqlNode SQL parse
 * tree}.
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 24, 2003
 */
public class RexToSqlTranslator
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Converts a row-expression tree into a SQL expression tree.
     */
    public SqlNode translate(
        SqlWriter writer,
        RexNode expression)
    {
        if (expression instanceof RexLiteral) {
            return translateLiteral((RexLiteral) expression);
        } else if (expression instanceof RexDynamicParam) {
            RexDynamicParam dynamicParam = (RexDynamicParam) expression;
            return new SqlDynamicParam(
                dynamicParam.getIndex(),
                null);
        } else /* if (expression instanceof Variable) {
               Variable variable = (Variable) expression; String name =
                variable.toString(); int ordinal =
                RelOptUtil.getInputOrdinal(name); assert (ordinal >= 0) :
                "variable " + name; SqlNode fromItem =
                getFromNode(writer.query,ordinal); return new
                SqlIdentifier(getAlias(fromItem)); } else if (expression
                instanceof BinaryExpression) { BinaryExpression binaryExpression
                = (BinaryExpression) expression; for (int i = 0; i <
                mapBinaryJavaToSql.length; i += 2) { int javaOp =
                mapBinaryJavaToSql[i]; if (javaOp ==
                binaryExpression.getOperator()) { int sqlOp =
                mapBinaryJavaToSql[i + 1]; if (sqlOp != SqlNode.Kind.Other) {
                SqlOperator operator =
                SqlOperatorTable.instance().lookup(sqlOp); return
                operator.createCall(
                translate(writer,binaryExpression.getLeft()),
                translate(writer,binaryExpression.getRight())); } } } throw
                Util.newInternal( "cannot translate operator " +
                binaryExpression); } else if (expression instanceof
                UnaryExpression) { UnaryExpression unaryExpression =
                (UnaryExpression) expression; for (int i = 0; i <
                mapUnaryJavaToSql.length; i += 2) { int javaOp =
                mapUnaryJavaToSql[i]; if (javaOp ==
                unaryExpression.getOperator()) { int sqlOp = mapUnaryJavaToSql[i
                + 1]; if (sqlOp != SqlNode.Kind.Other) { SqlOperator operator =
                SqlOperatorTable.instance().lookup(sqlOp); return
                operator.createCall(
                translate(writer,unaryExpression.getExpression())); } } } throw
                Util.newInternal( "cannot translate operator " +
                unaryExpression); } else if (expression instanceof FieldAccess)
                { FieldAccess fieldAccess = (FieldAccess) expression;
                SqlIdentifier fromAlias = (SqlIdentifier) translate( writer,
                fieldAccess.getReferenceExpr()); SqlNode fromNode =
                getFromNode(writer.query,fromAlias.names[0]); String fieldName =
                fieldAccess.getName(); if (fromNode instanceof SqlIdentifier) {
                SqlIdentifier identifier = (SqlIdentifier) fromNode; if
                (identifier.extra instanceof JdbcTable) { JdbcTable jdbcTable =
                (JdbcTable) identifier.extra; JdbcTable.JdbcColumn column =
                jdbcTable.getColumn(fieldName); fieldName =
                column.getColumnName(); } } return
                SqlOperatorTable.instance().dotOperator.createCall( fromNode,
                new SqlIdentifier(fieldName)); } else if (expression instanceof
                MethodCall) { MethodCall methodCall = (MethodCall) expression;
                String methodName = methodCall.getName(); if
                (methodName.equals("equals")) { // java "x.equals(y)" --> sql "x
                = y" (todo: null semantics) return
                SqlOperatorTable.instance().equalsOperator.createCall(
                translate(writer,methodCall.getReferenceExpr()),
                translate(writer,methodCall.getArguments().get(0))); } else if
                (methodName.equals("substring")) { // java "x.substring(y,z)"
                --> "substr(x,y,z)" return
                SqlOperatorTable.instance().substringFunction .createCall( new
                SqlNode [] { translate(writer,methodCall.getReferenceExpr()),
                translate(writer,methodCall.getArguments().get(0)),
                translate(writer,methodCall.getArguments().get(1)) }); } else {
                throw Util.newInternal( "cannot translate method '" + methodName
                + "' in expression '" + expression + "'"); }} else */ {
            throw Util.newInternal(
                "cannot translate '"
                + expression.getClass() + "' expression '" + expression + "'");
        }
    }

    private SqlNode translateLiteral(RexLiteral literal)
    {
        throw Util.needToImplement(this);

        //return new SqlLiteral(literal.getValue(), null);
    }
}

// End RexToSqlTranslator.java
