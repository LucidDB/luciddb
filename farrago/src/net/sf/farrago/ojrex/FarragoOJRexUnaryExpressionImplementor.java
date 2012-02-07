/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package net.sf.farrago.ojrex;

import net.sf.farrago.type.*;
import net.sf.farrago.type.runtime.*;

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.type.*;


/**
 * FarragoOJRexUnaryExpressionImplementor implements Farrago specifics of {@link
 * org.eigenbase.oj.rex.OJRexImplementor} for row expressions which can be
 * translated to instances of OpenJava {@link UnaryExpression}.
 *
 * @author Angel Chang
 * @version $Id$
 */
public class FarragoOJRexUnaryExpressionImplementor
    extends FarragoOJRexImplementor
{
    //~ Instance fields --------------------------------------------------------

    private final int ojUnaryExpressionOrdinal;

    //~ Constructors -----------------------------------------------------------

    public FarragoOJRexUnaryExpressionImplementor(
        int ojUnaryExpressionOrdinal)
    {
        this.ojUnaryExpressionOrdinal = ojUnaryExpressionOrdinal;
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoOJRexImplementor
    public Expression implementFarrago(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        // TODO:  overflow detection, type promotion, etc.  Also, if global
        // analysis is used on the expression, we can reduce the number of
        // null-tests.
        Expression [] valueOperands = new Expression[1];

        for (int i = 0; i < 1; ++i) {
            valueOperands[i] =
                translator.convertPrimitiveAccess(
                    operands[i],
                    call.operands[i]);
        }

        if (!call.getType().isNullable()) {
            return implementNotNull(translator, call, valueOperands);
        }

        Variable varResult = translator.createScratchVariable(call.getType());

        Expression nullTest = null;
        nullTest =
            translator.createNullTest(
                call.operands[0],
                operands[0],
                nullTest);
        assert nullTest != null;

        // TODO:  generalize to stuff other than NullablePrimitive
        Statement assignmentStmt =
            new ExpressionStatement(
                new AssignmentExpression(
                    new FieldAccess(
                        varResult,
                        NullablePrimitive.VALUE_FIELD_NAME),
                    AssignmentExpression.EQUALS,
                    implementNotNull(translator, call, valueOperands)));

        Statement ifStatement =
            new IfStatement(
                nullTest,
                new StatementList(
                    translator.createSetNullStatement(
                        varResult,
                        true)),
                new StatementList(
                    translator.createSetNullStatement(
                        varResult,
                        false),
                    assignmentStmt));

        translator.addStatement(ifStatement);

        return varResult;
    }

    private Expression implementNotNull(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        RelDataType returnType = call.getType();

        FarragoTypeFactory factory = translator.getFarragoTypeFactory();
        Expression expr =
            new UnaryExpression(
                operands[0],
                ojUnaryExpressionOrdinal);
        if ((returnType.getSqlTypeName() != SqlTypeName.BOOLEAN)
            && (factory.getClassForPrimitive(returnType) != null))
        {
            // Cast to correct primitive return type so compiler is happy
            return new CastExpression(
                OJClass.forClass(factory.getClassForPrimitive(returnType)),
                expr);
        } else {
            return expr;
        }
    }
}

// End FarragoOJRexUnaryExpressionImplementor.java
