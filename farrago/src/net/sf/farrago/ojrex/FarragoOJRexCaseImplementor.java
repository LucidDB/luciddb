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

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.oj.util.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.type.*;


/**
 * FarragoOJRexCaseImplementor implements Farrago specifics of {@link
 * org.eigenbase.oj.rex.OJRexImplementor} for <code>CASE</code> expressions.
 *
 * @author Xiaoyang Luo
 * @version $Id$
 */
public class FarragoOJRexCaseImplementor
    extends FarragoOJRexImplementor
{
    //~ Methods ----------------------------------------------------------------

    // implement FarragoOJRexImplementor
    public Expression implementFarrago(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        int i;

        RelDataType retType = call.getType();

        boolean hasElse = ((call.operands.length % 2) == 1);

        Variable varResult = null;

        if (SqlTypeUtil.isJavaPrimitive(retType) && !retType.isNullable()) {
            OJClass retClass =
                OJUtil.typeToOJClass(
                    retType,
                    translator.getFarragoTypeFactory());
            varResult = translator.getRelImplementor().newVariable();
            translator.addStatement(
                new VariableDeclaration(
                    TypeName.forOJClass(retClass),
                    new VariableDeclarator(
                        varResult.toString(),
                        null)));
        } else {
            varResult = translator.createScratchVariable(call.getType());
        }

        if (!hasElse) {
            translator.createSetNullStatement(varResult, true);
        }

        IfStatement wholeStatement = null;
        IfStatement prevIfStatement = null;

        for (i = 0; i < (operands.length - 1); i = i + 2) {
            Expression cond = operands[i];
            Expression value = operands[i + 1];
            boolean isCondNullable = call.operands[i].getType().isNullable();
            final FarragoRexToOJTranslator.Frame caseCondFrame =
                translator.getSubFrame(i);
            final StatementList caseCondStmtList = caseCondFrame.stmtList;
            FarragoRexToOJTranslator.Frame frame =
                translator.getSubFrame(i + 1);
            assert frame != null;
            IfStatement ifStmt = null;

            translator.convertCastOrAssignmentWithStmtList(
                frame.stmtList,
                call.toString(),
                call.getType(),
                call.operands[i + 1].getType(),
                varResult,
                value);

            if (isCondNullable) {
                // the result of comparison must be boolean.
                // If it is nullable then we get the result
                // from getBit.
                Expression getBitCondition =
                    new MethodCall(
                        cond,
                        "getBit",
                        new ExpressionList());
                Expression notNullTest =
                    new UnaryExpression(
                        translator.createNullTest(
                            call.operands[i],
                            cond,
                            null),
                        UnaryExpression.NOT);
                Expression condition =
                    new BinaryExpression(
                        notNullTest,
                        BinaryExpression.LOGICAL_AND,
                        getBitCondition);
                cond = condition;
            }

            final boolean bHasElseAndLastOne = (i == (operands.length - 3));
            if (bHasElseAndLastOne) {
                final FarragoRexToOJTranslator.Frame elseFrame =
                    translator.getSubFrame(operands.length - 1);
                final StatementList elseStmtList = elseFrame.stmtList;
                translator.convertCastOrAssignmentWithStmtList(
                    elseStmtList,
                    call.toString(),
                    call.getType(),
                    call.operands[operands.length - 1].getType(),
                    varResult,
                    operands[operands.length - 1]);
                ifStmt = new IfStatement(cond, frame.stmtList, elseStmtList);
            } else {
                ifStmt = new IfStatement(cond, frame.stmtList);
            }
            if (wholeStatement == null) {
                wholeStatement = ifStmt;
            }
            if (prevIfStatement == null) {
                prevIfStatement = ifStmt;
            } else {
                caseCondStmtList.add(ifStmt);
                prevIfStatement.setElseStatements(caseCondStmtList);
                prevIfStatement = ifStmt;
            }
        }
        translator.addStatement(wholeStatement);
        return varResult;
    }

    // implement OJRexImplementor
    public boolean canImplement(RexCall call)
    {
        return true;
    }
}

// End FarragoOJRexCaseImplementor.java
