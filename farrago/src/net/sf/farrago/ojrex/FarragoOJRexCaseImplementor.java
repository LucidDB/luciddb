/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2005 Xiaoyang Luo
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
