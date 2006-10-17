/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2005-2005 Xiaoyang Luo
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

import net.sf.farrago.type.*;
import net.sf.farrago.type.runtime.*;

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.oj.util.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;


/**
 * FarragoOJRexCaseImplementor implements Farrago specifics of {@link
 * OJRexImplementor} for CASE expressions.
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
            boolean bHasElseAndLastOne = false;
            Expression cond = operands[i];
            Expression value = operands[i + 1];
            boolean isCondNullable = call.operands[i].getType().isNullable();
            StatementList caseCondStmtList = translator.getSubStmtList(i);
            StatementList stmtList = translator.getSubStmtList(i + 1);
            if (stmtList == null) {
                stmtList = new StatementList();
            }
            if (i == 0) {
                for (int k = 0; k < caseCondStmtList.size(); k++) {
                    translator.addStatement(caseCondStmtList.get(k));
                }
            }
            IfStatement ifStmt = null;

            if (i == (operands.length - 3)) {
                bHasElseAndLastOne = true;
            }

            translator.convertCastOrAssignmentWithStmtList(
                stmtList,
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

            if (bHasElseAndLastOne) {
                StatementList elseStmtList =
                    translator.getSubStmtList(operands.length - 1);
                translator.convertCastOrAssignmentWithStmtList(
                    elseStmtList,
                    call.toString(),
                    call.getType(),
                    call.operands[operands.length - 1].getType(),
                    varResult,
                    operands[operands.length - 1]);
                ifStmt = new IfStatement(cond, stmtList, elseStmtList);
            } else {
                ifStmt = new IfStatement(cond, stmtList);
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
