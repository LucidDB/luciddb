/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2005-2007 Xiaoyang
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

import net.sf.farrago.type.runtime.*;

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.oj.util.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * FarragoOJRexBuiltinImplementor implements Farrago specifics of {@link
 * org.eigenbase.oj.rex.OJRexImplementor} for builtin functions
 *
 * @author Xiaoyang Luo
 * @version $Id$
 */
public class FarragoOJRexBuiltinImplementor
    extends FarragoOJRexImplementor
{
    //~ Enums ------------------------------------------------------------------

    /**
     * Enumeration of SQL operators that can be implemented in OJ.
     */
    public enum Function
    {
        FLOOR, CEIL, ABS, POWER, LN, LOG10, SUBSTRING, OVERLAY, MOD, EXP,
        CONCAT, TRIM, POSITION, CHAR_LENGTH, CHARACTER_LENGTH, UPPER, LOWER,
        INITCAP, CONVERT, TRANSLATE,
    }

    //~ Instance fields --------------------------------------------------------

    protected final Function builtinFunction;

    //~ Constructors -----------------------------------------------------------

    public FarragoOJRexBuiltinImplementor(Function function)
    {
        builtinFunction = function;
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoOJRexImplementor
    public Expression implementFarrago(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        Variable varResult;
        Expression nullTest = null;

        RelDataType retType = call.getType();

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
            varResult = translator.createScratchVariable(retType);
        }

        for (int i = 0; i < operands.length; i++) {
            nullTest =
                translator.createNullTest(
                    call.operands[i],
                    operands[i],
                    nullTest);
        }

        StatementList stmtList = new StatementList();

        switch (builtinFunction) {
        case FLOOR:
        case CEIL:
            implementFloorCeil(translator, call, operands, varResult, stmtList);
            break;
        case ABS:
            implementAbs(translator, call, operands, varResult, stmtList);
            break;
        case POWER:
            implementPower(translator, call, operands, varResult, stmtList);
            break;
        case LN:
        case LOG10:
            implementLog(translator, call, operands, varResult, stmtList);
            break;
        case SUBSTRING:
            implementSubstring(translator, call, operands, varResult, stmtList);
            break;
        case OVERLAY:
            implementOverlay(translator, call, operands, varResult, stmtList);
            break;
        case CONCAT:
            implementConcat(translator, call, operands, varResult, stmtList);
            break;
        case MOD:
            implementMod(translator, call, operands, varResult, stmtList);
            break;
        case EXP:
            implementExp(translator, call, operands, varResult, stmtList);
            break;
        case LOWER:
        case UPPER:
        case INITCAP:
            implementChangeCase(
                translator,
                call,
                operands,
                varResult,
                stmtList);
            break;
        case TRIM:
            implementTrim(translator, call, operands, varResult, stmtList);
            break;
        case POSITION:
            translator.addAssignmentStatement(
                stmtList,
                new MethodCall(
                    operands[1],
                    BytePointer.POSITION_METHOD_NAME,
                    new ExpressionList(operands[0])),
                call.getType(),
                varResult,
                false);
            break;
        case CHAR_LENGTH:
        case CHARACTER_LENGTH:
            translator.addAssignmentStatement(
                stmtList,
                new MethodCall(
                    operands[0],
                    "length",
                    new ExpressionList()),
                call.getType(),
                varResult,
                false);
            break;
        default:
            assert (false);
        }

        // All the builtin function returns null if
        // one of the arguements is null.

        if (nullTest != null) {
            translator.addStatement(
                new IfStatement(
                    nullTest,
                    new StatementList(
                        translator.createSetNullStatement(varResult, true)),
                    stmtList));
        } else {
            for (int i = 0; i < stmtList.size(); i++) {
                translator.addStatement(stmtList.get(i));
            }
        }
        return varResult;
    }

    private void implementLog(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands,
        Variable varResult,
        StatementList stmtList)
    {
        Expression argument =
            translator.convertPrimitiveAccess(
                operands[0],
                call.operands[0]);

        Expression logFunc =
            new MethodCall(
                new Literal(Literal.STRING, "java.lang.Math"),
                "log",
                new ExpressionList(argument));
        if (builtinFunction == Function.LOG10) {
            Expression ln10 =
                new MethodCall(
                    new Literal(Literal.STRING, "java.lang.Math"),
                    "log",
                    new ExpressionList(new Literal(Literal.DOUBLE, "10.0")));
            logFunc =
                new BinaryExpression(logFunc,
                    BinaryExpression.DIVIDE,
                    ln10);
        }
        StatementList stmtList1 = new StatementList();
        translator.addAssignmentStatement(
            stmtList1,
            logFunc,
            call.getType(),
            varResult,
            false);
        Statement ifStmt =
            new IfStatement(
                new BinaryExpression(
                    argument,
                    BinaryExpression.GREATER,
                    Literal.constantZero()),
                stmtList1,
                getThrowStatementList(builtinFunction));

        stmtList.add(ifStmt);
    }

    protected void implementFloorCeil(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands,
        Variable varResult,
        StatementList stmtList)
    {
        Expression argument =
            translator.convertPrimitiveAccess(
                operands[0],
                call.operands[0]);

        String funcStr = "floor";
        if (builtinFunction == Function.CEIL) {
            funcStr = "ceil";
        }
        Expression floorOrCeilFunction =
            new MethodCall(
                new Literal(Literal.STRING, "java.lang.Math"),
                funcStr,
                new ExpressionList(argument));
        translator.addAssignmentStatement(
            stmtList,
            floorOrCeilFunction,
            call.getType(),
            varResult,
            true);
    }

    private void implementAbs(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands,
        Variable varResult,
        StatementList stmtList)
    {
        Expression argument =
            translator.convertPrimitiveAccess(
                operands[0],
                call.operands[0]);

        Expression absFunc =
            new MethodCall(
                new Literal(Literal.STRING, "java.lang.Math"),
                "abs",
                new ExpressionList(argument));
        translator.addAssignmentStatement(
            stmtList,
            absFunc,
            call.getType(),
            varResult,
            true);
    }

    /**
     * @sql.2003 Part 2 Section 6.27 General Rule 12
     */
    private void implementPower(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands,
        Variable varResult,
        StatementList stmtList)
    {
        assert builtinFunction == Function.POWER;
        Expression argument1 =
            translator.convertPrimitiveAccess(
                operands[0],
                call.operands[0]);
        Expression argument2 =
            translator.convertPrimitiveAccess(
                operands[1],
                call.operands[1]);

        Expression powFunc =
            new MethodCall(
                new Literal(Literal.STRING, "java.lang.Math"),
                "pow",
                new ExpressionList(argument1, argument2));

        // General 12 b)
        Expression condb =
            new BinaryExpression(
                new BinaryExpression(
                    argument1,
                    BinaryExpression.EQUAL,
                    Literal.constantZero()),
                BinaryExpression.LOGICAL_AND,
                new BinaryExpression(
                    argument2,
                    BinaryExpression.LESS,
                    Literal.constantZero()));

        // General 12 e)
        Expression conde =
            new BinaryExpression(
                new BinaryExpression(
                    argument1,
                    BinaryExpression.LESS,
                    Literal.constantZero()),
                BinaryExpression.LOGICAL_AND,
                new BinaryExpression(
                    argument2,
                    BinaryExpression.NOTEQUAL,
                    new MethodCall(
                        new Literal(Literal.STRING, "java.lang.Math"),
                        "floor",
                        new ExpressionList(argument2))));
        Expression condition =
            new BinaryExpression(
                condb,
                BinaryExpression.LOGICAL_OR,
                conde);
        StatementList stmtList1 = new StatementList();
        translator.addAssignmentStatement(
            stmtList1,
            powFunc,
            call.getType(),
            varResult,
            false);
        Statement ifStmt =
            new IfStatement(
                condition,
                getThrowStatementList(builtinFunction),
                stmtList1);
        stmtList.add(ifStmt);
    }

    private void implementExp(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands,
        Variable varResult,
        StatementList stmtList)
    {
        Expression argument =
            translator.convertPrimitiveAccess(
                operands[0],
                call.operands[0]);

        Expression expFunc =
            new MethodCall(
                new Literal(Literal.STRING, "java.lang.Math"),
                "exp",
                new ExpressionList(argument));
        translator.addAssignmentStatement(
            stmtList,
            expFunc,
            call.getType(),
            varResult,
            false);
    }

    /**
     * @sql.2003 Part 2 Section 6.27 General Rule 9
     */
    private void implementMod(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands,
        Variable varResult,
        StatementList stmtList)
    {
        Expression argument1 =
            translator.convertPrimitiveAccess(
                operands[0],
                call.operands[0]);
        Expression argument2 =
            translator.convertPrimitiveAccess(
                operands[1],
                call.operands[1]);

        Expression modFunc =
            new BinaryExpression(
                argument1,
                BinaryExpression.MOD,
                argument2);

        translator.addAssignmentStatement(
            stmtList,
            modFunc,
            call.getType(),
            varResult,
            true);
    }

    private void implementSubstring(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands,
        Variable varResult,
        StatementList stmtList)
    {
        boolean bHasLength = (operands.length > 2);
        translator.convertCastOrAssignmentWithStmtList(
            stmtList,
            call.toString(),
            call.getType(),
            call.operands[0].getType(),
            varResult,
            operands[0]);
        Expression intS =
            translator.convertPrimitiveAccess(
                operands[1],
                call.operands[1]);
        Expression intL = Literal.constantZero();
        if (bHasLength) {
            intL =
                translator.convertPrimitiveAccess(
                    operands[2],
                    call.operands[2]);
        }

        ExpressionList expList = new ExpressionList(intS, intL);
        if (bHasLength) {
            expList.add(Literal.constantTrue());
        } else {
            expList.add(Literal.constantFalse());
        }
        stmtList.add(
            new ExpressionStatement(
                new MethodCall(
                    varResult,
                    BytePointer.SUBSTRING_METHOD_NAME,
                    expList)));
    }

    private void implementOverlay(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands,
        Variable varResult,
        StatementList stmtList)
    {
        boolean bHasLength = (operands.length > 3);
        Expression intS =
            translator.convertPrimitiveAccess(
                operands[2],
                call.operands[2]);
        Expression intL = Literal.constantZero();
        if (bHasLength) {
            intL =
                translator.convertPrimitiveAccess(
                    operands[3],
                    call.operands[3]);
        }
        ExpressionList expList =
            new ExpressionList(
                operands[0],
                operands[1],
                intS);
        expList.add(intL);
        if (bHasLength) {
            expList.add(Literal.constantTrue());
        } else {
            expList.add(Literal.constantFalse());
        }
        stmtList.add(
            new ExpressionStatement(
                new MethodCall(
                    varResult,
                    BytePointer.OVERLAY_METHOD_NAME,
                    expList)));
    }

    private void implementConcat(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands,
        Variable varResult,
        StatementList stmtList)
    {
        stmtList.add(
            new ExpressionStatement(
                new MethodCall(
                    varResult,
                    BytePointer.CONCAT_METHOD_NAME,
                    new ExpressionList(operands[0], operands[1]))));
    }

    private void implementChangeCase(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands,
        Variable varResult,
        StatementList stmtList)
    {
        String funcName;
        switch (builtinFunction) {
        case LOWER:
            funcName = BytePointer.LOWER_METHOD_NAME;
            break;
        case UPPER:
            funcName = BytePointer.UPPER_METHOD_NAME;
            break;
        case INITCAP:
            funcName = BytePointer.INITCAP_METHOD_NAME;
            break;
        default:
            throw Util.unexpected(builtinFunction);
        }
        stmtList.add(
            new ExpressionStatement(
                new MethodCall(
                    varResult,
                    funcName,
                    new ExpressionList(operands[0]))));
    }

    private void implementTrim(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands,
        Variable varResult,
        StatementList stmtList)
    {
        Expression trimOrdinal =
            translator.convertPrimitiveAccess(
                operands[0],
                call.operands[0]);
        stmtList.add(
            new ExpressionStatement(
                new MethodCall(
                    varResult,
                    BytePointer.TRIM_METHOD_NAME,
                    new ExpressionList(
                        trimOrdinal,
                        operands[1],
                        operands[2]))));
    }

    private StatementList getThrowStatementList(Function function)
    {
        // String quotedName = "\"" + funcName + "\"";
        Util.discard(
            net.sf.farrago.resource.FarragoResource.instance()
            .InvalidFunctionArgument);
        return new StatementList(
            new ThrowStatement(
                new MethodCall(
                    new Literal(
                        Literal.STRING,
                        "net.sf.farrago.resource.FarragoResource.instance().InvalidFunctionArgument"),
                    "ex",
                    new ExpressionList(
                        Literal.makeLiteral(function.name())))));
    }
}

// End FarragoOJRexBuiltinImplementor.java
