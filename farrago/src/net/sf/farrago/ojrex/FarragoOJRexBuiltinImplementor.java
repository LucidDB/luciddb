/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2005-2005 Xiaoyang
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


/**
 * FarragoOJRexBuiltinImplementor implements Farrago specifics of
 * {@link org.eigenbase.oj.rex.OJRexImplementor} for builtin functions
 *
 * @author Xiaoyang Luo
 * @version $Id$
 */
public class FarragoOJRexBuiltinImplementor
    extends FarragoOJRexImplementor
{

    //~ Static fields/initializers ---------------------------------------------

    public static final int FLOOR_FUNCTION = 1;
    public static final int CEIL_FUNCTION = 2;
    public static final int ABS_FUNCTION = 3;
    public static final int POW_FUNCTION = 4;
    public static final int LN_FUNCTION = 5;
    public static final int LOG10_FUNCTION = 6;
    public static final int SUBSTRING_FUNCTION = 7;
    public static final int OVERLAY_FUNCTION = 8;
    public static final int MOD_FUNCTION = 9;
    public static final int EXP_FUNCTION = 10;
    public static final int CONCAT_OPERATOR = 11;
    public static final int TRIM_FUNCTION = 12;
    public static final int POSITION_FUNCTION = 13;
    public static final int CHAR_LENGTH_FUNCTION = 14;
    public static final int CHARACTER_LENGTH_FUNCTION = 15;
    public static final int UPPER_FUNCTION = 16;
    public static final int LOWER_FUNCTION = 17;
    public static final int INITCAP_FUNCTION = 18;
    public static final int CONVERT_FUNCTION = 19;
    public static final int TRANSLATE_FUNCTION = 20;

    //~ Instance fields --------------------------------------------------------

    protected int builtinFunction;

    //~ Constructors -----------------------------------------------------------

    public FarragoOJRexBuiltinImplementor(int function)
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
        Variable varResult = null;
        Expression nullTest = null;
        /*
        System.out.println(operands[0]); System.out.println(call.operands[0]);
         System.out.println(call.operands[0].getType());
         */

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
        case FLOOR_FUNCTION:
        case CEIL_FUNCTION:
            implementFloorCeil(translator, call, operands, varResult, stmtList);
            break;
        case ABS_FUNCTION:
            implementAbs(translator, call, operands, varResult, stmtList);
            break;
        case POW_FUNCTION:
            implementPow(translator, call, operands, varResult, stmtList);
            break;
        case LN_FUNCTION:
        case LOG10_FUNCTION:
            implementLog(translator, call, operands, varResult, stmtList);
            break;
        case SUBSTRING_FUNCTION:
            implementSubstring(translator, call, operands, varResult, stmtList);
            break;
        case OVERLAY_FUNCTION:
            implementOverlay(translator, call, operands, varResult, stmtList);
            break;
        case CONCAT_OPERATOR:
            implementConcat(translator, call, operands, varResult, stmtList);
            break;
        case MOD_FUNCTION:
            implementMod(translator, call, operands, varResult, stmtList);
            break;
        case EXP_FUNCTION:
            implementExp(translator, call, operands, varResult, stmtList);
            break;
        case LOWER_FUNCTION:
        case UPPER_FUNCTION:
        case INITCAP_FUNCTION:
            implementChangeCase(translator,
                call,
                operands,
                varResult,
                stmtList);
            break;
        case TRIM_FUNCTION:
            implementTrim(translator, call, operands, varResult, stmtList);
            break;
        case POSITION_FUNCTION:
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
        case CHAR_LENGTH_FUNCTION:
        case CHARACTER_LENGTH_FUNCTION:
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
        if (builtinFunction == LOG10_FUNCTION) {
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
        String funcName = "LN";
        if (builtinFunction == LOG10_FUNCTION) {
            funcName = "LOG10";
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
                getThrowStatementList(funcName));

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
        if (builtinFunction == CEIL_FUNCTION) {
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
     * @2003.sql Part 2 Section 6.27 General Rule 12
     */

    private void implementPow(
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
                getThrowStatementList("POW"),
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
     * @2003.sql Part 2 Section 6.27 General Rule 9
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
        String funcName = null;
        if (builtinFunction == LOWER_FUNCTION) {
            funcName = BytePointer.LOWER_METHOD_NAME;
        } else if (builtinFunction == UPPER_FUNCTION) {
            funcName = BytePointer.UPPER_METHOD_NAME;
        } else if (builtinFunction == INITCAP_FUNCTION) {
            funcName = BytePointer.INITCAP_METHOD_NAME;
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

    private StatementList getThrowStatementList(String funcName)
    {
        // String quotedName = "\"" + funcName + "\"";
        return
            new StatementList(
                new ThrowStatement(
                    new MethodCall(
                        new Literal(
                            Literal.STRING,
                            "net.sf.farrago.resource.FarragoResource.instance().InvalidFunctionArgument"),
                        "ex",
                        new ExpressionList(
                            Literal.makeLiteral(funcName)))));
    }
}

// End FarragoOJRexBuiltinImplementor.java
