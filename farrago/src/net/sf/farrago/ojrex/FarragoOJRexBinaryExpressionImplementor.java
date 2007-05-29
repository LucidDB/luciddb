/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
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
package net.sf.farrago.ojrex;

import net.sf.farrago.type.*;
import net.sf.farrago.type.runtime.*;

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.type.*;


/**
 * FarragoOJRexBinaryExpressionImplementor implements Farrago specifics of
 * {@link org.eigenbase.oj.rex.OJRexImplementor} for row expressions which can
 * be translated to instances of OpenJava {@link BinaryExpression}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoOJRexBinaryExpressionImplementor
    extends FarragoOJRexImplementor
{

    //~ Instance fields --------------------------------------------------------

    private final int ojBinaryExpressionOrdinal;

    //~ Constructors -----------------------------------------------------------

    public FarragoOJRexBinaryExpressionImplementor(
        int ojBinaryExpressionOrdinal)
    {
        this.ojBinaryExpressionOrdinal = ojBinaryExpressionOrdinal;
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
        Expression [] valueOperands = new Expression[2];

        for (int i = 0; i < 2; ++i) {
            valueOperands[i] =
                translator.convertPrimitiveAccess(
                    operands[i],
                    call.operands[i]);
        }

        if (!call.getType().isNullable()) {
            Expression expr = implementNotNull(translator, call, valueOperands);
            Statement ifstmt = checkOverflow(
                    expr,
                    call.getType());
            if (ifstmt != null) {
                translator.addStatement(ifstmt);
            }

            return expr;
        }

        Variable varResult = translator.createScratchVariable(call.getType());

        // special cases for three-valued logic
        if (ojBinaryExpressionOrdinal == BinaryExpression.LOGICAL_AND) {
            return
                implement3VL(
                    translator,
                    call,
                    operands,
                    valueOperands,
                    varResult,
                    "assignFromAnd3VL");
        } else if (ojBinaryExpressionOrdinal == BinaryExpression.LOGICAL_OR) {
            return
                implement3VL(
                    translator,
                    call,
                    operands,
                    valueOperands,
                    varResult,
                    "assignFromOr3VL");
        }

        Expression nullTest = null;
        for (int i = 0; i < 2; ++i) {
            nullTest =
                translator.createNullTest(
                    call.operands[i],
                    operands[i],
                    nullTest);
        }

        // TODO:  generalize to stuff other than NullablePrimitive
        Expression varResultValue =
            FarragoOJRexUtil.getValueAccessExpression(
                translator,
                call.getType(),
                varResult);

        Statement assignmentStmt =
            new ExpressionStatement(
                new AssignmentExpression(
                    varResultValue,
                    AssignmentExpression.EQUALS,
                    implementNotNull(translator, call, valueOperands)));

        Statement overflowStmt =
            checkOverflow(
                varResultValue,
                call.getType());
        StatementList stmtList =
            new StatementList(
                translator.createSetNullStatement(
                    varResult,
                    false),
                assignmentStmt);
        if (overflowStmt != null) {
            stmtList.add(overflowStmt);
        }

        if (nullTest == null) {
            // REVIEW jvs 11-Dec-2006:  Because of constant reduction,
            // something that was expected to be nullable got rewritten
            // to be definitely NOT NULL, and that's how we got here.
            // See LER-3482 example in unitsql/expressions/constants.sql.
            // Might be better to properly reevaluate the types in
            // the filter Rex tree during constant reduction instead.
            translator.addStatementsFromList(stmtList);
        } else {
            Statement ifStatement =
                new IfStatement(
                    nullTest,
                    new StatementList(
                        translator.createSetNullStatement(
                            varResult,
                            true)),
                    stmtList);

            translator.addStatement(ifStatement);
        }

        return varResult;
    }

    private Expression implement3VL(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands,
        Expression [] valueOperands,
        Expression varResult,
        String methodName)
    {
        ExpressionList expressionList = new ExpressionList();

        Expression n0 =
            translator.createNullTest(
                call.operands[0],
                operands[0],
                null);
        if (n0 == null) {
            n0 = Literal.makeLiteral(false);
        }

        Expression n1 =
            translator.createNullTest(
                call.operands[1],
                operands[1],
                null);
        if (n1 == null) {
            n1 = Literal.makeLiteral(false);
        }

        expressionList.add(n0);
        expressionList.add(valueOperands[0]);
        expressionList.add(n1);
        expressionList.add(valueOperands[1]);

        translator.addStatement(
            new ExpressionStatement(
                new MethodCall(
                    varResult,
                    methodName,
                    expressionList)));

        return varResult;
    }

    private Expression implementNotNull(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        // REVIEW:  heterogeneous operands?
        RelDataType type = call.operands[0].getType();

        FarragoTypeFactory factory = translator.getFarragoTypeFactory();

        Expression [] valueOperands = new Expression[2];
        for (int i = 0; i < 2; i++) {
            valueOperands[i] = operands[i];
        }

        // Special handling for boolean operands with >, <, <=, and >=
        // Use value of 1 for true, 0 for false, when doing these comparisons
        switch (ojBinaryExpressionOrdinal) {
        case BinaryExpression.GREATER:
        case BinaryExpression.GREATEREQUAL:
        case BinaryExpression.LESS:
        case BinaryExpression.LESSEQUAL:
            for (int i = 0; i < 2; i++) {
                if (call.operands[i].getType().getSqlTypeName()
                    == SqlTypeName.BOOLEAN) {
                    valueOperands[i] =
                        new ConditionalExpression(
                            operands[i],
                            Literal.makeLiteral(1),
                            Literal.makeLiteral(0));
                }
            }
        }

        if (factory.getClassForPrimitive(type) != null) {
            RelDataType returnType = call.getType();
            Expression expr =
                new BinaryExpression(valueOperands[0],
                    ojBinaryExpressionOrdinal,
                    valueOperands[1]);

            if ((returnType.getSqlTypeName() != SqlTypeName.BOOLEAN)
                && (factory.getClassForPrimitive(returnType) != null)) {
                // Cast to correct primitive return type so compiler is happy
                return
                    new CastExpression(
                        OJClass.forClass(
                            factory.getClassForPrimitive(returnType)),
                        expr);
            } else {
                return expr;
            }
        }
        Expression comparisonResultExp;
        if (SqlTypeUtil.inCharFamily(type)) {
            // TODO:  collation sequences, operators other than
            // comparison, etc.
            comparisonResultExp =
                new MethodCall(
                    OJClass.forClass(CharStringComparator.class),
                    "compareCharStrings",
                    new ExpressionList(operands[0], operands[1]));
        } else {
            comparisonResultExp =
                new MethodCall(
                    OJClass.forClass(VarbinaryComparator.class),
                    "compareVarbinary",
                    new ExpressionList(operands[0], operands[1]));
        }
        return
            new BinaryExpression(
                comparisonResultExp,
                ojBinaryExpressionOrdinal,
                Literal.makeLiteral(0));
    }

    private Statement checkOverflow(Expression expr, RelDataType returnType)
    {
        if (SqlTypeUtil.isApproximateNumeric(returnType)
            && (
                (ojBinaryExpressionOrdinal == BinaryExpression.DIVIDE)
                || (ojBinaryExpressionOrdinal == BinaryExpression.TIMES)
               )) {
            Statement ifStatement =
                new IfStatement(
                    new MethodCall(
                        new Literal(
                            Literal.STRING,
                            "Double"),
                        "isInfinite",
                        new ExpressionList(expr)),
                    new StatementList(
                        new ThrowStatement(
                            new MethodCall(
                                new Literal(
                                    Literal.STRING,
                                    "net.sf.farrago.resource.FarragoResource.instance().Overflow"),
                                "ex",
                                new ExpressionList()))));
            return ifStatement;
        } else {
            return null;
        }
    }
}

// End FarragoOJRexBinaryExpressionImplementor.java
