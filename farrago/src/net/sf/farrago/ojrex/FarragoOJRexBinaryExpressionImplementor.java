/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/
package net.sf.farrago.ojrex;

import net.sf.farrago.type.*;
import net.sf.farrago.type.runtime.*;

import openjava.mop.*;
import openjava.ptree.*;

import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;

/**
 * FarragoOJRexBinaryExpressionImplementor implements Farrago specifics of
 * {@link org.eigenbase.oj.rex.OJRexImplementor} for row expressions which can be translated to
 * instances of OpenJava {@link BinaryExpression}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoOJRexBinaryExpressionImplementor
    extends FarragoOJRexImplementor
{
    //~ Instance fields -------------------------------------------------------

    private final int ojBinaryExpressionOrdinal;

    //~ Constructors ----------------------------------------------------------

    public FarragoOJRexBinaryExpressionImplementor(
        int ojBinaryExpressionOrdinal)
    {
        this.ojBinaryExpressionOrdinal = ojBinaryExpressionOrdinal;
    }

    //~ Methods ---------------------------------------------------------------

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
                translator.convertPrimitiveAccess(operands[i], call.operands[i]);
        }

        if (!call.getType().isNullable()) {
            return implementNotNull(translator, call, valueOperands);
        }

        if (ojBinaryExpressionOrdinal == BinaryExpression.LOGICAL_AND)
            return implementNullableAnd(translator, call,
                operands, valueOperands);
        else if (ojBinaryExpressionOrdinal == BinaryExpression.LOGICAL_OR)
            return implementNullableOr(translator, call,
                operands, valueOperands);

        Variable varResult = translator.createScratchVariable(call.getType());

        Expression nullTest = null;
        for (int i = 0; i < 2; ++i) {
            nullTest =
                translator.createNullTest(call.operands[i], operands[i],
                    nullTest);
        }
        assert (nullTest != null);

        // TODO:  generalize to stuff other than NullablePrimitive
        Statement assignmentStmt =
            new ExpressionStatement(new AssignmentExpression(
                    new FieldAccess(varResult,
                        NullablePrimitive.VALUE_FIELD_NAME),
                    AssignmentExpression.EQUALS,
                    implementNotNull(translator, call, valueOperands)));

        Statement ifStatement =
            new IfStatement(nullTest,
                new StatementList(translator.createSetNullStatement(
                        varResult, true)),
                new StatementList(translator.createSetNullStatement(
                        varResult, false),
                    assignmentStmt));

        translator.addStatement(ifStatement);

        return varResult;
    }

    private Expression implementNullableAnd(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands,
        Expression [] valueOperands)
    {
        // Generator code for AND so that
        // null AND false = false
        // null AND true = null

        Variable varResult = translator.createScratchVariable(call.getType());
        Expression varValue = new FieldAccess(varResult,
                                            NullablePrimitive.VALUE_FIELD_NAME);

        Expression nullTest[] = new Expression[2];
        nullTest[0] =
            translator.createNullTest(call.operands[0], operands[0], null);
        nullTest[1] =
            translator.createNullTest(call.operands[1], operands[1], null);

        Statement setVarNullStmt =
            translator.createSetNullStatement(varResult, true);
        Statement setVarNotNullStmt =
            translator.createSetNullStatement(varResult, false);

        Statement assignmentFalseStmt =
            new ExpressionStatement(new AssignmentExpression(
                varValue,AssignmentExpression.EQUALS,
                Literal.makeLiteral(false)));

        Statement assignmentStmt =
            new ExpressionStatement(new AssignmentExpression(
                    varValue,
                    AssignmentExpression.EQUALS,
                    implementNotNull(translator, call, valueOperands)));

        if (nullTest[0] == null) {
            assert(nullTest[1] != null);

            Statement ifNullStatement =
                new IfStatement(valueOperands[0],
                    new StatementList(setVarNullStmt),
                    new StatementList(setVarNotNullStmt, assignmentFalseStmt));

            Statement ifStatement =
                new IfStatement(nullTest[1],
                    new StatementList(ifNullStatement),
                   new StatementList(setVarNotNullStmt, assignmentStmt));

            translator.addStatement(ifStatement);
        }
        else if (nullTest[1] == null) {
            assert(nullTest[0] != null);
            Statement ifNullStatement =
                new IfStatement(valueOperands[1],
                    new StatementList(setVarNullStmt),
                    new StatementList(setVarNotNullStmt, assignmentFalseStmt));

            Statement ifStatement =
                new IfStatement(nullTest[0],
                    new StatementList(ifNullStatement),
                   new StatementList(setVarNotNullStmt, assignmentStmt));

            translator.addStatement(ifStatement);
        }
        else {
            assert(nullTest[0] != null);
            assert(nullTest[1] != null);

            Statement ifNullStatement =
                new IfStatement(
                    new BinaryExpression(nullTest[1],
                                         BinaryExpression.LOGICAL_OR,
                                         valueOperands[1]),
                    new StatementList(setVarNullStmt),
                    new StatementList(setVarNotNullStmt, assignmentFalseStmt));

            Statement ifNotNullStatement =
                new IfStatement(nullTest[1],
                    new StatementList(
                        new IfStatement(valueOperands[0],
                            new StatementList(setVarNullStmt),
                            new StatementList(setVarNotNullStmt,
                                assignmentFalseStmt))),
                    new StatementList(setVarNotNullStmt, assignmentStmt));

            Statement ifStatement =
                new IfStatement(nullTest[0],
                    new StatementList(ifNullStatement),
                    new StatementList(ifNotNullStatement));

            translator.addStatement(ifStatement);
        }

        return varResult;

    }

    private Expression implementNullableOr(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands,
        Expression [] valueOperands)
    {
        // Generator code for OR so that
        // null OR false = null
        // null OR true = true

        Variable varResult = translator.createScratchVariable(call.getType());
        Expression varValue = new FieldAccess(varResult,
                                            NullablePrimitive.VALUE_FIELD_NAME);

        Expression nullTest[] = new Expression[2];
        nullTest[0] =
            translator.createNullTest(call.operands[0], operands[0], null);
        nullTest[1] =
            translator.createNullTest(call.operands[1], operands[1], null);

        Statement setVarNullStmt =
            translator.createSetNullStatement(varResult, true);
        Statement setVarNotNullStmt =
            translator.createSetNullStatement(varResult, false);

        Statement assignmentTrueStmt =
            new ExpressionStatement(new AssignmentExpression(
                varValue,AssignmentExpression.EQUALS,
                Literal.makeLiteral(true)));

        Statement assignmentStmt =
            new ExpressionStatement(new AssignmentExpression(
                    varValue,
                    AssignmentExpression.EQUALS,
                    implementNotNull(translator, call, valueOperands)));

        if (nullTest[0] == null) {
            assert(nullTest[1] != null);

            Statement ifNullStatement =
                new IfStatement(valueOperands[0],
                    new StatementList(setVarNotNullStmt, assignmentTrueStmt),
                    new StatementList(setVarNullStmt));

            Statement ifStatement =
                new IfStatement(nullTest[1],
                    new StatementList(ifNullStatement),
                    new StatementList(setVarNotNullStmt, assignmentStmt));

            translator.addStatement(ifStatement);
        }
        else if (nullTest[1] == null) {
            assert(nullTest[0] != null);
            Statement ifNullStatement =
                new IfStatement(valueOperands[1],
                    new StatementList(setVarNotNullStmt, assignmentTrueStmt),
                    new StatementList(setVarNullStmt));

            Statement ifStatement =
                new IfStatement(nullTest[0],
                    new StatementList(ifNullStatement),
                   new StatementList(setVarNotNullStmt, assignmentStmt));

            translator.addStatement(ifStatement);
        }
        else {
            assert(nullTest[0] != null);
            assert(nullTest[1] != null);

            Statement ifNullStatement =
                new IfStatement(
                    new BinaryExpression(nullTest[1],
                                         BinaryExpression.LOGICAL_OR,
                                         new UnaryExpression(
                                             UnaryExpression.NOT,
                                             valueOperands[1])),
                    new StatementList(setVarNullStmt),
                    new StatementList(setVarNotNullStmt, assignmentTrueStmt));

            Statement ifNotNullStatement =
                new IfStatement(
                    nullTest[1],
                    new StatementList(
                        new IfStatement(valueOperands[0],
                            new StatementList(setVarNotNullStmt,
                                assignmentTrueStmt),
                            new StatementList(setVarNullStmt))),
                    new StatementList(setVarNotNullStmt, assignmentStmt));

            Statement ifStatement =
                new IfStatement(nullTest[0],
                    new StatementList(ifNullStatement),
                    new StatementList(ifNotNullStatement));

            translator.addStatement(ifStatement);
        }

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
        if (factory.getClassForPrimitive(type) != null) {
            return new BinaryExpression(operands[0],
                ojBinaryExpressionOrdinal, operands[1]);
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
        return new BinaryExpression(
            comparisonResultExp,
            ojBinaryExpressionOrdinal,
            Literal.makeLiteral(0));
    }
}


// End FarragoOJRexBinaryExpressionImplementor.java
