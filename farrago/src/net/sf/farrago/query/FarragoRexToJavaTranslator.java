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

package net.sf.farrago.query;

import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.rex.*;
import net.sf.saffron.core.*;
import net.sf.saffron.oj.*;
import net.sf.saffron.oj.util.*;
import net.sf.farrago.type.*;
import net.sf.farrago.runtime.*;
import openjava.mop.*;
import openjava.ptree.*;

/**
 * FarragoRexToJavaTranslator is an eager assistant for FarragoRelImplementor.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FarragoRexToJavaTranslator extends RelImplementor.Translator
{
    private FarragoRelImplementor farragoImplementor;
    private StatementList stmtList;
    private MemberDeclarationList memberList;

    FarragoRexToJavaTranslator(
        FarragoRelImplementor implementor,
        SaffronRel rel,
        StatementList stmtList,
        MemberDeclarationList memberList)
    {
        super(implementor,rel);
        farragoImplementor = implementor;
        this.stmtList = stmtList;
        this.memberList = memberList;
    }

    // override RelImplementor.Translator
    protected Expression convertCall(RexCall call, Expression[] operands)
    {
        Integer binaryExpNum = (Integer)
            implementor.mapRexBinaryToOpenJava.get(call.op);
        if (binaryExpNum != null) {
            return convertBinary(
                call,binaryExpNum.intValue(),operands);
        }
        if (call.op.equals(
                implementor.getRexBuilder().operatorTable.isTrueOperator))
        {
            return convertIsTrue(call,operands[0]);
        }
        if (call.op.equals(
                implementor.getRexBuilder().operatorTable.isNotNullOperator))
        {
            return convertIsNotNull(call,operands[0]);
        }
        if (!call.op.equals(implementor.getRexBuilder().funcTab.cast))
        {
            return super.convertCall(call,operands);
        }

        SaffronType lhsType = call.getType();
        SaffronType rhsType = call.operands[0].getType();
        Expression rhsExp = operands[0];
        return convertCastOrAssignment(lhsType,rhsType,null,rhsExp);
    }

    // override RelImplementor.Translator
    protected Expression convertContextVariable(
        RexContextVariable contextVariable)
    {
        return convertVariable(
            contextVariable,
            "getContextVariable_" + contextVariable.getName(),
            new ExpressionList());
    }

    // override RelImplementor.Translator
    protected Expression convertDynamicParam(RexDynamicParam dynamicParam)
    {
        return convertVariable(
            dynamicParam,
            "getDynamicParamValue",
            new ExpressionList(
                Literal.makeLiteral(dynamicParam.index)));
    }

    private Expression convertVariable(
        RexVariable rexVariable,
        String accessorName,
        ExpressionList accessorArgList)
    {
        Variable variable = createScratchVariable(rexVariable.getType());
        stmtList.add(
            new ExpressionStatement(
                new MethodCall(
                    variable,
                    AssignableValue.ASSIGNMENT_METHOD_NAME,
                    new ExpressionList(
                        new MethodCall(
                            farragoImplementor.preparingStmt.
                            getConnectionVariable(),
                            accessorName,
                            accessorArgList)))));
        return new CastExpression(
            OJUtil.typeToOJClass(rexVariable.getType()),
            variable);
    }

    protected Expression convertByteArrayLiteral(byte [] bytes)
    {
        // NOTE:  we override Saffron's default because DynamicJava
        // has a bug dealing with anonymous byte [] initializers
        Variable variable = farragoImplementor.newVariable();
        memberList.add(
            new FieldDeclaration(
                new ModifierList(ModifierList.PRIVATE),
                TypeName.forClass(byte [].class),
                variable.toString(),
                new ArrayAllocationExpression(
                    TypeName.forOJClass(OJSystem.BYTE),
                    new ExpressionList(
                        Literal.makeLiteral(bytes.length)))));
        // TODO:  generate this code in class static init block instead
        for (int i = 0; i < bytes.length; ++i) {
            stmtList.add(
                new ExpressionStatement(
                    new AssignmentExpression(
                        new ArrayAccess(variable,Literal.makeLiteral(i)),
                        AssignmentExpression.EQUALS,
                        Literal.makeLiteral(bytes[i]))));
        }
        return variable;
    }

    private Expression convertIsTrue(RexCall call,Expression operand)
    {
        if (call.operands[0].getType().isNullable()) {
            return new BinaryExpression(
                new UnaryExpression(
                    UnaryExpression.NOT,
                    new MethodCall(
                        operand,
                        NullableValue.NULL_IND_ACCESSOR_NAME,
                        new ExpressionList())),
                BinaryExpression.LOGICAL_AND,
                new FieldAccess(
                    operand,
                    NullablePrimitive.VALUE_FIELD_NAME));
        } else {
            return operand;
        }
    }

    private Expression convertIsNotNull(RexCall call,Expression operand)
    {
        if (call.operands[0].getType().isNullable()) {
            return new UnaryExpression(
                UnaryExpression.NOT,
                new MethodCall(
                    operand,
                    NullableValue.NULL_IND_ACCESSOR_NAME,
                    new ExpressionList()));
        } else {
            return Literal.constantTrue();
        }
    }

    private Expression convertCastNull(
        SaffronType lhsType,
        SaffronType rhsType,
        Expression lhsExp,
        Expression rhsExp)
    {
        if (lhsExp == null) {
            lhsExp = createScratchVariable(lhsType);
        }
        if (lhsType instanceof FarragoType) {
            stmtList.add(
                createSetNullStatement(lhsExp,true));
        } else {
            stmtList.add(
                new ExpressionStatement(
                    new AssignmentExpression(
                        lhsExp,
                        AssignmentExpression.EQUALS,
                        rhsExp)));
        }
        return lhsExp;
    }

    private Expression convertCastPrimitiveToNullablePrimitive(
        SaffronType lhsType,
        SaffronType rhsType,
        Expression lhsExp,
        Expression rhsExp)
    {
        FarragoPrimitiveType primitiveType =
            (FarragoPrimitiveType) lhsType;
        if (lhsExp == null) {
            lhsExp = createScratchVariable(lhsType);
        }
        Expression rhsIsNull;
        if (rhsType.isNullable()) {
            rhsIsNull = new FieldAccess(
                rhsExp,
                NullablePrimitive.NULL_IND_FIELD_NAME);
            rhsExp = new FieldAccess(
                rhsExp,
                NullablePrimitive.VALUE_FIELD_NAME);
        } else {
            rhsIsNull = Literal.constantFalse();
        }
        stmtList.add(
            new ExpressionStatement(
                new AssignmentExpression(
                    new FieldAccess(
                        lhsExp,
                        NullablePrimitive.NULL_IND_FIELD_NAME),
                    AssignmentExpression.EQUALS,
                    rhsIsNull)));
        stmtList.add(
            new ExpressionStatement(
                new AssignmentExpression(
                    new FieldAccess(
                        lhsExp,
                        NullablePrimitive.VALUE_FIELD_NAME),
                    AssignmentExpression.EQUALS,
                    new CastExpression(
                        OJClass.forClass(
                            primitiveType.getClassForPrimitive()),
                        rhsExp))));
        return lhsExp;
    }

    private Expression convertCastToNotNullPrimitive(
        SaffronType lhsType,
        SaffronType rhsType,
        Expression lhsExp,
        Expression rhsExp)
    {
        if (isNullablePrimitive(rhsType)) {
            rhsExp = new FieldAccess(
                rhsExp,
                NullablePrimitive.VALUE_FIELD_NAME);
        }
        OJClass lhsClass = OJUtil.typeToOJClass(lhsType);
        rhsExp = new CastExpression(lhsClass,rhsExp);
        if (lhsExp == null) {
            return rhsExp;
        } else {
            stmtList.add(
                new ExpressionStatement(
                    new AssignmentExpression(
                        lhsExp,
                        AssignmentExpression.EQUALS,
                        rhsExp)));
            return lhsExp;
        }
    }

    private Expression convertCastToAssignableValue(
        SaffronType lhsType,
        SaffronType rhsType,
        Expression lhsExp,
        Expression rhsExp)
    {
        if (lhsExp == null) {
            lhsExp = createScratchVariable(lhsType);
        }
        stmtList.add(
            new ExpressionStatement(
                new MethodCall(
                    lhsExp,
                    AssignableValue.ASSIGNMENT_METHOD_NAME,
                    new ExpressionList(rhsExp))));

        if (lhsType instanceof FarragoPrecisionType) {

            // may need to pad or truncate
            FarragoPrecisionType lhsPrecisionType =
                (FarragoPrecisionType) lhsType;

            if (rhsType instanceof FarragoPrecisionType) {
                // we may be able to skip pad/truncate based on
                // known facts about source and target precisions
                FarragoPrecisionType rhsPrecisionType =
                    (FarragoPrecisionType) rhsType;

                if (lhsPrecisionType.isBoundedVariableWidth()) {
                    if (lhsPrecisionType.getPrecision() >=
                        rhsPrecisionType.getPrecision())
                    {
                        // target precision is greater than source
                        // precision, so truncation is impossible
                        // and we can skip adjustment
                        return lhsExp;
                    }
                } else {
                    if (lhsPrecisionType.getPrecision()
                        == rhsPrecisionType.getPrecision())
                    {
                        // target is fixed-width, and precisions are the same,
                        // so there's no adjustment needed
                        return lhsExp;
                    }
                }
            }

            // determine target precision
            Expression precisionExp = Literal.makeLiteral(
                lhsPrecisionType.getPrecision());

            // need to pad only for fixed width
            Expression needPadExp = Literal.makeLiteral(
                !lhsPrecisionType.isBoundedVariableWidth());

            // pad character is 0 for binary, space for character
            Expression padByteExp;
            if (lhsPrecisionType.getCharsetName() == null) {
                padByteExp = Literal.makeLiteral(0);
            } else {
                padByteExp = new CastExpression(
                    OJSystem.BYTE,
                    Literal.makeLiteral(' '));
            }

            // generate the call to do the job
            stmtList.add(
                new ExpressionStatement(
                    new MethodCall(
                        lhsExp,
                        BytePointer.ENFORCE_PRECISION_METHOD_NAME,
                        new ExpressionList(
                            precisionExp,
                            needPadExp,
                            padByteExp))));
        }
        return lhsExp;
    }

    Expression convertCastOrAssignment(
        SaffronType lhsType,
        SaffronType rhsType,
        Expression lhsExp,
        Expression rhsExp)
    {
        // TODO:  overflow etc.

        if (!lhsType.isNullable() && rhsType.isNullable()) {
            // generate code which will throw an exception whenever an attempt
            // is made to cast a null value to a NOT NULL type

            if (SaffronTypeFactoryImpl.isJavaType(rhsType)) {
                stmtList.add(
                    new ExpressionStatement(
                        new MethodCall(
                            farragoImplementor.preparingStmt.
                            getConnectionVariable(),
                            "checkNotNull",
                            new ExpressionList(rhsExp))));
            } else {
                Variable variable = createScratchVariable(rhsType);
                stmtList.add(
                    new ExpressionStatement(
                        new AssignmentExpression(
                            variable,
                            AssignmentExpression.EQUALS,
                            rhsExp)));
                // TODO:  provide exception context
                stmtList.add(
                    new ExpressionStatement(
                        new MethodCall(
                            farragoImplementor.preparingStmt.
                            getConnectionVariable(),
                            "checkNotNull",
                            new ExpressionList(variable))));
                rhsExp = variable;
            }
        }

        // special case for source explicit null
        OJClass rhsClass = OJUtil.typeToOJClass(rhsType);
        if (rhsClass == OJSystem.NULLTYPE) {
            if (lhsType.isNullable()) {
                return convertCastNull(lhsType,rhsType,lhsExp,rhsExp);
            } else {
                // null check code generated previously means we never
                // have to worry about this case
                return rhsExp;
            }
        }

        if (isNullablePrimitive(lhsType)) {
            if (rhsType instanceof FarragoPrimitiveType) {
                return convertCastPrimitiveToNullablePrimitive(
                    lhsType,rhsType,lhsExp,rhsExp);
            } else {
                return convertCastToAssignableValue(
                    lhsType,rhsType,lhsExp,rhsExp);
            }
        } else if (lhsType instanceof FarragoPrimitiveType) {
            return convertCastToNotNullPrimitive(
                lhsType,rhsType,lhsExp,rhsExp);
        } else {
            return convertCastToAssignableValue(
                lhsType,rhsType,lhsExp,rhsExp);
        }
    }

    private Variable createScratchVariable(SaffronType type)
    {
        OJClass ojClass = OJUtil.typeToOJClass(type);
        Variable variable = farragoImplementor.newVariable();
        memberList.add(
            new FieldDeclaration(
                new ModifierList(ModifierList.PRIVATE),
                TypeName.forOJClass(ojClass),
                variable.toString(),
                new AllocationExpression(
                    ojClass,
                    new ExpressionList())));
        return variable;
    }

    private Expression convertBinary(
        RexCall call,
        int binaryKind,
        Expression [] operands)
    {
        // TODO:  overflow detection, type promotion, etc.  Also, if global
        // analysis is used on the expression, we can reduce the number of
        // null-tests.

        if (!call.getType().isNullable()) {
            return convertBinaryNotNull(call,binaryKind,operands);
        }

        Variable varResult = createScratchVariable(call.getType());

        Expression [] newOperands = new Expression[2];
        Expression nullTest = null;
        for (int i = 0; i < 2; ++i) {
            nullTest = createNullTest(call,operands,newOperands,i,nullTest);
        }
        assert(nullTest != null);

        // TODO:  generalize to stuff other than NullablePrimitive
        Statement assignmentStmt = new ExpressionStatement(
            new AssignmentExpression(
                new FieldAccess(varResult,NullablePrimitive.VALUE_FIELD_NAME),
                AssignmentExpression.EQUALS,
                convertBinaryNotNull(call,binaryKind,newOperands)));

        Statement ifStatement = new IfStatement(
            nullTest,
            new StatementList(
                createSetNullStatement(varResult,true)),
            new StatementList(
                createSetNullStatement(varResult,false),
                assignmentStmt));

        stmtList.add(ifStatement);

        return varResult;
    }

    private Expression convertBinaryNotNull(
        RexCall call,int binaryKind,Expression [] operands)
    {
        // REVIEW:  heterogeneous operands?
        SaffronType type = call.operands[0].getType();
        if (type instanceof FarragoPrimitiveType) {
            return new BinaryExpression(
                operands[0],
                binaryKind,
                operands[1]);
        }
        Expression comparisonResultExp;
        if (((FarragoPrecisionType) type).getCharsetName() != null) {
            // TODO:  collation sequences, operators other than
            // comparison, etc.
            comparisonResultExp =
                new MethodCall(
                    OJClass.forClass(CharStringComparator.class),
                    "compareCharStrings",
                    new ExpressionList(operands[0],operands[1]));
        } else {
            comparisonResultExp =
                new MethodCall(
                    OJClass.forClass(VarbinaryComparator.class),
                    "compareVarbinary",
                    new ExpressionList(operands[0],operands[1]));
        }
        return new BinaryExpression(
            comparisonResultExp,
            binaryKind,
            Literal.makeLiteral(0));
    }

    private Statement createSetNullStatement(
        Expression varResult,boolean isNull)
    {
        return new ExpressionStatement(
            new MethodCall(
                varResult,
                NullableValue.NULL_IND_MUTATOR_NAME,
                new ExpressionList(Literal.makeLiteral(isNull))));
    }

    private Expression createNullTest(
        RexCall call,
        Expression [] originalOperands,Expression [] newOperands,
        int i,Expression nullTest)
    {
        if (call.operands[i].getType().isNullable()) {
            Expression newNullTest = new MethodCall(
                originalOperands[i],
                NullableValue.NULL_IND_ACCESSOR_NAME,
                new ExpressionList());
            if (nullTest == null) {
                nullTest = newNullTest;
            } else {
                nullTest = new BinaryExpression(
                    nullTest,
                    BinaryExpression.LOGICAL_OR,
                    newNullTest);
            }
            OJClass ojClass = OJUtil.typeToOJClass(call.operands[i].getType());
            if (farragoImplementor.ojNullablePrimitive.isAssignableFrom(
                    ojClass))
            {
                newOperands[i] = new FieldAccess(
                    originalOperands[i],
                    NullablePrimitive.VALUE_FIELD_NAME);
            } else {
                newOperands[i] = originalOperands[i];
            }
        } else {
            newOperands[i] = originalOperands[i];
        }
        return nullTest;
    }

    boolean isNullablePrimitive(SaffronType type)
    {
        OJClass ojClass = OJUtil.typeToOJClass(type);
        return farragoImplementor.ojNullablePrimitive.isAssignableFrom(ojClass);
    }
}

// End FarragoRexToJavaTranslator.java
