/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
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

import net.sf.farrago.type.*;
import net.sf.farrago.type.runtime.*;
import net.sf.saffron.core.SaffronField;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.core.SaffronTypeFactoryImpl;
import net.sf.saffron.oj.rel.RexToJavaTranslator;
import net.sf.saffron.oj.util.OJUtil;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.rex.*;
import net.sf.saffron.util.Util;
import openjava.mop.OJClass;
import openjava.mop.OJSystem;
import openjava.ptree.*;

/**
 * FarragoRexToJavaTranslator is an eager assistant for FarragoRelImplementor.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FarragoRexToJavaTranslator extends RexToJavaTranslator
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

    public void addMember(MemberDeclaration member) {
        memberList.add(member);
    }

    public void addStatement(Statement stmt) {
        stmtList.add(stmt);
    }

    // override JavaRelImplementor.Translator
    protected Expression convertCall(RexCall call, Expression[] operands)
    {
        Integer binaryExpNum = 
            getImplementor().getBinaryExpressionOrdinal(call.op);
        if (binaryExpNum != null) {
            return convertBinary(call,binaryExpNum.intValue(),operands);
        }
        if (call.op.equals(
                getImplementor().getRexBuilder()._opTab.isTrueOperator))
        {
            return convertIsTrue(call,operands[0]);
        }
        if (call.op.equals(
                getImplementor().getRexBuilder().
                _opTab.isNotNullOperator))
        {
            return convertIsNotNull(call,operands[0]);
        }
        if (call.op.equals(
                getImplementor().getRexBuilder()._opTab.rowConstructor))
        {
            return convertRowConstructor(call,operands);
        }
        if (!call.op.equals(getImplementor().getRexBuilder()._opTab.castFunc))
        {
            return super.convertCall(call,operands);
        }

        SaffronType lhsType = call.getType();
        SaffronType rhsType = call.operands[0].getType();
        Expression rhsExp = operands[0];
        return convertCastOrAssignment(lhsType,rhsType,null,rhsExp);
    }

    // override JavaRelImplementor.Translator
    protected Expression convertContextVariable(
        RexContextVariable contextVariable)
    {
        return convertVariable(
            contextVariable,
            "getContextVariable_" + contextVariable.getName(),
            new ExpressionList());
    }

    // override JavaRelImplementor.Translator
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

    // override JavaRelImplementor.Translator
    protected Expression convertLiteral(RexLiteral literal)
    {
        SaffronType type = literal.getType();
        if (type instanceof FarragoDateTimeType) {
            // TODO jvs 22-May-2004: Need to do something similar for anything
            // which requires a holder class at runtime (e.g. VARCHAR),
            // using a more general test than instanceof FarragoDateTimeType.
            // Also, initialize once and only once.
            return convertCastToAssignableValue(
                type,
                type,
                null,
                super.convertLiteral(literal));
        } else {
            return super.convertLiteral(literal);
        }
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
        FarragoAtomicType lhsAtomicType = (FarragoAtomicType) lhsType;
        stmtList.add(
            new ExpressionStatement(
                new AssignmentExpression(
                    new FieldAccess(
                        lhsExp,
                        NullablePrimitive.VALUE_FIELD_NAME),
                    AssignmentExpression.EQUALS,
                    new CastExpression(
                        OJClass.forClass(lhsAtomicType.getClassForPrimitive()),
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
        return convertDirectAssignment(lhsExp,rhsExp);
    }

    private Expression convertDirectAssignment(
        Expression lhsExp,Expression rhsExp)
    {
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
        } else if (lhsType.isProject()) {
            assert(rhsType.isProject());
            
            // TODO jvs 27-May-2004:  relax this assert and deal with
            // conversions, null checks, etc.
            assert(lhsType.equals(rhsType));

            return convertDirectAssignment(lhsExp,rhsExp);
        } else {
            return convertCastToAssignableValue(
                lhsType,rhsType,lhsExp,rhsExp);
        }
    }

    public Variable createScratchVariable(OJClass ojClass, ExpressionList exprs,
                                          MemberDeclarationList mdlst)
    {
        Variable variable = farragoImplementor.newVariable();
        memberList.add(
            new FieldDeclaration(
                new ModifierList(ModifierList.PRIVATE),
                TypeName.forOJClass(ojClass),
                variable.toString(),
                new AllocationExpression(TypeName.forOJClass(ojClass), exprs, mdlst)));
        return variable;
    }

    public Variable createScratchVariable(SaffronType type)
    {
        OJClass ojClass = OJUtil.typeToOJClass(type);
        return createScratchVariable(ojClass, null, null);
    }

    private Expression convertBinary(
        RexCall call,
        int binaryKind,
        Expression [] operands)
    {
        // TODO:  overflow detection, type promotion, etc.  Also, if global
        // analysis is used on the expression, we can reduce the number of
        // null-tests.
        Expression [] valueOperands = new Expression[2];

        for (int i = 0; i < 2; ++i) {
            valueOperands[i] = convertPrimitiveAccess(operands[i],call.operands[i]);
        }

        if (!call.getType().isNullable()) {
            return convertBinaryNotNull(call,binaryKind,valueOperands);
        }

        Variable varResult = createScratchVariable(call.getType());

        Expression nullTest = null;
        for (int i = 0; i < 2; ++i) {
            nullTest = createNullTest(call.operands[i],operands[i],nullTest);
        }
        assert(nullTest != null);

        // TODO:  generalize to stuff other than NullablePrimitive
        Statement assignmentStmt = new ExpressionStatement(
            new AssignmentExpression(
                new FieldAccess(varResult,NullablePrimitive.VALUE_FIELD_NAME),
                AssignmentExpression.EQUALS,
                convertBinaryNotNull(call,binaryKind,valueOperands)));

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

    /**
     * Handle date/time - which is always of type nullable primitive.
     **/
    private Expression convertPrimitiveAccess(Expression expr, RexNode op)
    {
        assert (op.getType() instanceof FarragoAtomicType);
        FarragoAtomicType type = (FarragoAtomicType)op.getType();
        if (type.requiresValueAccess()) {
            return new FieldAccess(
                expr,
                NullablePrimitive.VALUE_FIELD_NAME);
        } else {
           return expr;
        }
    }

    private Expression convertBinaryNotNull(
        RexCall call,int binaryKind,Expression [] operands)
    {
        // REVIEW:  heterogeneous operands?
        assert (call.operands[0].getType() instanceof FarragoAtomicType);
        FarragoAtomicType type = (FarragoAtomicType) call.operands[0].getType();

        if (type.hasClassForPrimitive()) {
            return new BinaryExpression(
                operands[0],
                binaryKind,
                operands[1]);
        }
        Expression comparisonResultExp;
        assert (type instanceof FarragoPrecisionType);
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
        RexNode node,
        Expression  originalOperand,
        Expression nullTest)
    {
        if (node.getType().isNullable()) {
            Expression newNullTest;
            newNullTest = new MethodCall(
                    originalOperand,
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
        }
        return nullTest;
    }

    private Expression convertRowConstructor(
        RexCall call,Expression [] operands)
    {
        SaffronType rowType = call.getType();
        Variable variable = createScratchVariable(rowType);
        SaffronField [] fields = rowType.getFields();
        for (int i = 0; i < operands.length; ++i) {
            convertCastOrAssignment(
                fields[i].getType(),
                call.operands[i].getType(),
                // TODO jvs 27-May-2004:  proper field name translation
                new FieldAccess(variable,fields[i].getName()),
                operands[i]);
        }
        return variable;
    }

    boolean isNullablePrimitive(SaffronType type)
    {
        OJClass ojClass = OJUtil.typeToOJClass(type);
        return farragoImplementor.ojNullablePrimitive.isAssignableFrom(ojClass);
    }

    public Expression convertToJava(Expression expr, Class clazz) {
        throw net.sf.saffron.util.Util.needToImplement(this);
    }
}

// End FarragoRexToJavaTranslator.java
