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

import org.eigenbase.oj.rel.*;
import org.eigenbase.oj.rex.*;
import org.eigenbase.oj.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;


/**
 * FarragoRexToOJTranslator refines {@link RexToOJTranslator} with
 * Farrago-specifics.
 *
 *<p>
 *
 * NOTE jvs 22-June-2004: If you're scratching your head trying to understand
 * the code generation methods in this package, it might help to look at
 * examples of the generated code.  One way to do this is by turning on {@link
 * net.sf.farrago.trace.FarragoTrace#getDynamicTracer} and then examining the
 * code for generated classes after running queries.  See also the .ref files
 * under farrago/testlog/FarragoRexToOJTranslatorTest; these correspond to the
 * test cases in {@link net.sf.farrago.test.FarragoRexToOJTranslatorTest}.  You
 * can also add new test cases to that class, run the test, and examine the
 * output.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoRexToOJTranslator extends RexToOJTranslator
{
    //~ Instance fields -------------------------------------------------------

    private StatementList stmtList;
    private MemberDeclarationList memberList;
    private FarragoOJRexCastImplementor castImplementor;
    private OJClass ojNullablePrimitive;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a translator based on a {@link OJRexImplementorTable}.
     *
     * @param relImplementor implementation context
     *
     * @param contextRel relational expression which is the context for the
     * row-expressions which are to be translated
     *
     * @param implementorTable table of implementations for SQL operators
     *
     * @param stmtList statement list for side-effects of translation
     *
     * @param memberList member list for class-level state required by
     * translation
     */
    public FarragoRexToOJTranslator(
        JavaRelImplementor relImplementor,
        RelNode contextRel,
        OJRexImplementorTable implementorTable,
        StatementList stmtList,
        MemberDeclarationList memberList)
    {
        super(relImplementor, contextRel, implementorTable);
        this.stmtList = stmtList;
        this.memberList = memberList;

        // keep a reference to the implementor for CAST, which
        // is needed for implementing assignments also
        castImplementor =
            (FarragoOJRexCastImplementor) getImplementorTable().get(SqlOperatorTable
                    .std().castFunc);

        ojNullablePrimitive = OJClass.forClass(NullablePrimitive.class);
    }

    //~ Methods ---------------------------------------------------------------

    public void addMember(MemberDeclaration member)
    {
        memberList.add(member);
    }

    public void addStatement(Statement stmt)
    {
        stmtList.add(stmt);
    }

    // implement RexVisitor
    public void visitContextVariable(RexContextVariable contextVariable)
    {
        setTranslation(
            convertVariable(
                contextVariable,
                "getContextVariable_" + contextVariable.getName(),
                new ExpressionList()));
    }

    // implement RexVisitor
    public void visitDynamicParam(RexDynamicParam dynamicParam)
    {
        setTranslation(
            convertVariable(
                dynamicParam,
                "getDynamicParamValue",
                new ExpressionList(Literal.makeLiteral(dynamicParam.index))));
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
                            getRelImplementor().getConnectionVariable(),
                            accessorName,
                            accessorArgList)))));
        return new CastExpression(
            OJUtil.typeToOJClass(rexVariable.getType()),
            variable);
    }

    // override RexToOJTranslator
    public void visitLiteral(RexLiteral literal)
    {
        super.visitLiteral(literal);
        RelDataType type = literal.getType();
        if (type instanceof FarragoDateTimeType) {
            // TODO jvs 22-May-2004: Need to do something similar for anything
            // which requires a holder class at runtime (e.g. VARCHAR),
            // using a more general test than instanceof FarragoDateTimeType.
            // Also, initialize once and only once.
            setTranslation(
                castImplementor.convertCastToAssignableValue(
                    this,
                    type,
                    type,
                    null,
                    getTranslation()));
        }
    }

    public Variable createScratchVariable(
        OJClass ojClass,
        ExpressionList exprs,
        MemberDeclarationList mdlst)
    {
        Variable variable = getRelImplementor().newVariable();
        memberList.add(
            new FieldDeclaration(
                new ModifierList(ModifierList.EMPTY),
                TypeName.forOJClass(ojClass),
                variable.toString(),
                new AllocationExpression(
                    TypeName.forOJClass(ojClass),
                    exprs,
                    mdlst)));
        return variable;
    }

    public Variable createScratchVariable(RelDataType type)
    {
        OJClass ojClass = OJUtil.typeToOJClass(type);
        return createScratchVariable(ojClass, null, null);
    }

    public Statement createSetNullStatement(
        Expression varResult,
        boolean isNull)
    {
        return new ExpressionStatement(
            new MethodCall(
                varResult,
                NullableValue.NULL_IND_MUTATOR_NAME,
                new ExpressionList(Literal.makeLiteral(isNull))));
    }

    public Expression createNullTest(
        RexNode node,
        Expression originalOperand,
        Expression nullTest)
    {
        if (node.getType().isNullable()) {
            Expression newNullTest;
            newNullTest =
                new MethodCall(
                    originalOperand,
                    NullableValue.NULL_IND_ACCESSOR_NAME,
                    new ExpressionList());

            if (nullTest == null) {
                nullTest = newNullTest;
            } else {
                nullTest =
                    new BinaryExpression(nullTest,
                        BinaryExpression.LOGICAL_OR, newNullTest);
            }
        }
        return nullTest;
    }

    public boolean isNullablePrimitive(RelDataType type)
    {
        OJClass ojClass = OJUtil.typeToOJClass(type);
        return ojNullablePrimitive.isAssignableFrom(ojClass);
    }

    public FieldAccess convertFieldAccess(
        Variable variable,
        RelDataTypeField field)
    {
        final String javaFieldName =
            Util.toJavaId(
                field.getName(),
                field.getIndex());
        return new FieldAccess(variable, javaFieldName);
    }

    public Expression convertPrimitiveAccess(
        Expression expr,
        RexNode op)
    {
        assert (op.getType() instanceof FarragoAtomicType);
        FarragoAtomicType type = (FarragoAtomicType) op.getType();
        if (type.requiresValueAccess()) {
            return new FieldAccess(expr, NullablePrimitive.VALUE_FIELD_NAME);
        } else {
            return expr;
        }
    }

    public Expression convertCastOrAssignment(
        RelDataType lhsType,
        RelDataType rhsType,
        Expression lhsExp,
        Expression rhsExp)
    {
        return castImplementor.convertCastOrAssignment(this, lhsType, rhsType,
            lhsExp, rhsExp);
    }
}


// End FarragoRexToOJTranslator.java
