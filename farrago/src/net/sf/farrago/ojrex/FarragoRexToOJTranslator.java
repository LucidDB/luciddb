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

import net.sf.farrago.catalog.*;
import net.sf.farrago.type.*;
import net.sf.farrago.type.runtime.*;

import openjava.mop.*;
import openjava.ptree.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.oj.rex.*;
import org.eigenbase.oj.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.type.*;
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

    private final FarragoRepos repos;
    private StatementList stmtList;
    private final MemberDeclarationList memberList;
    private final FarragoOJRexCastImplementor castImplementor;
    private final OJClass ojNullablePrimitive;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a translator based on a {@link OJRexImplementorTable}.
     *
     * @param repos repository
     * @param relImplementor implementation context
     * @param contextRel relational expression which is the context for the
     *    row-expressions which are to be translated
     * @param implementorTable table of implementations for SQL operators
     * @param stmtList statement list for side-effects of translation
     * @param memberList member list for class-level state required by
     * @param program Program, may be null
     */
    public FarragoRexToOJTranslator(
        FarragoRepos repos,
        JavaRelImplementor relImplementor,
        RelNode contextRel,
        OJRexImplementorTable implementorTable,
        StatementList stmtList,
        MemberDeclarationList memberList,
        RexProgram program)
    {
        super(relImplementor, contextRel, implementorTable);
        this.repos = repos;
        this.stmtList = stmtList;
        this.memberList = memberList;

        // keep a reference to the implementor for CAST, which
        // is needed for implementing assignments also
        castImplementor = (FarragoOJRexCastImplementor)
            getImplementorTable().get(
                SqlStdOperatorTable.castFunc);

        ojNullablePrimitive = OJClass.forClass(NullablePrimitive.class);
        if (program != null) {
            pushProgram(program);
        }
    }

    //~ Methods ---------------------------------------------------------------
    //

    public RexToOJTranslator push(StatementList stmtList)
    {
        // TODO: jhyde, 2005/11/5: The child translator should inherit the
        //  mapping of expressions to variables, which means it should have a
        //  pointer to this translator, or at least some of its state.
        //  Otherwise common expressions will be translated and evaluated
        //  more than once.
        return new FarragoRexToOJTranslator(
            repos, getRelImplementor(), getContextRel(), getImplementorTable(),
            stmtList, memberList, getProgram());
    }

    public void addMember(MemberDeclaration member)
    {
        memberList.add(member);
    }

    public void addStatement(Statement stmt)
    {
        stmtList.add(stmt);
    }

    // implement RexVisitor
    public Expression visitDynamicParam(RexDynamicParam dynamicParam)
    {
        return setTranslation(
            convertVariable(
                dynamicParam.getType(),
                "getDynamicParamValue",
                new ExpressionList(
                    Literal.makeLiteral(
                        dynamicParam.getIndex()))));
    }

    public Expression convertVariable(
        RelDataType type,
        String accessorName,
        ExpressionList accessorArgList)
    {
        return castImplementor.convertCastToAssignableValue(
            this,
            null,
            type,
            null,
            null,
            new MethodCall(
                getRelImplementor().getConnectionVariable(),
                accessorName,
                accessorArgList));
    }

    public Expression convertVariableWithCast(
        RelDataType type,
        Class accessorClassCast,
        String accessorName,
        ExpressionList accessorArgList)
    {
        return castImplementor.convertCastToAssignableValue(
            this,
            null,
            type,
            null,
            null,
            new MethodCall(
                new CastExpression(
                    OJUtil.typeNameForClass(accessorClassCast), 
                    getRelImplementor().getConnectionVariable()),
                accessorName,
                accessorArgList));        
    }
    
    // override RexToOJTranslator
    public Expression visitLiteral(RexLiteral literal)
    {
        super.visitLiteral(literal);
        RelDataType type = literal.getType();
        if (SqlTypeUtil.isJavaPrimitive(type)) {
            return null;
        }
        if (type.getSqlTypeName() == SqlTypeName.Null) {
            return null;
        }

        // Create a constant member.
        Variable variable = getRelImplementor().newVariable();
        final TypeName typeName = OJUtil.toTypeName(type, getTypeFactory());
        memberList.add(
            new FieldDeclaration(
                new ModifierList(ModifierList.STATIC | ModifierList.FINAL),
                typeName,
                variable.toString(),
                new AllocationExpression(
                    typeName,
                    null,
                    null)));

        // Generate initialization code, and add it as a static initializer.
        // TODO: Static initializers are painful! We should generate
        //  constructors so that all SQL types can be initialized using an
        //  expression.
        final StatementList statementList = new StatementList();
        castImplementor.convertCastToAssignableValue(
            this,
            statementList,
            type,
            type,
            variable,
            getTranslation());
        assert !statementList.isEmpty();
        memberList.add(
            new MemberInitializer(statementList, true));
        return setTranslation(variable);
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

    /**
     * Creates a member of this class with a given set of modifiers, and
     * initializes it with an expression.
     *
     * @param ojClass Type of the variable
     * @param exp Expression to initialize it
     * @return Variable
     */
    public Variable createScratchVariableWithExpression(
        OJClass ojClass,
        Expression exp)
    {
        Variable variable = getRelImplementor().newVariable();
        memberList.add(
            new FieldDeclaration(
                new ModifierList(ModifierList.EMPTY),
                TypeName.forOJClass(ojClass),
                variable.toString(),
                exp));
        return variable;
    }

    public Variable createScratchVariable(RelDataType type)
    {
        OJClass ojClass = OJUtil.typeToOJClass(
            type, getFarragoTypeFactory());
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
        OJClass ojClass = OJUtil.typeToOJClass(
            type, getFarragoTypeFactory());
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
        RelDataType type = op.getType();
        return getFarragoTypeFactory().getValueAccessExpression(type, expr);
    }

    public Expression convertCastOrAssignmentWithStmtList(
        StatementList stmtList,
        String targetName,
        RelDataType lhsType,
        RelDataType rhsType,
        Expression lhsExp,
        Expression rhsExp)
    {
        return castImplementor.convertCastOrAssignment(
            this, stmtList, targetName,
            lhsType, rhsType,
            lhsExp, rhsExp);
    }

    public Expression convertCastOrAssignment(
        String targetName,
        RelDataType lhsType,
        RelDataType rhsType,
        Expression lhsExp,
        Expression rhsExp)
    {
        return castImplementor.convertCastOrAssignment(
            this, null, targetName,
            lhsType, rhsType,
            lhsExp, rhsExp);
    }

    public FarragoTypeFactory getFarragoTypeFactory()
    {
        return (FarragoTypeFactory) getTypeFactory();
    }

    public FarragoRepos getRepos()
    {
        return repos;
    }

    // override
    public void translateAssignment(RelDataTypeField lhsField,
        Expression lhsExp,
        RexNode rhs)
    {
        Expression rhsExp = translateRexNode(rhs);
        convertCastOrAssignmentWithStmtList(
            stmtList,
            getRepos().getLocalizedObjectName(
                lhsField.getName()),
            lhsField.getType(),
            rhs.getType(),
            lhsExp,
            rhsExp);
    }

    public void addAssignmentStatement(
        StatementList stmtList,
        Expression funcResult,
        RelDataType retType,
        Variable varResult,
        boolean needCast)
    {
        Expression lhsExp;
        if (SqlTypeUtil.isJavaPrimitive(retType) && !retType.isNullable()) {
            lhsExp = varResult;
        } else {
            lhsExp = new FieldAccess(varResult, 
                        NullablePrimitive.VALUE_FIELD_NAME);
        }
        if (!SqlTypeUtil.isJavaPrimitive(retType) || retType.isNullable()) {
            stmtList.add(createSetNullStatement(varResult, false));
        }
        Expression result = funcResult;
        if (needCast) {
            OJClass lhsClass = 
                OJClass.forClass(
                    getFarragoTypeFactory().getClassForPrimitive(
                        retType));
            
            result = new CastExpression(lhsClass, funcResult);
        }
        Statement stmt = new ExpressionStatement(
                            new AssignmentExpression(
                                lhsExp,
                                AssignmentExpression.EQUALS, 
                                result));
        stmtList.add(stmt);
    }
}


// End FarragoRexToOJTranslator.java
