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
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
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

    private FarragoRepos repos;
    private StatementList stmtList;
    private MemberDeclarationList memberList;
    private FarragoOJRexCastImplementor castImplementor;
    private OJClass ojNullablePrimitive;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a translator based on a {@link OJRexImplementorTable}.
     *
     * @param repos repository
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
        FarragoRepos repos,
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
        castImplementor = (FarragoOJRexCastImplementor)
            getImplementorTable().get(
                SqlStdOperatorTable.castFunc);

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
    public void visitDynamicParam(RexDynamicParam dynamicParam)
    {
        setTranslation(
            convertVariable(
                dynamicParam.getType(),
                "getDynamicParamValue",
                new ExpressionList(
                    Literal.makeLiteral(
                        dynamicParam.getIndex()))));
    }

    Expression convertVariable(
        RelDataType type,
        String accessorName,
        ExpressionList accessorArgList)
    {
        return castImplementor.convertCastToAssignableValue(
            this,
            type,
            null,
            null,
            new MethodCall(
                getRelImplementor().getConnectionVariable(),
                accessorName,
                accessorArgList));
    }

    // override RexToOJTranslator
    public void visitLiteral(RexLiteral literal)
    {
        super.visitLiteral(literal);
        RelDataType type = literal.getType();
        if (SqlTypeUtil.isJavaPrimitive(type)) {
            return;
        }
        if (type.getSqlTypeName() == SqlTypeName.Null) {
            return;
        }
        
        // TODO jvs 22-May-2004:  Initialize once and only once.
        setTranslation(
            castImplementor.convertCastToAssignableValue(
                this,
                type,
                type,
                null,
                getTranslation()));
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

    public Expression convertCastOrAssignment(
        String targetName, 
        RelDataType lhsType,
        RelDataType rhsType,
        Expression lhsExp,
        Expression rhsExp)
    {
        return castImplementor.convertCastOrAssignment(
            this, targetName,
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
}


// End FarragoRexToOJTranslator.java
