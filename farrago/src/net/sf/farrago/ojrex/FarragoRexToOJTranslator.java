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

import java.util.*;

/**
 * FarragoRexToOJTranslator refines {@link RexToOJTranslator} with
 * Farrago-specifics.
 *
 * <p>NOTE jvs 22-June-2004: If you're scratching your head trying to understand
 * the code generation methods in this package, it might help to look at
 * examples of the generated code. One way to do this is by turning on {@link
 * net.sf.farrago.trace.FarragoTrace#getDynamicTracer} and then examining the
 * code for generated classes after running queries. See also the .ref files
 * under farrago/testlog/FarragoRexToOJTranslatorTest; these correspond to the
 * test cases in {@link net.sf.farrago.test.FarragoRexToOJTranslatorTest}. You
 * can also add new test cases to that class, run the test, and examine the
 * output.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoRexToOJTranslator
    extends RexToOJTranslator
{

    //~ Static fields/initializers ---------------------------------------------

    private static String TO_STRING_METHOD_NAME = "toString";

    //~ Instance fields --------------------------------------------------------

    private final FarragoRepos repos;
    private StatementList stmtList;
    private final MemberDeclarationList memberList;
    private final FarragoOJRexCastImplementor castImplementor;
    private final OJClass ojNullablePrimitive;
    private final Map<Integer, String> localRefMap;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a translator based on a {@link OJRexImplementorTable}.
     *
     * @param repos repository
     * @param relImplementor implementation context
     * @param contextRel relational expression which is the context for the
     * row-expressions which are to be translated
     * @param implementorTable table of implementations for SQL operators
     * @param stmtList statement list for side-effects of translation
     * @param memberList member list for class-level state required by
     * @param program Program, may be null
     * @param localRefMap map from RexLocalRef index to
     * name of method which computes it
     */
    public FarragoRexToOJTranslator(
        FarragoRepos repos,
        JavaRelImplementor relImplementor,
        RelNode contextRel,
        OJRexImplementorTable implementorTable,
        StatementList stmtList,
        MemberDeclarationList memberList,
        RexProgram program,
        Map<Integer, String> localRefMap)
    {
        super(relImplementor, contextRel, implementorTable);
        this.repos = repos;
        this.stmtList = stmtList;
        this.memberList = memberList;

        this.localRefMap = localRefMap;
        
        // keep a reference to the implementor for CAST, which
        // is needed for implementing assignments also
        castImplementor =
            (FarragoOJRexCastImplementor) getImplementorTable().get(
                SqlStdOperatorTable.castFunc);

        ojNullablePrimitive = OJClass.forClass(NullablePrimitive.class);
        if (program != null) {
            pushProgram(program);
        }
    }

    public FarragoRexToOJTranslator(
        FarragoRepos repos,
        JavaRelImplementor relImplementor,
        RelNode contextRel,
        OJRexImplementorTable implementorTable,
        StatementList stmtList,
        MemberDeclarationList memberList,
        RexProgram program)
    {
        this(
            repos,
            relImplementor,
            contextRel,
            implementorTable,
            stmtList,
            memberList,
            program,
            new HashMap<Integer, String>());
    }

    //~ Methods ----------------------------------------------------------------

    public RexToOJTranslator push(StatementList stmtList)
    {
        // NOTE jvs 16-Oct-2006: The child translator inherits important state
        // like localRefMap.  (Otherwise common expressions would be translated
        // and evaluated more than once.)
        return
            new FarragoRexToOJTranslator(
                repos,
                getRelImplementor(),
                getContextRel(),
                getImplementorTable(),
                stmtList,
                memberList,
                getProgram(),
                localRefMap);
    }

    public void addMember(MemberDeclaration member)
    {
        memberList.add(member);
    }

    public void addStatement(Statement stmt)
    {
        stmtList.add(stmt);
    }

    public void addStatementsFromList(StatementList newStmtList)
    {
        stmtList.addAll(newStmtList);
    }

    // override RexToOJTranslator
    public Expression visitLocalRef(RexLocalRef localRef)
    {
        // TODO jvs 16-Oct-2006: Generate code in such a way that we compute
        // common subexpressions on demand and only once per input row.  The
        // codegen below causes them to be recomputed at each point of use,
        // which is inefficient.  (But not as inefficient as the superclass
        // behavior, which re-expands them at each point of use, causing us to
        // quickly exceed the Java classfile limits!)  For an example of why
        // on-demand rather than eager is necessary, see
        // http://issues.eigenbase.org/browse/FRG-23.

        // For input expressions, delegate to superclass.
        if (isInputRef(localRef)) {
            return super.visitLocalRef(localRef);
        }

        // If it's not a true common subexpression, just expand it
        int [] refCounts = getProgram().getReferenceCounts();
        int refCount = refCounts[localRef.getIndex()];
        assert(refCount > 0);
        if (refCount == 1) {
            return super.visitLocalRef(localRef);
        }
        
        // See if we've already generated code for this common subexpression.
        String methodName = localRefMap.get(localRef.getIndex());
        if (methodName != null) {
            return setTranslation(
                new MethodCall(
                    methodName,
                    new ExpressionList()));
        }

        // Nope, generate it now.

        StatementList methodBody = new StatementList();
        FarragoRexToOJTranslator subTranslator =
            (FarragoRexToOJTranslator) push(methodBody);

        Expression expr = subTranslator.translateSubExpression(localRef);

        int complexity = OJUtil.countParseTreeNodes(expr);
        if (methodBody.isEmpty() && (complexity < 5)) {
            // The expression is very simple (like maybe just a constant);
            // don't bother with a separate method.
            return setTranslation(expr);
        }
        
        methodName =
            "calc_cse_" + localRef.getIndex();
        localRefMap.put(localRef.getIndex(), methodName);

        methodBody.add(
            new ReturnStatement(expr));

        MemberDeclaration methodDecl =
            new MethodDeclaration(
                new ModifierList(
                    ModifierList.PRIVATE | ModifierList.FINAL),
                TypeName.forOJClass(
                    OJUtil.typeToOJClass(
                        localRef.getType(),
                        getFarragoTypeFactory())),
                methodName,
                new ParameterList(),
                null,
                methodBody);
        addMember(methodDecl);

        return setTranslation(
            new MethodCall(methodName, new ExpressionList()));
    }
    
    // implement RexVisitor
    public Expression visitDynamicParam(RexDynamicParam dynamicParam)
    {
        return
            setTranslation(
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
        return
            castImplementor.convertCastToAssignableValue(
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
        return
            castImplementor.convertCastToAssignableValue(
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

        // Create a constant member.  Note that it can't be static
        // because we might be generating code inside of an anonymous
        // inner class.  And it can't be final because we can't initialize
        // it until first use.
        Variable variable = getRelImplementor().newVariable();
        final TypeName typeName = OJUtil.toTypeName(
                type,
                getTypeFactory());
        memberList.add(
            new FieldDeclaration(
                new ModifierList(ModifierList.PRIVATE),
                typeName,
                variable.toString(),
                null));

        // Generate initialization code, and add it inside of
        // an if block to be executed first time through.  Use
        // null to represent uninitialized.  For better performance,
        // we should do this in a constructor or a top-level
        // static initializer.
        
        final StatementList initStmtList = new StatementList();
        initStmtList.add(
            new ExpressionStatement(
                new AssignmentExpression(
                    variable,
                    AssignmentExpression.EQUALS,
                    new AllocationExpression(
                        typeName,
                        null,
                        null))));
        castImplementor.convertCastToAssignableValue(
            this,
            initStmtList,
            type,
            type,
            variable,
            getTranslation());
        assert !initStmtList.isEmpty();

        addStatement(
            new IfStatement(
                isNull(variable),
                initStmtList));
        
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
     *
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
                type,
                getFarragoTypeFactory());
        if (SqlTypeUtil.isJavaPrimitive(type) && (!type.isNullable())) {
            return createPrimitiveScratchVariable(ojClass);
        }
        return createScratchVariable(ojClass, null, null);
    }

    /**
     * Creates a primitive scratch variable without initialization.
     *
     * @param ojClass
     *
     * @return
     */
    public Variable createPrimitiveScratchVariable(
        OJClass ojClass)
    {
        Variable variable = getRelImplementor().newVariable();
        memberList.add(
            newMember(ModifierList.EMPTY, ojClass, variable, null));
        return variable;
    }

    /**
     * Generates a variable with a unique name
     */
    public Variable newVariable()
    {
        return getRelImplementor().newVariable();
    }

    /**
     * Generates a declaration for a field in a class. This should be followed
     * by {@link #addMember}.
     *
     * @param modifiers bitmap of modifiers such as private, or static
     * @param ojClass type of the member
     * @param var uniquely named variable
     * @param init the initial value of the member, may be null
     */
    public FieldDeclaration newMember(
        int modifiers,
        OJClass ojClass,
        Variable var,
        VariableInitializer init)
    {
        return
            new FieldDeclaration(
                new ModifierList(modifiers),
                TypeName.forOJClass(ojClass),
                var.toString(),
                init);
    }

    /**
     * Retrieves the OpenJava type corresponding to a Sql type
     *
     * @param type the Sql type
     */
    public OJClass typeToOJClass(RelDataType type)
    {
        return OJUtil.typeToOJClass(
                type,
                getFarragoTypeFactory());
    }

    /**
     * Generates a local variable declaration
     *
     * @param ojClass type of the variable
     * @param var the uniquely named variable
     * @param init the initial value of the variable, may be null
     */
    public Statement declareLocalVariable(
        OJClass ojClass,
        Variable var,
        Expression init)
    {
        return
            new VariableDeclaration(
                TypeName.forOJClass(ojClass),
                new VariableDeclarator(
                    var.toString(),
                    init));
    }

    /**
     * Generates a boolean expression describing whether an input expression is
     * null
     *
     * @param exp the input expression
     */
    public Expression isNull(Expression exp)
    {
        return
            new BinaryExpression(
                exp,
                BinaryExpression.EQUAL,
                Literal.constantNull());
    }

    /**
     * Generates the string expression <code>exp.toString()</code>
     *
     * @param exp expression to be converted into a string
     */
    public Expression toString(
        Expression exp)
    {
        return new MethodCall(
                exp,
                TO_STRING_METHOD_NAME,
                new ExpressionList());
    }

    /**
     * Generates a simple assignment statement
     *
     * @param lhs the expression on left side of the assignment
     * @param rhs the expression on the right side of the assignment
     */
    public Statement assign(
        Expression lhs,
        Expression rhs)
    {
        return
            new ExpressionStatement(
                new AssignmentExpression(
                    lhs,
                    AssignmentExpression.EQUALS,
                    rhs));
    }

    /**
     * Generates a statement to initializes a variable if the variable is null
     *
     * @param var the variable to be initialed
     * @param init the initial value for the variable
     */
    public Statement setIfNull(
        Variable var,
        Expression init)
    {
        return
            new IfStatement(
                isNull(var),
                new StatementList(assign(var, init)));
    }

    public Statement createSetNullStatement(
        Expression varResult,
        boolean isNull)
    {
        return
            new ExpressionStatement(
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
                        BinaryExpression.LOGICAL_OR,
                        newNullTest);
            }
        }
        return nullTest;
    }

    public boolean isNullablePrimitive(RelDataType type)
    {
        OJClass ojClass = OJUtil.typeToOJClass(
                type,
                getFarragoTypeFactory());
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
        return
            castImplementor.convertCastOrAssignment(
                this,
                stmtList,
                targetName,
                lhsType,
                rhsType,
                lhsExp,
                rhsExp);
    }

    public Expression convertCastOrAssignment(
        String targetName,
        RelDataType lhsType,
        RelDataType rhsType,
        Expression lhsExp,
        Expression rhsExp)
    {
        return
            castImplementor.convertCastOrAssignment(
                this,
                null,
                targetName,
                lhsType,
                rhsType,
                lhsExp,
                rhsExp);
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
            lhsExp =
                new FieldAccess(varResult,
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
        Statement stmt =
            new ExpressionStatement(
                new AssignmentExpression(
                    lhsExp,
                    AssignmentExpression.EQUALS,
                    result));
        stmtList.add(stmt);
    }
}

// End FarragoRexToOJTranslator.java
