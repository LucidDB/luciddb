/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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

import java.util.*;
import java.util.List;

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
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


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
    private final Frame frame;
    private final MemberDeclarationList memberList;
    private final FarragoOJRexCastImplementor castImplementor;
    private final OJClass ojNullablePrimitive;
    private final Map<Integer, String> localRefMap;

    /**
     * Scopes, each consisting of a {@link StatementList} and zero or more
     * variable declarations, being built up for ROW or CASE expression. For
     * CASE expressions, each WHEN, THEN or ELSE has one statementList. For ROW
     * expressions, each value in a row has one statementList.
     */
    private Frame[] frames;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a translator based on a {@link OJRexImplementorTable}.
     *
     * @param repos repository
     * @param relImplementor implementation context
     * @param contextRel relational expression which is the context for the
     *     row-expressions which are to be translated
     * @param implementorTable table of implementations for SQL operators
     * @param frame Frame containing statement list for side-effects of
     *     translation
     * @param memberList member list for class-level state required by
     * @param program Program, may be null
     * @param localRefMap map from RexLocalRef index to name of method which
     */
    public FarragoRexToOJTranslator(
        FarragoRepos repos,
        JavaRelImplementor relImplementor,
        RelNode contextRel,
        OJRexImplementorTable implementorTable,
        Frame frame,
        MemberDeclarationList memberList,
        RexProgram program,
        Map<Integer, String> localRefMap)
    {
        super(relImplementor, contextRel, implementorTable);
        this.repos = repos;
        this.frame = frame;
        assert frame != null;
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
            new Frame(null, false, stmtList),
            memberList,
            program,
            new HashMap<Integer, String>());
    }

    //~ Methods ----------------------------------------------------------------

    public FarragoRexToOJTranslator push(StatementList stmtList)
    {
        return pushFrame(new Frame(frame, true, stmtList));
    }

    public FarragoRexToOJTranslator pushFrame(Frame frame)
    {
        // The child translator inherits important state like localRefMap.
        // (Otherwise common expressions would be translated more than once.)
        // But the frame has no parent, so it cannot inherit the result of
        // common expressions that have already been calculated.
        return new FarragoRexToOJTranslator(
            repos,
            getRelImplementor(),
            getContextRel(),
            getImplementorTable(),
            frame,
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
        frame.stmtList.add(stmt);
    }

    public void addStatementsFromList(StatementList newStmtList)
    {
        frame.stmtList.addAll(newStmtList);
    }

    /**
     * Returns the frame (including a {@link StatementList} a set of variables
     * in scope) corresponding to a subexpression of a CASE or ROW expression.
     */
    public Frame getSubFrame(int i)
    {
        if (frames == null) {
            return null;
        }
        if (i < frames.length) {
            return frames[i];
        }
        return null;
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
        assert (refCount > 0);
        if (refCount == 1) {
            return super.visitLocalRef(localRef);
        }

        // First see if there is a variable in scope for this common
        // subexpression.
        Expression expr = frame.lookupExprImpl(localRef.getIndex());
        if (expr != null) {
            return setTranslation(expr);
        }

        // See if we've already generated code for this common subexpression.
        String methodName = localRefMap.get(localRef.getIndex());
        int complexity = OJUtil.countParseTreeNodes(expr);
        if (methodName != null) {
            expr =
                new MethodCall(
                    methodName,
                    new ExpressionList());
        } else if (complexity > 100) {
            // The expression is very complicated, so to avoid pushing this
            // method over the 32kB bytecode limit, put the new code in its
            // own method. There is a cost to this: any common subexpressions
            // will have to be evaluated afresh in the method.
            StatementList methodBody = new StatementList();
            FarragoRexToOJTranslator subTranslator =
                pushFrame(
                    new Frame(null, false, methodBody));

            Expression subExpr = subTranslator.translateSubExpression(localRef);
            methodName = "calc_cse_" + localRef.getIndex();
            localRefMap.put(localRef.getIndex(), methodName);

            methodBody.add(new ReturnStatement(subExpr));

            // Wrap the expression as a method declaration. Allow the method to
            // throw any Exception, because without analyzing the parse tree,
            // it's difficult to know which exceptions the code will throw.
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

            expr = new MethodCall(methodName, new ExpressionList());
        } else {
            expr = translateSubExpression(localRef);
        }

        // If the expression is very simple (say a literal, or another
        // variable) don't store it in its own variable. If it's a variable,
        // register it.
        complexity = OJUtil.countParseTreeNodes(expr);
        if (expr instanceof Variable || complexity > 1) {
            final Variable variable =
                variablize(localRef.getType(), expr);
            frame.varMap.put(localRef.getIndex(), variable);
            expr = variable;
        }

        return setTranslation(expr);
    }

    /**
     * Converts an expression to a local variable, to prevent propagation of
     * the same expression.
     *
     * <p>To make sure that all references to a given
     * {@link org.eigenbase.rex.RexLocalRef} get the same variable, calling this
     * method is not sufficient. You also have to register the ref index in
     * {@link Frame#varMap}.
     *
     * @param type Value type
     * @param expr Expression
     * @return Variable
     */
    public Variable variablize(RelDataType type, Expression expr)
    {
        if (expr instanceof Variable) {
            return (Variable) expr;
        }
        final Variable variable = newVariable();
        frame.stmtList.add(
            declareLocalVariable(
                typeToOJClass(type),
                variable,
                expr));
        return variable;
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

    @Override
    protected Expression convertCallAndOperands(RexCall call)
    {
        // TODO jvs 16-Oct-2006:  make this properly extensible
        final boolean needSub;
        final SqlKind operatorKind = call.getOperator().getKind();
        switch (operatorKind) {
        case CASE:
        case NEW_SPECIFICATION:
        case ROW:
            needSub = true;
            break;
        default:
            needSub = false;
            break;
        }
        if (!needSub) {
            return super.convertCallAndOperands(call);
        }
        final Frame[] savedFrames = frames;
        try {
            final RexNode[] operands = call.getOperands();
            frames = new Frame[operands.length];
            List<Expression> exprs = new ArrayList<Expression>();
            for (int i = 0; i < operands.length; i++) {
                RexNode operand = operands[i];
                switch (operatorKind) {
                case CASE:
                    frames[i] =
                        i == 0
                        ? frame
                        : new Frame(
                            frames[i - 2 + (i % 2)],
                            false,
                            new StatementList());
                    break;
                case NEW_SPECIFICATION:
                case ROW:
                    // Each expression in a ROW or NEW has its own statement
                    // list. Expression #0 inherits variables from the parent
                    // context, and the other expressions inherit from their
                    // older sibling. It is not feasible to share expressions
                    // back with the parent frame, because we are generated
                    // inside a function call.
                    frames[i] =
                        new Frame(
                            i == 0
                            ? frame
                            : frames[i - 1],
                            i > 0,
                            new StatementList());
                    break;
                default:
                    throw Util.unexpected(operatorKind);
                }
                RexToOJTranslator subTranslator = pushFrame(frames[i]);
                exprs.add(subTranslator.translateRexNode(operand));
            }
            return convertCall(call, exprs);
        } finally {
            frames = savedFrames;
        }
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
        if (type.getSqlTypeName() == SqlTypeName.NULL) {
            return null;
        }

        // Create a constant member.  Note that it can't be static
        // because we might be generating code inside of an anonymous
        // inner class.  And it can't be final because we can't initialize
        // it until first use.
        Variable variable = getRelImplementor().newVariable();
        final TypeName typeName =
            OJUtil.toTypeName(
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

        addStatement(new IfStatement(
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
        OJClass ojClass =
            OJUtil.typeToOJClass(
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
     * @param ojClass Variable's type
     *
     * @return scratch variable
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
        return new FieldDeclaration(
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
        return new VariableDeclaration(
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
        return new BinaryExpression(
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
        return new ExpressionStatement(
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
        return new IfStatement(
            isNull(var),
            new StatementList(assign(var, init)));
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
                    new BinaryExpression(
                        nullTest,
                        BinaryExpression.LOGICAL_OR,
                        newNullTest);
            }
        }
        return nullTest;
    }

    public boolean isNullablePrimitive(RelDataType type)
    {
        OJClass ojClass =
            OJUtil.typeToOJClass(
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
        return castImplementor.convertCastOrAssignment(
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
        return castImplementor.convertCastOrAssignment(
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
    public void translateAssignment(
        RelDataTypeField lhsField,
        Expression lhsExp,
        RexNode rhs)
    {
        Expression rhsExp = translateRexNode(rhs);
        convertCastOrAssignmentWithStmtList(
            frame.stmtList,
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
                new FieldAccess(
                    varResult,
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

    /**
     * A location to which code is being generated.
     *
     * <p>Contains a {@link openjava.ptree.StatementList}. May define variables;
     * inherits variables from parent frames.
     *
     * <p>Immutable after construction.
     */
    public static class Frame
    {
        public final Frame parentFrame;
        public final StatementList stmtList;
        public final Map<Integer, Variable> varMap;

        /**
         * Creates a Frame.
         *
         * @param parentFrame Parent frame
         * @param mandatory Whether this is a mandatory child of the parent
         *     frame. This means that expressions will definitely be executed.
         *     Therefore it is safe to declare variables in the frame's variable
         *     map.
         * @param stmtList Statement list
         */
        public Frame(
            Frame parentFrame,
            boolean mandatory,
            StatementList stmtList)
        {
            this.parentFrame = parentFrame;
            this.stmtList = stmtList;
            assert stmtList != null;
            assert !(parentFrame == null && mandatory);
            this.varMap =
                mandatory
                ? parentFrame.varMap
                : new HashMap<Integer, Variable>(0);
        }

        /**
         * Looks up an implementation of a given local reference in this frame
         * or a frame it inherits from.
         *
         * @param index Local variable ordinal
         * @return Variable, or null if not found
         */
        public Expression lookupExprImpl(int index)
        {
            for (Frame frame = this; frame != null; frame = frame.parentFrame) {
                Variable variable = frame.varMap.get(index);
                if (variable != null) {
                    return variable;
                }
            }
            return null;
        }
    }
}

// End FarragoRexToOJTranslator.java
