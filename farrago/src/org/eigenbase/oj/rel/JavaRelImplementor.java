/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

package org.eigenbase.oj.rel;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.util.logging.Level;
import java.util.logging.Logger;

import openjava.ptree.*;

import org.eigenbase.oj.rex.*;
import org.eigenbase.oj.util.OJUtil;
import org.eigenbase.rel.JoinRel;
import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelImplementor;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.trace.EigenbaseTrace;
import org.eigenbase.util.SaffronProperties;
import org.eigenbase.util.Util;


/**
 * <code>JavaRelImplementor</code> deals with the nastiness of converting a tree
 * of relational expressions into an implementation, generally an {@link
 * ParseTree openjava parse tree}.
 *
 * <p>
 * The {@link #bind} method allows relational expressions to register which
 * Java variable holds their row. They can bind 'lazily', so that the
 * variable is only declared and initialized if it is actually used in
 * another expression.
 * </p>
 *
 * <p>
 * TODO jvs 14-June-2004:  some of JavaRelImplementor is specific to
 * the JAVA calling convention; those portions should probably be
 * factored out into a subclass.
 * </p>
 */
public class JavaRelImplementor implements RelImplementor
{
    //~ Static fields/initializers --------------------------------------------

    private static final Logger tracer =
        EigenbaseTrace.getRelImplementorTracer();

    //~ Instance fields -------------------------------------------------------

    /** Maps a {@link String} to the {@link Frame} whose
     * {@link Frame#rel}.correlVariable == correlName.
     */
    final HashMap _mapCorrel2Frame = new HashMap();
    final HashMap _mapCorrelNameToVariable = new HashMap();

    /**
     * Maps a {@link RelNode} to the unique frame whose {@link Frame#rel}
     * is that relational expression.
     */
    final HashMap _mapRel2Frame = new HashMap();

    /**
     * Stack of {@link StatementList} objects.
     */
    final Stack _stmtListStack = new Stack();
    Statement _exitStatement;
    public final RexBuilder _rexBuilder;
    private int nextVariableId;

    //~ Constructors ----------------------------------------------------------

    public JavaRelImplementor(RexBuilder rexBuilder)
    {
        _rexBuilder = rexBuilder;
        nextVariableId = 0;
    }

    //~ Methods ---------------------------------------------------------------

    public void setExitStatement(openjava.ptree.Statement stmt)
    {
        this._exitStatement = stmt;
    }

    public Statement getExitStatement()
    {
        return _exitStatement;
    }

    public StatementList getStatementList()
    {
        return (StatementList) _stmtListStack.peek();
    }

    public RexBuilder getRexBuilder()
    {
        return _rexBuilder;
    }

    /**
     * Record the fact that instances of <code>rel</code> are available in
     * <code>variable</code>.
     */
    public void bind(
        RelNode rel,
        Variable variable)
    {
        bind(
            rel,
            new EagerBind(variable));
    }

    /**
     * Declare a variable, and bind it lazily, so it only gets initialized if
     * it is actually used.
     *
     * @return the Variable so declared
     */
    public Variable bind(
        RelNode rel,
        StatementList statementList,
        final VariableInitializer initializer)
    {
        VariableInitializerThunk thunk =
            new VariableInitializerThunk() {
                public VariableInitializer getInitializer()
                {
                    return initializer;
                }
            };
        Variable variable = newVariable();
        LazyBind bind =
            new LazyBind(variable, statementList,
                rel.getRowType(), thunk);
        bind(rel, bind);
        return bind.getVariable();
    }

    /**
     * Shares a variable between relations.  <code>previous</code> already has
     * a variable, and calling this method indicates that <code>rel</code>'s
     * output will appear in this variable too.
     */
    public void bind(
        RelNode rel,
        RelNode previous)
    {
        bind(
            rel,
            new RelBind(previous));
    }

    /**
     * Binds a correlating variable. References to correlating variables such
     * as <code>$cor2</code> will be replaced with java variables such as
     * <code>$Oj14</code>.
     */
    public void bindCorrel(
        String correlName,
        Variable variable)
    {
        _mapCorrelNameToVariable.put(correlName, variable);
    }

    public JavaRel findRel(
        JavaRel rel,
        RexNode expression)
    {
        if (expression instanceof RexInputRef) {
            RexInputRef variable = (RexInputRef) expression;
            if (rel instanceof JoinRel && false) {
                return (JavaRel) findInputRel(rel, variable.index);
            } else {
                return (JavaRel) rel.getInput(variable.index);
            }
        } else if (expression instanceof RexFieldAccess) {
            RexFieldAccess fieldAccess = (RexFieldAccess) expression;
            String fieldName = fieldAccess.getName();
            RexNode refExp = fieldAccess.getReferenceExpr();
            JavaRel rel2 = findRel(rel, refExp); // recursive
            if (rel2 == null) {
                return null;
            }
            return implementFieldAccess(rel2, fieldName);
        } else {
            return null;
        }
    }

    /**
     * Burrows into a synthetic record and returns the underlying relation
     * which provides the field called <code>fieldName</code>.
     */
    public JavaRel implementFieldAccess(
        JavaRel rel,
        String fieldName)
    {
        if (rel instanceof ProjectRelBase) {
            return ((ProjectRelBase) rel).implementFieldAccess(this, fieldName);
        } else {
            return null;
        }
    }

    /**
     * Implements the body of the current expression's parent.  If
     * <code>variable</code> is not null, bind the current expression to
     * <code>variable</code>. For example, a nested loops join would generate
     * <blockquote>
     * <pre>
     * for (int i = 0; i < emps.length; i++) {
     *   Emp emp = emps[i];
     *   for (int j = 0; j < depts.length; j++) {
     *     Dept dept = depts[j];
     *     if (emp.deptno == dept.deptno) {
     *       <<parent body>>
     *     }
     *   }
     * }
     * </pre>
     * </blockquote>
     * which corresponds to
     * <blockquote>
     * <pre>
     * [emp:iter
     *   [dept:iter
     *     [join:body(emp,dept)
     *       [parent:body]
     *     ]
     *   ]
     * ]
     * </pre>
     * </blockquote>
     *
     * @param rel child relation
     * @param stmtList block that child was generating its code into
     */
    public void generateParentBody(
        RelNode rel,
        StatementList stmtList)
    {
        if (stmtList != null) {
            pushStatementList(stmtList);
        }
        Frame frame = (Frame) _mapRel2Frame.get(rel);
        bindDeferred(frame, rel);
        ((JavaLoopRel) frame.parent).implementJavaParent(this, frame.ordinal);
        if (stmtList != null) {
            popStatementList(stmtList);
        }
    }

    private void bindDeferred(
        Frame frame,
        final RelNode rel)
    {
        final StatementList statementList = getStatementList();
        if (frame.bind == null) {
            // this relational expression has not bound itself, so we presume
            // that we can call its implementSelf() method
            if (!(rel instanceof JavaSelfRel)) {
                throw Util.newInternal("In order to bind-deferred, a "
                    + "relational expression must implement JavaSelfRel: "
                    + rel);
            }
            final JavaSelfRel selfRel = (JavaSelfRel) rel;
            LazyBind lazyBind =
                new LazyBind(
                    newVariable(),
                    statementList,
                    rel.getRowType(),
                    new VariableInitializerThunk() {
                        public VariableInitializer getInitializer()
                        {
                            return selfRel.implementSelf(JavaRelImplementor.this);
                        }
                    });
            bind(rel, lazyBind);
        } else if (frame.bind instanceof LazyBind
                && (((LazyBind) frame.bind).statementList != statementList)) {
            // Frame is already bound, but to a variable declared in a different
            // scope. Re-bind it.
            final LazyBind lazyBind = (LazyBind) frame.bind;
            lazyBind.statementList = statementList;
            lazyBind.bound = false;
        }
    }

    /**
     * Convenience wrapper around {@link RelImplementor#visitChild} for the
     * common case where {@link JavaRel} has a child which is a {@link JavaRel}.
     */
    public final Expression visitJavaChild(
        RelNode parent,
        int ordinal,
        JavaRel child)
    {
        return (Expression) visitChild(parent, ordinal, child);
    }

    public final Object visitChild(
        RelNode parent,
        int ordinal,
        RelNode child)
    {
        if (parent != null) {
            assert (child == parent.getInputs()[ordinal]);
        }
        Frame frame = new Frame();
        frame.rel = child;
        frame.parent = parent;
        frame.ordinal = ordinal;
        _mapRel2Frame.put(child, frame);
        String correl = child.getCorrelVariable();
        if (correl != null) {
            // Record that this frame is responsible for setting this
            // variable. But if another frame is already doing the job --
            // this frame's parent, which belongs to the same set -- don't
            // override it.
            if (_mapCorrel2Frame.get(correl) == null) {
                _mapCorrel2Frame.put(correl, frame);
            }
        }
        return visitChildInternal(child);
    }

    public Object visitChildInternal(RelNode child)
    {
        final CallingConvention convention = child.getConvention();
        if (!(child instanceof JavaRel)) {
            throw Util.newInternal("Relational expression '" + child
                + "' has '" + convention
                + "' calling convention, so must implement interface "
                + JavaRel.class);
        }
        JavaRel javaRel = (JavaRel) child;
        final ParseTree p = javaRel.implement(this);
        if ((convention == CallingConvention.JAVA) && (p != null)) {
            throw Util.newInternal("Relational expression '" + child
                + "' returned '" + p + " on implement, but should have "
                + "returned null, because it has JAVA calling-convention. "
                + "(Note that similar calling-conventions, such as "
                + "Iterator, must return a value.)");
        }
        return p;
    }

    /**
     * Starts an iteration, by calling
     * {@link org.eigenbase.oj.rel.JavaRel#implement} on the root element.
     */
    public Expression implementRoot(JavaRel rel)
    {
        return visitJavaChild(null, -1, rel);
    }

    /**
     * Creates an expression which references correlating variable
     * <code>correlName</code> from the context of <code>rel</code>.  For
     * example, if <code>correlName</code> is set by the 1st child of
     * <code>rel</code>'s 2nd child, then this method returns
     * <code>$input2.$input1</code>.
     */
    public Expression makeReference(
        String correlName,
        RelNode rel)
    {
        Frame frame = (Frame) _mapCorrel2Frame.get(correlName);
        assert (frame != null);
        assert (Util.equal(
            frame.rel.getCorrelVariable(),
            correlName));
        assert (frame.hasVariable());
        return frame.getVariable();
    }

    public Variable newVariable()
    {
        return new Variable("oj_var" + generateVariableId());
    }

    private int generateVariableId()
    {
        return nextVariableId++;
    }

    public Variable getConnectionVariable()
    {
        throw Util.needToImplement("getConnectionVariable");
    }

    public void popStatementList(StatementList stmtList)
    {
        assert (stmtList == getStatementList());
        _stmtListStack.pop();
    }

    public void pushStatementList(StatementList stmtList)
    {
        _stmtListStack.push(stmtList);
    }

    /**
     * Converts an expression in internal form (the input relation is
     * referenced using the variable <code>$input0</code>) to generated form
     * (the input relation is referenced using the bindings in this
     * <code>JavaRelImplementor</code>).  Compare this method with
     * net.sf.saffron.oj.xlat.QueryInfo.convertExpToInternal(), which converts
     * from source form to internal form.
     *
     * @param exp the expression to translate (it is cloned, not modified)
     * @param rel the relational expression which is the context for
     *        <code>exp</code>
     */
    public Expression translate(
        JavaRel rel,
        RexNode exp)
    {
        RexToOJTranslator translator = newTranslator(rel);
        return translator.translateRexNode(exp);
    }

    /**
     * Determines whether it is possible to implement a set of expressions
     * in Java.
     *
     * @param condition Condition, may be null
     * @param exps Expression list
     * @return whether all expressions can be implemented
     */
    public boolean canTranslate(
        RelNode rel,
        RexNode condition,
        RexNode [] exps)
    {
        RexToOJTranslator translator = newTranslator(rel);
        TranslationTester tester = new TranslationTester(translator, true);
        if ((condition != null) && !tester.canTranslate(condition)) {
            return false;
        }
        for (int i = 0; i < exps.length; i++) {
            RexNode exp = exps[i];
            if (!tester.canTranslate(exp)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Determines whether it is possible to implement an expression in
     * Java.
     *
     * @param condition Condition, may be null
     * @param exps Expression list
     * @param deep if true, operands of the given expression are
     *             tested for translatability as well; if false only
     *             the top level expression is tested
     * @return whether the expression can be implemented
     */
    public boolean canTranslate(
        RelNode rel,
        RexNode expression,
        boolean deep)
    {
        RexToOJTranslator translator = newTranslator(rel);
        TranslationTester tester = new TranslationTester(translator, deep);
        return tester.canTranslate(expression);
    }

    /**
     * Generates code for an expression, possibly using multiple statements,
     * scratch variables, and helper functions.
     *
     * @param rel the relational expression which is the context for exp
     *
     * @param exp the row expression to be translated
     *
     * @param stmtList optional code can be appended here
     *
     * @param memberList optional member declarations can be appended here (if
     * needed for reusable scratch space or helper functions; local variables
     * can also be allocated in stmtList)
     */
    public Expression translateViaStatements(
        JavaRel rel,
        RexNode exp,
        StatementList stmtList,
        MemberDeclarationList memberList)
    {
        return translate(rel, exp);
    }

    /**
     * Generates code for an assignment.  REVIEW:  should assignment
     * instead be represented as a kind of RexNode?
     *
     * @param rel the relational expression which is the context for the lhs
     * and rhs expressions
     *
     * @param lhsType type of the target expression
     *
     * @param lhs the target expression (as OpenJava)
     *
     * @param rhs the source expression (as RexNode)
     *
     * @param stmtList the assignment code is appended here
     * (multiple statements may be required for the assignment)
     *
     * @param memberList optional member declarations can be appended here (if
     * needed for reusable scratch space or helper functions; local variables
     * can also be allocated in stmtList)
     */
    public void translateAssignment(
        JavaRel rel,
        RelDataType lhsType,
        Expression lhs,
        RexNode rhs,
        StatementList stmtList,
        MemberDeclarationList memberList)
    {
        stmtList.add(
            new ExpressionStatement(
                new AssignmentExpression(
                    lhs,
                    AssignmentExpression.EQUALS,
                    translateViaStatements(rel, rhs, stmtList, memberList))));
    }

    /**
     * Converts an array of expressions in internal into a list of expressions
     * in generated form.
     *
     * @see #translate(JavaRel,RexNode)
     */
    public ExpressionList translateList(
        JavaRel rel,
        RexNode [] exps)
    {
        final ExpressionList list = new ExpressionList();
        RexToOJTranslator translator = newTranslator(rel);
        for (int i = 0; i < exps.length; i++) {
            list.add(translator.translateRexNode(exps[i]));
        }
        return list;
    }

    /**
     * Creates a {@link RexToOJTranslator} with which to translate the
     * {@link RexNode row-expressions} within a relational expression
     * into OpenJava expressions.
     */
    protected RexToOJTranslator newTranslator(RelNode rel)
    {
        return new RexToOJTranslator(this, rel, null);
    }

    /**
     * Creates an expression which references the <i>ordinal</i><sup>th</sup>
     * input.
     */
    public Expression translateInput(
        JavaRel rel,
        int ordinal)
    {
        final RelDataType rowType = rel.getRowType();
        int fieldOffset = computeFieldOffset(rel, ordinal);
        return translate(
            rel,
            _rexBuilder.makeRangeReference(rowType, fieldOffset));
    }

    /**
     * Returns the index of the first field in <code>rel</code> which comes
     * from its <code>ordinal</code>th input.
     *
     * <p>For example, if rel joins T0(A,B,C) to T1(D,E), then
     * countFields(0,rel) yields 0, and countFields(1,rel) yields 3.</p>
     */
    private int computeFieldOffset(
        RelNode rel,
        int ordinal)
    {
        if (ordinal == 0) {
            // short-circuit for the common case
            return 0;
        }
        int fieldOffset = 0;
        final RelNode [] inputs = rel.getInputs();
        for (int i = 0; i < ordinal; i++) {
            RelNode input = inputs[i];
            fieldOffset += input.getRowType().getFieldCount();
        }
        return fieldOffset;
    }

    /**
     * Creates an expression which references the
     * <i>fieldOrdinal</i><sup>th</sup> field of the
     * <i>ordinal</i><sup>th</sup> input.
     *
     * <p>
     * (We can potentially optimize the generation process, so we can access
     * field values without actually instantiating the row.)
     * </p>
     */
    public Expression translateInputField(
        JavaRel rel,
        int ordinal,
        int fieldOrdinal)
    {
        assert ordinal >= 0;
        assert ordinal < rel.getInputs().length;
        assert fieldOrdinal >= 0;
        assert fieldOrdinal < rel.getInput(ordinal).getRowType().getFieldCount();
        RelDataType rowType = rel.getRowType();
        final RelDataTypeField [] fields = rowType.getFields();
        final int fieldIndex = computeFieldOffset(rel, ordinal) + fieldOrdinal;
        assert fieldIndex >= 0;
        assert fieldIndex < fields.length;
        final RexNode expr =
            _rexBuilder.makeInputRef(
                fields[fieldIndex].getType(),
                fieldIndex);
        return translate(rel, expr);
    }

    /**
     * Record the fact that instances of <code>rel</code> are available via
     * <code>bind</code> (which may be eager or lazy).
     */
    private void bind(
        RelNode rel,
        Bind bind)
    {
        tracer.log(Level.FINE, "Bind " + rel.toString() + " to " + bind);
        Frame frame = (Frame) _mapRel2Frame.get(rel);
        frame.bind = bind;
        boolean stupid = SaffronProperties.instance().stupid.get();
        if (stupid) {
            // trigger the declaration of the variable, even though it
            // may not be used
            Util.discard(bind.getVariable());
        }
    }

    private RelNode findInputRel(
        RelNode rel,
        int offset)
    {
        return findInputRel(
            rel,
            offset,
            new int [] { 0 });
    }

    private RelNode findInputRel(
        RelNode rel,
        int offset,
        int [] offsets)
    {
        if (rel instanceof JoinRel) {
            // no variable here -- go deeper
            RelNode [] inputs = rel.getInputs();
            for (int i = 0; i < inputs.length; i++) {
                RelNode result = findInputRel(inputs[i], offset, offsets);
                if (result != null) {
                    return result;
                }
            }
        } else if (offset == offsets[0]) {
            return rel;
        } else {
            offsets[0]++;
        }
        return null; // not found
    }

    /**
     * Returns the variable which, in the generated program, will hold the
     * current row of a given relational expression. This method is only
     * applicable if the relational expression is the current one or an input;
     * if it is an ancestor, there is no current value, and this method returns
     * null.
     */
    public Variable findInputVariable(RelNode rel)
    {
        while (true) {
            Frame frame = (Frame) _mapRel2Frame.get(rel);
            if ((frame != null) && frame.hasVariable()) {
                return frame.getVariable();
            }
            RelNode [] inputs = rel.getInputs();
            if (inputs.length == 1) {
                rel = inputs[0];
            } else {
                return null;
            }
        }
    }

    //~ Inner Interfaces ------------------------------------------------------

    /**
     * A <code>VariableInitializerThunk</code> yields a {@link
     * VariableInitializer}.
     */
    public interface VariableInitializerThunk
    {
        VariableInitializer getInitializer();
    }

    private interface Bind
    {
        Variable getVariable();
    }

    //~ Inner Classes ---------------------------------------------------------

    private static class EagerBind implements Bind
    {
        Variable variable;

        EagerBind(Variable variable)
        {
            this.variable = variable;
        }

        public Variable getVariable()
        {
            return variable;
        }

        public String toString()
        {
            return super.toString() + "(variable=" + variable.toString() + ")";
        }
    }

    /**
     * Binds a relational expression to whatever another relational expression
     * is currently bound to.
     *
     * <p>Note that if relational expressions are shared, then this binding can
     * change over time. Consider, for instance "select * from emps where deptno
     * = 10 union select * from emps where deptno = 20". There will be a reader
     * of "emps" which is used by the two filter expressions "deptno = 10" and
     * "deptno = 20". The "emp" reader will assign its current row to "var4" in
     * the first binding, and to "var8" in the second.</p>
     */
    private class RelBind implements Bind
    {
        private final RelNode rel;

        RelBind(RelNode rel)
        {
            this.rel = rel;
        }

        public Variable getVariable()
        {
            final Frame frame = findFrame();
            return frame.getVariable();
        }

        private Frame findFrame()
        {
            RelNode previous = rel;
            while (true) {
                Frame frame = (Frame) _mapRel2Frame.get(previous);
                if (frame.bind != null) {
                    tracer.log(Level.FINE,
                        "Bind " + rel.toString() + " to "
                        + previous.toString() + "(" + frame.bind + ")");
                    return frame;
                }

                // go deeper
                RelNode [] inputs = previous.getInputs();
                assert (inputs.length == 1) : "input is not bound";
                previous = inputs[0];
            }
        }
    }

    private static class Frame
    {
        /** <code>rel</code>'s parent */
        RelNode parent;

        /** relation which is being implemented in this frame */
        RelNode rel;

        /** ordinal of <code>rel</code> within <code>parent</code> */
        int ordinal;

        /** Holds variable which hasn't been declared yet. */
        private Bind bind;

        /**
         * Retrieves the variable, executing the lazy bind if necessary.
         */
        Variable getVariable()
        {
            assert (hasVariable());
            return bind.getVariable();
        }

        /**
         * Returns whether the frame has, or potentially has, a variable.
         */
        boolean hasVariable()
        {
            return bind != null;
        }
    }

    private static class LazyBind implements Bind
    {
        final RelDataType type;
        final Statement after;
        StatementList statementList;
        final Variable variable;
        final VariableInitializerThunk thunk;
        boolean bound;

        LazyBind(
            Variable variable,
            StatementList statementList,
            RelDataType type,
            VariableInitializerThunk thunk)
        {
            this.variable = variable;
            this.statementList = statementList;
            this.after =
                (statementList.size() == 0) ? null
                : statementList.get(statementList.size() - 1);
            this.type = type;
            this.thunk = thunk;
        }

        public Variable getVariable()
        {
            if (!bound) {
                bound = true;
                int position = find(statementList, after);
                VariableDeclaration varDecl =
                    new VariableDeclaration(
                        OJUtil.toTypeName(type),
                        variable.toString(),
                        null);
                statementList.insertElementAt(varDecl, position);
                varDecl.setInitializer(thunk.getInitializer());
            }
            return variable;
        }

        public String toString()
        {
            return super.toString() + "(variable=" + variable.toString()
            + ", thunk=" + thunk.toString() + ")";
        }

        private static int find(
            StatementList list,
            Statement statement)
        {
            if (statement == null) {
                return 0;
            } else {
                for (int i = 0, n = list.size(); i < n; i++) {
                    if (list.get(i) == statement) {
                        return i + 1;
                    }
                }
                throw Util.newInternal("could not find statement " + statement
                    + " in list " + list);
            }
        }
    }

    /**
     * Similar to {@link org.eigenbase.oj.rel.RexToJavaTranslator}, but instead of translating, merely tests
     * whether an expression can be translated.
     */
    public static class TranslationTester
    {
        private final RexToOJTranslator translator;
        private final boolean deep;

        public TranslationTester(
            RexToOJTranslator translator,
            boolean deep)
        {
            this.translator = translator;
            this.deep = deep;
        }

        /**
         * Returns whether an expression can be translated.
         */
        public boolean canTranslate(RexNode rex)
        {
            try {
                go(rex);
                return true;
            } catch (CannotTranslate cannotTranslate) {
                return false;
            }
        }

        /**
         * Walks over an expression, and throws {@link org.eigenbase.oj.rel.JavaRelImplementor.TranslationTester.CannotTranslate} if
         * expression cannot be translated.
         *
         * @param rex Expression
         * @throws org.eigenbase.oj.rel.JavaRelImplementor.TranslationTester.CannotTranslate if expression or a sub-expression cannot be
         *   translated
         */
        protected void go(RexNode rex)
            throws CannotTranslate
        {
            if (rex instanceof RexCall) {
                final RexCall call = (RexCall) rex;
                if (!translator.canConvertCall(call)) {
                    throw new CannotTranslate();
                }
                if (!deep) {
                    return;
                }
                RexNode [] operands = call.operands;
                for (int i = 0; i < operands.length; i++) {
                    go(operands[i]);
                }
            } else if (rex instanceof RexFieldAccess) {
                if (!deep) {
                    return;
                }

                go(((RexFieldAccess) rex).getReferenceExpr());
            }
        }

        /**
         * Thrown when we encounter an expression which cannot be translated.
         * It is always handled by {@link #canTranslate}, and is not really an
         * error.
         */
        private static class CannotTranslate extends Exception
        {
            public CannotTranslate()
            {
            }
        }
    }
}


// End JavaRelImplementor.java
