/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
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

package net.sf.saffron.opt;

import net.sf.saffron.core.SaffronField;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.oj.rel.JavaRel;
import net.sf.saffron.oj.util.JavaRowExpression;
import net.sf.saffron.oj.util.OJUtil;
import net.sf.saffron.oj.OJTypeFactory;
import net.sf.saffron.rel.JoinRel;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.rex.*;
import net.sf.saffron.util.SaffronProperties;
import net.sf.saffron.util.Util;
import net.sf.saffron.sql.SqlOperator;
import net.sf.saffron.sql.SqlBinaryOperator;
import net.sf.saffron.sql.SqlFunction;
import net.sf.saffron.sql.SqlLiteral;
import openjava.mop.*;
import openjava.ptree.*;
import openjava.tools.DebugOut;

import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;


/**
 * <code>RelImplementor</code> deals with the nastiness of converting a tree
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
 * TODO jvs 29-Sept-2003:  This class is way too Java-dependent to remain in
 * opt.  Most of it needs to be factored out to oj, leaving an interface or
 * abstract base class behind.  I intentionally left one macker failure
 * unsuppressed so that this will get some attention.
 * </p>
 */
public class RelImplementor
{
    //~ Instance fields -------------------------------------------------------

    /** Map String --> Frame such that frame.rel.correlVariable == correlName */
    final HashMap mapCorrel2Frame = new HashMap();
    final HashMap mapCorrelNameToVariable = new HashMap();
    final HashMap mapRel2Frame = new HashMap();// Rel --> Frame such that frame.rel == rel
    final Stack stmtListStack = new Stack(); // elements are StatementList
    openjava.ptree.Statement exitStatement;
    RexBuilder rexBuilder;

    // REVIEW jvs 15-Dec-2003:  Maybe this belongs someplace sharable?  Also,
    // maybe use the {@link net.sf.saffron.util.Glossary#PrototypePattern}
    // to generalize this to all operators?
    public final Map mapRexBinaryToOpenJava;

    private Map createRexBinaryToOpenJavaMap()
    {
        HashMap map = new HashMap();
        map.put(
            rexBuilder.operatorTable.equalsOperator,
            new Integer(BinaryExpression.EQUAL));
        map.put(
            rexBuilder.operatorTable.lessThanOperator,
            new Integer(BinaryExpression.LESS));
        map.put(
            rexBuilder.operatorTable.greaterThanOperator,
            new Integer(BinaryExpression.GREATER));
        map.put(
            rexBuilder.operatorTable.plusOperator,
            new Integer(BinaryExpression.PLUS));
        map.put(
            rexBuilder.operatorTable.minusOperator,
            new Integer(BinaryExpression.MINUS));
        map.put(
            rexBuilder.operatorTable.multiplyOperator,
            new Integer(BinaryExpression.TIMES));
        map.put(
            rexBuilder.operatorTable.divideOperator,
            new Integer(BinaryExpression.DIVIDE));
        map.put(
            rexBuilder.operatorTable.andOperator,
            new Integer(BinaryExpression.LOGICAL_AND));
        return Collections.unmodifiableMap(map);
    }

    //~ Constructors ----------------------------------------------------------

    public RelImplementor(RexBuilder rexBuilder)
    {
        this.rexBuilder = rexBuilder;
        mapRexBinaryToOpenJava = createRexBinaryToOpenJavaMap();
    }

    //~ Methods ---------------------------------------------------------------

    public void setExitStatement(openjava.ptree.Statement stmt)
    {
        this.exitStatement = stmt;
    }

    public Statement getExitStatement()
    {
        return exitStatement;
    }

    public StatementList getStatementList()
    {
        return (StatementList) stmtListStack.peek();
    }

    public RexBuilder getRexBuilder()
    {
        return rexBuilder;
    }

    /**
     * Record the fact that instances of <code>rel</code> are available in
     * <code>variable</code>.
     */
    public void bind(SaffronRel rel,Variable variable)
    {
        bind(rel,new EagerBind(variable));
    }

    /**
     * Declare a variable, and bind it lazily, so it only gets initialized if
     * it is actually used.
     *
     * @return the Variable so declared
     */
    public Variable bind(
        SaffronRel rel,
        StatementList statementList,
        final VariableInitializer initializer)
    {
        VariableInitializerThunk thunk = new VariableInitializerThunk() {
            public VariableInitializer getInitializer()
            {
                return initializer;
            }
        };
        Variable variable = newVariable();
        LazyBind bind = new LazyBind(variable,statementList,rel.getRowType(),thunk);
        bind(rel,bind);
        return bind.getVariable();
    }

    /**
     * Shares a variable between relations.  <code>previous</code> already has
     * a variable, and calling this method indicates that <code>rel</code>'s
     * output will appear in this variable too.
     */
    public void bind(SaffronRel rel,SaffronRel previous)
    {
        bind(rel,new RelBind(previous));
    }

    /**
     * Binds a correlating variable. References to correlating variables such
     * as <code>$cor2</code> will be replaced with java variables such as
     * <code>$Oj14</code>.
     */
    public void bindCorrel(String correlName,Variable variable)
    {
        mapCorrelNameToVariable.put(correlName,variable);
    }

    public SaffronRel findRel(SaffronRel rel,RexNode expression)
    {
        if (expression instanceof RexInputRef) {
            RexInputRef variable = (RexInputRef) expression;
            if (rel instanceof JoinRel && false) {
                return findInputRel(rel,variable.index);
            } else {
                return rel.getInput(variable.index);
            }
        } else if (expression instanceof RexFieldAccess) {
            RexFieldAccess fieldAccess = (RexFieldAccess) expression;
            String fieldName = fieldAccess.getName();
            RexNode refExp = fieldAccess.getReferenceExpr();
            SaffronRel rel2 = findRel(rel,refExp); // recursive
            if (rel2 == null) {
                return null;
            }
            return rel2.implementFieldAccess(this,fieldName);
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
    public void generateParentBody(SaffronRel rel,StatementList stmtList)
    {
        if (stmtList != null) {
            pushStatementList(stmtList);
        }
        Frame frame = (Frame) mapRel2Frame.get(rel);
        bindDeferred(frame, rel);
        Object o = frame.parent.implement(this,frame.ordinal);
        assert(o == null);
        if (stmtList != null) {
            popStatementList(stmtList);
        }
    }

    private void bindDeferred(Frame frame, final SaffronRel rel) {
        final StatementList statementList = getStatementList();
        if (frame.bind == null) {
            // this relational expression has not bound itself, so we presume
            // that we can call its implementSelf() method
            if (!(rel instanceof JavaRel)) {
                throw Util.newInternal("In order to bind-deferred, a " +
                        "relational expression must implement JavaRel: " + rel);
            }
            LazyBind lazyBind = new LazyBind(newVariable(),
                    statementList,
                    rel.getRowType(),
                    new VariableInitializerThunk() {
                        public VariableInitializer getInitializer()
                        {
                            return ((JavaRel) rel).implementSelf(RelImplementor.this);
                        }
                    });
            bind(rel,lazyBind);
        } else if (frame.bind instanceof LazyBind &&
                (((LazyBind) frame.bind).statementList != statementList)) {
            // Frame is already bound, but to a variable declared in a different
            // scope. Re-bind it.
            final LazyBind lazyBind = (LazyBind) frame.bind;
            lazyBind.statementList = statementList;
            lazyBind.bound = false;
        }
    }

    /**
     * Implements a relational expression according to a calling convention.
     */
    public Object implementChild(
        SaffronRel parent,
        int ordinal,
        SaffronRel child)
    {
        if (parent != null) {
            assert(child == parent.getInputs()[ordinal]);
        }
        Frame frame = new Frame();
        frame.rel = child;
        frame.parent = parent;
        frame.ordinal = ordinal;
        mapRel2Frame.put(child,frame);
        String correl = child.getCorrelVariable();
        if (correl != null) {
            // Record that this frame is responsible for setting this
            // variable. But if another frame is already doing the job --
            // this frame's parent, which belongs to the same set -- don't
            // override it.
            if (mapCorrel2Frame.get(correl) == null) {
                mapCorrel2Frame.put(correl,frame);
            }
        }
        int ordinal2 = -1; // indicates that parent is calling child
        return child.implement(this,ordinal2);
    }

    /**
     * Starts an iteration, by calling {@link SaffronRel#implement} on the
     * root element.
     */
    public Object implementRoot(SaffronRel rel)
    {
        return implementChild(null,-1,rel);
    }

    /**
     * Creates an expression which references correlating variable
     * <code>correlName</code> from the context of <code>rel</code>.  For
     * example, if <code>correlName</code> is set by the 1st child of
     * <code>rel</code>'s 2nd child, then this method returns
     * <code>$input2.$input1</code>.
     */
    public Expression makeReference(String correlName,SaffronRel rel)
    {
        Frame frame = (Frame) mapCorrel2Frame.get(correlName);
        assert(frame != null);
        assert(Util.equal(frame.rel.getCorrelVariable(),correlName));
        assert(frame.hasVariable());
        return frame.getVariable();
    }

    public Variable newVariable()
    {
        return Variable.generateUniqueVariable();
    }

    public void popStatementList(StatementList stmtList)
    {
        assert(stmtList == getStatementList());
        stmtListStack.pop();
    }

    public void pushStatementList(StatementList stmtList)
    {
        stmtListStack.push(stmtList);
    }

    /**
     * Converts an expression in internal form (the input relation is
     * referenced using the variable <code>$input0</code>) to generated form
     * (the input relation is referenced using the bindings in this
     * <code>RelImplementor</code>).  Compare this method with {@link
     * net.sf.saffron.oj.xlat.QueryInfo#convertExpToInternal}, which converts
     * from source form to internal form.
     *
     * @param exp the expression to translate (it is cloned, not modified)
     * @param rel the relational expression which is the context for
     *        <code>exp</code>
     */
    public Expression translate(SaffronRel rel,RexNode exp)
    {
        Translator translator = newTranslator(this,rel);
        return translator.go(exp);
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
        SaffronRel rel,
        RexNode exp,
        StatementList stmtList,
        MemberDeclarationList memberList)
    {
        return translate(rel,exp);
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
        SaffronRel rel,
        SaffronType lhsType,
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
                    translateViaStatements(rel,rhs,stmtList,memberList))));
    }

    /**
     * Converts an array of expressions in internal into a list of expressions
     * in generated form.
     *
     * @see #translate(SaffronRel,RexNode)
     */
    public ExpressionList translateList(SaffronRel rel, RexNode[] exps) {
        final ExpressionList list = new ExpressionList();
        Translator translator = newTranslator(this,rel);
        for (int i = 0; i < exps.length; i++) {
            list.add(translator.go(exps[i]));
        }
        return list;
    }

    protected Translator newTranslator(
        RelImplementor relImplementor,SaffronRel rel)
    {
        return new Translator(relImplementor,rel);
    }

    /**
     * Creates an expression which references the <i>ordinal</i><sup>th</sup>
     * input.
     */
    public Expression translateInput(SaffronRel rel,int ordinal)
    {
        final SaffronType rowType = rel.getRowType();
        int fieldOffset = computeFieldOffset(rel, ordinal);
        return translate(rel,
                rexBuilder.makeRangeReference(rowType, fieldOffset));
    }

    /**
     * Returns the index of the first field in <code>rel</code> which comes
     * from its <code>ordinal</code>th input.
     *
     * <p>For example, if rel joins T0(A,B,C) to T1(D,E), then
     * countFields(0,rel) yields 0, and countFields(1,rel) yields 3.</p>
     */
    private int computeFieldOffset(SaffronRel rel, int ordinal) {
        if (ordinal == 0) {
            // short-circuit for the common case
            return 0;
        }
        int fieldOffset = 0;
        final SaffronRel [] inputs = rel.getInputs();
        for (int i = 0; i < ordinal; i++) {
            SaffronRel input = inputs[i];
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
        SaffronRel rel,
        int ordinal,
        int fieldOrdinal)
    {
        assert ordinal >= 0;
        assert ordinal < rel.getInputs().length;
        assert fieldOrdinal >= 0;
        assert fieldOrdinal < rel.getInput(ordinal).getRowType().getFieldCount();
        SaffronType rowType = rel.getRowType();
        final SaffronField [] fields = rowType.getFields();
        final int fieldIndex = computeFieldOffset(rel, ordinal) + fieldOrdinal;
        assert fieldIndex >= 0;
        assert fieldIndex < fields.length;
        final RexNode expr = rexBuilder.makeInputRef(fields[fieldIndex].getType(), fieldIndex);
        return translate(rel,expr);
    }

    /**
     * Record the fact that instances of <code>rel</code> are available via
     * <code>bind</code> (which may be eager or lazy).
     */
    private void bind(SaffronRel rel,Bind bind)
    {
        if (DebugOut.getDebugLevel() > 0) {
            DebugOut.println("Bind " + rel.toString() + " to " + bind);
        }
        Frame frame = (Frame) mapRel2Frame.get(rel);
        frame.bind = bind;
        boolean stupid =
            SaffronProperties.instance().getBooleanProperty(
                SaffronProperties.PROPERTY_saffron_stupid);
        if (stupid) {
            // trigger the declaration of the variable, even though it
            // may not be used
            Util.discard(bind.getVariable());
        }
    }

    private SaffronRel findInputRel(SaffronRel rel,int offset)
    {
        return findInputRel(rel,offset,new int [] { 0 });
    }

    private SaffronRel findInputRel(SaffronRel rel,int offset,int [] offsets)
    {
        if (rel instanceof JoinRel) {
            // no variable here -- go deeper
            SaffronRel [] inputs = rel.getInputs();
            for (int i = 0; i < inputs.length; i++) {
                SaffronRel result = findInputRel(inputs[i],offset,offsets);
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
    Variable findInputVariable(SaffronRel rel)
    {
        while (true) {
            Frame frame = (Frame) mapRel2Frame.get(rel);
            if ((frame != null) && frame.hasVariable()) {
                return frame.getVariable();
            }
            SaffronRel [] inputs = rel.getInputs();
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
        private final SaffronRel rel;

        RelBind(SaffronRel rel) {
            this.rel = rel;
        }

        public Variable getVariable() {
            final Frame frame = findFrame();
            return frame.getVariable();
        }

        private Frame findFrame() {
            SaffronRel previous = rel;
            while (true) {
                Frame frame = (Frame) mapRel2Frame.get(previous);
                if (frame.bind != null) {
                    if (DebugOut.getDebugLevel() > 0) {
                        DebugOut.println(
                            "Bind " + rel.toString() + " to "
                            + previous.toString() + "(" + frame.bind + ")");
                    }
                    return frame;
                }

                // go deeper
                SaffronRel [] inputs = previous.getInputs();
                assert (inputs.length == 1) : "input is not bound";
                previous = inputs[0];
            }
        }
    }

    private static class Frame
    {
        /** <code>rel</code>'s parent */
        SaffronRel parent;

        /** relation which is being implemented in this frame */
        SaffronRel rel;

        /** ordinal of <code>rel</code> within <code>parent</code> */
        int ordinal;

        /** Holds variable which hasn't been declared yet. */
        private Bind bind;

        /**
         * Retrieves the variable, executing the lazy bind if necessary.
         */
        Variable getVariable()
        {
            assert(hasVariable());
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
        final SaffronType type;
        final Statement after;
        StatementList statementList;
        final Variable variable;
        final VariableInitializerThunk thunk;
        boolean bound;

        LazyBind(
            Variable variable,
            StatementList statementList,
            SaffronType type,
            VariableInitializerThunk thunk)
        {
            this.variable = variable;
            this.statementList = statementList;
            this.after =
                (statementList.size() == 0) ? null
                                            : statementList.get(
                    statementList.size() - 1);
            this.type = type;
            this.thunk = thunk;
        }

        public Variable getVariable()
        {
            if (!bound) {
                bound = true;
                int position = find(statementList,after);
                VariableDeclaration varDecl =
                    new VariableDeclaration(
                        OJUtil.toTypeName(type),
                        variable.toString(),
                        null);
                statementList.insertElementAt(varDecl,position);
                varDecl.setInitializer(thunk.getInitializer());
            }
            return variable;
        }

        public String toString()
        {
            return super.toString() + "(variable=" + variable.toString()
            + ", thunk=" + thunk.toString() + ")";
        }

        private static int find(StatementList list,Statement statement)
        {
            if (statement == null) {
                return 0;
            } else {
                for (int i = 0,n = list.size(); i < n; i++) {
                    if (list.get(i) == statement) {
                        return i + 1;
                    }
                }
                throw Util.newInternal(
                    "could not find statement " + statement + " in list "
                    + list);
            }
        }
    }

    /**
     * <code>Translator</code> is a shuttle used to implement
     * {@link RelImplementor#translate}.
     */
    public static class Translator
    {
        protected RelImplementor implementor;
        protected SaffronRel rel;

        protected Translator(RelImplementor implementor,SaffronRel rel)
        {
            this.implementor = implementor;
            this.rel = rel;
        }

        public Expression go(RexNode rex) {
            if (rex instanceof RexCall) {
                final RexCall call = (RexCall) rex;
                RexNode[] operands = call.operands;
                Expression[] exprs = new Expression[operands.length];
                for (int i = 0; i < operands.length; i++) {
                    RexNode operand = operands[i];
                    exprs[i] = go(operand);
                }
                return convertCall(call,exprs);
            }
            if (rex instanceof JavaRowExpression) {
                return ((JavaRowExpression) rex).expression;
            }
            if (rex instanceof RexInputRef) {
                RexInputRef inputColRef = (RexInputRef) rex;
                WhichInputResult inputAndCol = whichInput(inputColRef.index, rel);
                if (inputAndCol == null) {
                    throw Util.newInternal("input not found");
                }
                final Variable v = implementor.findInputVariable(inputAndCol.input);
                final SaffronField field = inputAndCol.input.getRowType().getFields()[inputAndCol.fieldIndex];
                return OptUtil.makeFieldAccess(v, field.getName());
            }
            if (rex instanceof RexLiteral) {
                RexLiteral literal = (RexLiteral) rex;
                return convertLiteral(literal);
            }
            if (rex instanceof RexDynamicParam) {
                return convertDynamicParam((RexDynamicParam) rex);
            }
            if (rex instanceof RexContextVariable) {
                return convertContextVariable((RexContextVariable) rex);
            }
            if (rex instanceof RexRangeRef) {
                RexRangeRef range = (RexRangeRef) rex;
                final WhichInputResult inputAndCol = whichInput(range.offset, rel);
                if (inputAndCol == null) {
                    throw Util.newInternal("input not found");
                }
                final SaffronType inputRowType = inputAndCol.input.getRowType();
                // Simple case is if the range refers to every field of the
                // input. Return the whole input.
                final Variable inputExpr =
                        implementor.findInputVariable(inputAndCol.input);
                final SaffronType rangeType = range.getType();
                if (inputAndCol.fieldIndex == 0 &&
                        rangeType == inputRowType) {
                    return inputExpr;
                }
                // More complex case is if the range refers to a subset of
                // the input's fields. Generate "new Type(argN,...,argM)".
                final SaffronField [] rangeFields = rangeType.getFields();
                final SaffronField [] inputRowFields = inputRowType.getFields();
                final ExpressionList args = new ExpressionList();
                for (int i = 0; i < rangeFields.length; i++) {
                    args.add(new FieldAccess(inputExpr,
                            inputRowFields[inputAndCol.fieldIndex + i].getName()));
                }
                return new AllocationExpression(toOJClass(rangeType), args);
            }
            throw Util.needToImplement("Translate row-expression of kind " +
                    rex.getKind() + "(" + rex + ")");
        }

        protected Expression convertLiteral(RexLiteral literal)
        {
            final Object value = literal.getValue();
            if (value == null) {
                return Literal.constantNull();
            } else if (value instanceof String) {
                return Literal.makeLiteral((String) value);
            } else if (value instanceof SqlLiteral.StringLiteral) {
                return Literal.makeLiteral(((SqlLiteral.StringLiteral) value).getValue());
            } else if (value instanceof Boolean) {
                return Literal.makeLiteral((Boolean) value);
            } else if (value instanceof BigInteger) {
                long longValue = ((BigInteger) value).longValue();
                int intValue = (int) longValue;
                if (longValue == intValue) {
                    return Literal.makeLiteral(intValue);
                } else {
                    return Literal.makeLiteral(longValue);
                }
            } else if (value instanceof byte []) {
                return convertByteArrayLiteral((byte []) value);
            } else {
                throw Util.newInternal(
                    "Bad literal value " + value +
                    " (" + value.getClass() + "); breaches " +
                    "post-condition on RexLiteral.getValue()");
            }
        }

        protected ArrayInitializer convertByteArrayLiteralToInitializer(
            byte [] bytes)
        {
            ExpressionList byteList = new ExpressionList();
            for (int i = 0; i < bytes.length; ++i) {
                byteList.add(Literal.makeLiteral(bytes[i]));
            }
            return new ArrayInitializer(byteList);
        }

        protected Expression convertByteArrayLiteral(byte [] bytes)
        {
            return new ArrayAllocationExpression(
                TypeName.forOJClass(OJSystem.BYTE),
                new ExpressionList(null),
                convertByteArrayLiteralToInitializer(bytes));
        }

        /**
         * Returns the ordinal of the input relational expression which a given
         * column ordinal comes from.
         *
         * <p>For example, if <code>rel</code> has inputs
         * <code>I(a, b, c)</code> and <code>J(d, e)</code>, then
         * <code>whichInput(0, rel)</code> returns 0 (column a),
         * <code>whichInput(2, rel)</code> returns 0 (column c),
         * <code>whichInput(3, rel)</code> returns 1 (column d).</p>
         *
         * @param fieldIndex Index of field
         * @param rel   Relational expression
         * @return      a {@link WhichInputResult} if found, otherwise null.
         */
        private static WhichInputResult whichInput(int fieldIndex, SaffronRel rel) {
            assert fieldIndex >= 0;
            final SaffronRel [] inputs = rel.getInputs();
            for (int inputIndex = 0, firstFieldIndex = 0;
                 inputIndex < inputs.length; inputIndex++) {
                SaffronRel input = inputs[inputIndex];
                // Index of first field in next input. Special case if this
                // input has no fields: it's ambiguous (we could be looking
                // at the first field of the next input) but we allow it.
                final int fieldCount = input.getRowType().getFieldCount();
                final int lastFieldIndex = firstFieldIndex + fieldCount;
                if (lastFieldIndex > fieldIndex ||
                        fieldCount == 0 && lastFieldIndex == fieldIndex) {
                    final int fieldIndex2 = fieldIndex - firstFieldIndex;
                    return new WhichInputResult(input, inputIndex, fieldIndex2);
                }
                firstFieldIndex = lastFieldIndex;
            }
            return null;
        }

		/**
		 * Result of call to {@link #whichInput}, contains the input relational
         * expression, its index, and the index of the field within that
         * relational expression.
		 */
        private static class WhichInputResult {
            WhichInputResult(SaffronRel input, int inputIndex,int fieldIndex) {
                this.input = input;
                this.inputIndex = inputIndex;
                this.fieldIndex = fieldIndex;
            }
            final SaffronRel input;
            final int inputIndex;
            final int fieldIndex;
        }

        protected Expression convertCall(RexCall call, Expression[] operands) {
            SqlOperator op;
            op = call.op;
            if (op instanceof SqlBinaryOperator) {
                Integer binaryExpNum = (Integer)
                        implementor.mapRexBinaryToOpenJava.get(call.op);
                if (binaryExpNum != null) {
                    return new BinaryExpression(
                            operands[0],
                            binaryExpNum.intValue(),
                            operands[1]);
                }
            } else if (op instanceof SqlFunction) {
                if (call.op.equals(implementor.rexBuilder.funcTab.cast)) {
                    OJClass type = ((OJTypeFactory) implementor.rexBuilder.getTypeFactory()).toOJClass(null, call.getType());
                    return new CastExpression(type, operands[0]);
                }
            }
            // TODO: Get rid of this if-else-if... logic: use a table to do
            // the mapping of rex calls to oj expressions.
            throw Util.needToImplement("Row-expression " + call);
        }

        private OJClass toOJClass(final SaffronType saffronType) {
            return OJUtil.typeToOJClass(saffronType);
        }


        protected Expression convertDynamicParam(RexDynamicParam dynamicParam) {
            throw Util.needToImplement("Row-expression RexDynamicParam");
        }

        protected Expression convertContextVariable(
            RexContextVariable contextVariable)
        {
            throw Util.needToImplement("Row-expression RexContextVariable");
        }
    }
}


// End RelImplementor.java
