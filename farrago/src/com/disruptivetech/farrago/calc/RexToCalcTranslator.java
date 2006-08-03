/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2002-2006 Disruptive Tech
// Copyright (C) 2005-2006 The Eigenbase Project
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
package com.disruptivetech.farrago.calc;

import java.util.*;

import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * Converts expressions in logical format ({@link RexNode}) into calculator
 * assembly-language programs.
 *
 * @author Wael Chatila
 * @version $Id$
 * @see CalcProgramBuilder
 * @since Feb 5, 2004
 */
public class RexToCalcTranslator
    implements RexVisitor<CalcReg>
{

    //~ Instance fields --------------------------------------------------------

    // The following 3 fields comprise the program; they are reset each time a
    // new program is started.
    final CalcProgramBuilder builder = new CalcProgramBuilder();
    CalcReg trueReg = builder.newBoolLiteral(true);
    CalcReg falseReg = builder.newBoolLiteral(false);
    private int nullRegOrdinal = -1;

    protected final CalcRexImplementorTable implementorTable;

    /**
     * Eliminates common-subexpressions, by mapping every expression (the key is
     * generated from a {@link RexNode} using {@link #getKey(RexNode)}) to the
     * {@link CalcReg} which holds its value.
     */
    private ExpressionScope scope = new ExpressionScope();

    protected final RexBuilder rexBuilder;

    /**
     * The relational expression this program is being generated for.
     */
    private final RelNode rel;

    /**
     * Whether the code generator should short-circuit logical operators. The
     * default value is <em>false</em>.
     */
    protected boolean generateShortCircuit = false;
    protected int labelOrdinal = 0;

    /**
     * Ordered mapping from Saffron types (representing a family of types) to
     * the corresponding calculator type. The ordering ensures determinacy.
     */
    private final TypePair [] knownTypes;

    /**
     * @deprecated this can be removed
     */
    private AggOp aggOp;
    private RexNode [] inputExprs = null;
    private RexProgram program;

    /**
     * List of expressions which are currently being implemented.
     */
    final Set<RexNode> inProgressNodeSet = new HashSet<RexNode>();

    //~ Constructors -----------------------------------------------------------

    public RexToCalcTranslator(
        RexBuilder rexBuilder,
        RelNode rel)
    {
        this.rexBuilder = rexBuilder;
        this.rel = rel;

        RelOptPlanner planner = rel.getCluster().getPlanner();
        if (planner instanceof FarragoSessionPlanner) {
            FarragoSessionPreparingStmt preparingStmt =
                ((FarragoSessionPlanner) planner).getPreparingStmt();

            CalcRexImplementorTable comp =
                preparingStmt.getSession().getPersonality().newComponentImpl(
                    CalcRexImplementorTable.class);
            if (comp != null) {
                implementorTable = comp;
            } else {
                assert (false);
                implementorTable = CalcRexImplementorTableImpl.std();
            }
        } else {
            // Not a FarragoSessionPlanner?  There's going to be
            // trouble eventually.
            implementorTable = CalcRexImplementorTableImpl.std();
        }

        setGenerateComments(
            SaffronProperties.instance().generateCalcProgramComments.get());
        RelDataTypeFactory fac = this.rexBuilder.getTypeFactory();
        knownTypes = new TypePair[] { new TypePair(
                    fac.createSqlType(SqlTypeName.Tinyint),
                    CalcProgramBuilder.OpType.Int1), new TypePair(
                    fac.createSqlType(SqlTypeName.Smallint),
                    CalcProgramBuilder.OpType.Int2), new TypePair(
                    fac.createSqlType(SqlTypeName.Integer),
                    CalcProgramBuilder.OpType.Int4), new TypePair(
                    fac.createSqlType(SqlTypeName.Bigint),
                    CalcProgramBuilder.OpType.Int8), new TypePair(
                    fac.createSqlType(SqlTypeName.Decimal),
                    CalcProgramBuilder.OpType.Int8), new TypePair(
                    fac.createSqlType(SqlTypeName.Float),
                    CalcProgramBuilder.OpType.Double), new TypePair(
                    fac.createSqlType(SqlTypeName.Double),
                    CalcProgramBuilder.OpType.Double), new TypePair(
                    fac.createSqlType(SqlTypeName.Real),
                    CalcProgramBuilder.OpType.Real), new TypePair(
                    fac.createSqlType(SqlTypeName.Varbinary, 0),
                    CalcProgramBuilder.OpType.Varbinary), new TypePair(
                    fac.createSqlType(SqlTypeName.Varchar, 0),
                    CalcProgramBuilder.OpType.Varchar), new TypePair(
                    fac.createSqlType(SqlTypeName.Boolean),
                    CalcProgramBuilder.OpType.Bool), 

                // FIXME: not right for T/w TZ.
                new TypePair(
                    fac.createSqlType(SqlTypeName.Date),
                    CalcProgramBuilder.OpType.Int8), new TypePair(
                    fac.createSqlType(SqlTypeName.Time),
                    CalcProgramBuilder.OpType.Int8), new TypePair(
                    fac.createSqlType(SqlTypeName.Timestamp),
                    CalcProgramBuilder.OpType.Int8),

                new TypePair(
                    fac.createJavaType(Byte.class),
                    CalcProgramBuilder.OpType.Int1), new TypePair(
                    fac.createJavaType(byte.class),
                    CalcProgramBuilder.OpType.Int1), new TypePair(
                    fac.createJavaType(Short.class),
                    CalcProgramBuilder.OpType.Int2), new TypePair(
                    fac.createJavaType(short.class),
                    CalcProgramBuilder.OpType.Int2), new TypePair(
                    fac.createJavaType(Integer.class),
                    CalcProgramBuilder.OpType.Int4), new TypePair(
                    fac.createJavaType(int.class),
                    CalcProgramBuilder.OpType.Int4), new TypePair(
                    fac.createJavaType(Long.class),
                    CalcProgramBuilder.OpType.Int8), new TypePair(
                    fac.createJavaType(long.class),
                    CalcProgramBuilder.OpType.Int8), new TypePair(
                    fac.createJavaType(Double.class),
                    CalcProgramBuilder.OpType.Double), new TypePair(
                    fac.createJavaType(double.class),
                    CalcProgramBuilder.OpType.Double), new TypePair(
                    fac.createJavaType(Float.class),
                    CalcProgramBuilder.OpType.Real), new TypePair(
                    fac.createJavaType(float.class),
                    CalcProgramBuilder.OpType.Real), new TypePair(
                    fac.createJavaType(String.class),
                    CalcProgramBuilder.OpType.Varchar), new TypePair(
                    fac.createJavaType(Boolean.class),
                    CalcProgramBuilder.OpType.Bool), new TypePair(
                    fac.createJavaType(boolean.class),
                    CalcProgramBuilder.OpType.Bool), };
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the relational expression this program is being generated for.
     */
    public RelNode getRelNode()
    {
        return rel;
    }

    private void clearProgram(RexProgram program)
    {
        builder.clear();
        trueReg = builder.newBoolLiteral(true);
        falseReg = builder.newBoolLiteral(false);
        scope.clear();
        nullRegOrdinal = -1;
        inputExprs = null;
        this.program = program;
    }

    public String newLabel()
    {
        return "label$" + (labelOrdinal++);
    }

    /**
     * Returns whether an expression can be translated.
     *
     * @param node Expression
     * @param deep Whether to check child expressions
     */
    public boolean canTranslate(
        RexNode node,
        boolean deep)
    {
        try {
            node.accept(new TranslationTester(this, deep));
            return true;
        } catch (TranslationException e) {
            // We don't consider a TranslationException to be an error -- it's
            // just a convenient way to abort a traversal.
            Util.swallow(e, null);
            return false;
        }
    }

    /**
     * Returns whether a program can be translated.
     */
    public boolean canTranslate(RexProgram program)
    {
        boolean deep = true;
        final TranslationTester tester = new TranslationTester(this, deep);
        try {
            for (RexNode expr : program.getExprList()) {
                expr.accept(tester);
            }
            return true;
        } catch (TranslationException e) {
            // We don't consider a TranslationException to be an error -- it's
            // just a convenient way to abort a traversal.
            Util.swallow(e, null);
            return false;
        }
    }

    CalcProgramBuilder.RegisterDescriptor getCalcRegisterDescriptor(
        RexNode node)
    {
        return getCalcRegisterDescriptor(node.getType());
    }

    CalcProgramBuilder.RegisterDescriptor getCalcRegisterDescriptor(
        RelDataType relDataType)
    {
        String typeDigest = relDataType.toString();

        // Special case for Char and Binary, because have the same
        // respective relDataType families as Varchar and Varbinary, but the
        // calc needs to treat them differently.
        CalcProgramBuilder.OpType calcType = null;
        if (typeDigest.startsWith("CHAR")) {
            calcType = CalcProgramBuilder.OpType.Char;
        } else if (typeDigest.startsWith("BINARY")) {
            calcType = CalcProgramBuilder.OpType.Binary;
        } else if (typeDigest.endsWith("MULTISET")) {
            // hack for now
            return
                new CalcProgramBuilder.RegisterDescriptor(
                    CalcProgramBuilder.OpType.Varbinary,
                    4096);
        } else if (typeDigest.startsWith("INTERVAL")) {
            return
                new CalcProgramBuilder.RegisterDescriptor(
                    CalcProgramBuilder.OpType.Int8,
                    -1);
        }
        for (int i = 0; i < knownTypes.length; i++) {
            TypePair knownType = knownTypes[i];
            if (SqlTypeUtil.sameNamedType(relDataType, knownType.relDataType)) {
                calcType = knownType.opType;
                break;
            }
        }

        if (null == calcType) {
            throw Util.newInternal("unknown type " + relDataType);
        }

        int bytes;
        switch (calcType.getOrdinal()) {
        case CalcProgramBuilder.OpType.Binary_ordinal:
        case CalcProgramBuilder.OpType.Char_ordinal:
        case CalcProgramBuilder.OpType.Varbinary_ordinal:
        case CalcProgramBuilder.OpType.Varchar_ordinal:
            bytes = SqlTypeUtil.getMaxByteSize(relDataType);
            if (bytes < 0) {
                bytes = 0;
            }
            break;
        default:
            bytes = -1;
        }

        return new CalcProgramBuilder.RegisterDescriptor(calcType, bytes);
    }

    public String getKey(RexNode node)
    {
        if (node instanceof RexSlot) {
            return ((RexSlot) node).getName();
        }
        return node.toString() + node.getType().toString();
    }

    public CalcReg setResult(
        RexNode node,
        CalcReg register)
    {
        final String key = getKey(node);
        scope.set(key, register);
        return register;
    }

    /**
     * Returns the register which contains the result of evaluating the given
     * expression.
     *
     * <p>If <code>failIfNotFound</code>, the node must already have been
     * implemented.
     *
     * <p>To check whether the result exists, use {@link
     * #containsResult(RexNode)}, or pass in <code>failIfNotFound =
     * false</code>.
     */
    CalcReg getResult(RexNode node,
        boolean failIfNotFound)
    {
        final String key = getKey(node);
        CalcReg found = scope.get(key);
        if ((found == null) && failIfNotFound) {
            throw Util.newInternal(
                "Expression " + node
                + " has not been implemented as a register");
        }
        return found;
    }

    /**
     * Returns whether a given expression has been implemented as a register.
     *
     * @see #getResult(RexNode,boolean)
     */
    boolean containsResult(RexNode node)
    {
        return scope.get(getKey(node)) != null;
    }

    /**
     * @deprecated Remove all usages of this method (they all seem to have the
     * same pattern) and put the pattern into the translator, and remove the
     * {@link #inputExprs} field
     */
    protected RexNode [] getInputExprs()
    {
        return inputExprs;
    }

    protected int getNullRegisterOrdinal()
    {
        return nullRegOrdinal;
    }

    protected void setNullRegisterOrdinal(int ordinal)
    {
        nullRegOrdinal = ordinal;
    }

    /**
     * Translates an array of project expressions and an optional filter
     * expression into a {@link CalcProgramBuilder} calculator program using a
     * depth-first recursive algorithm.
     *
     * <p>This method is NOT stateless. TODO: Make method stateless -- so you
     * can call this method several times with different inputs -- and therefore
     * the translator is re-usable.
     *
     * @param inputRowType The type of the input row to the calculator. If
     * <code>inputRowType</code> is not null, the program contains an input
     * register for every field in the input row type; otherwise it contains
     * inputs for only those fields used.
     * @param program Program consisting of a set of common expressions, a list
     * of expressions to project, and an optional condition. Must not be null,
     * but the project list may be empty.
     */
    public String generateProgram(
        RelDataType inputRowType,
        RexProgram program)
    {
        final List<RexLocalRef> projectRefList = program.getProjectList();
        RexNode conditionRef = program.getCondition();
        aggOp = AggOp.None;

        clearProgram(program);

        // Step 0. Create input fields.
        // Create a calculator input for each field in the input relation,
        // regardless of whether the calcualtor program uses them.
        if (inputRowType != null) {
            final RexLocalRef [] inputRefs = RexUtil.toLocalRefs(inputRowType);
            for (int i = 0; i < inputRefs.length; i++) {
                RexLocalRef inputRef = inputRefs[i];
                implement(inputRef);
            }
        }

        // Step 1: implement all the filtering logic
        if (conditionRef != null) {
            CalcReg filterResult =
                implementNode(conditionRef);
            assert CalcProgramBuilder.OpType.Bool == filterResult.getOpType() : "Condition must be boolean: "
                + conditionRef;

            // Step 2: report the status of the filtering
            CalcReg statusReg =
                builder.newStatus(CalcProgramBuilder.OpType.Bool, -1);

            // Step 2-1: figure out the status
            String prepareOutput = newLabel();
            builder.addLabelJumpTrue(prepareOutput, filterResult);

            // row didn't match
            CalcProgramBuilder.move.add(builder, statusReg, trueReg);
            builder.addReturn();
            builder.addLabel(prepareOutput);
            CalcProgramBuilder.move.add(builder, statusReg, falseReg);
        }

        // row matched. Now calculate all the outputs
        for (RexLocalRef projectRef : projectRefList) {
            // This could probably be optimized by writing to the outputs
            // directly instead of temp registers but harder to (java)
            // implement.
            implementNode(projectRef);
        }

        // all outputs calculated, now assign results to outputs by reference
        CalcReg isNullRes = null;
        for (RexLocalRef projectRef : projectRefList) {
            CalcProgramBuilder.RegisterDescriptor desc =
                getCalcRegisterDescriptor(projectRef);
            CalcReg res = getResult(projectRef, true);

            // check and assert that if type was declared as NOT NULLABLE, value
            // is not null
            if (!projectRef.getType().isNullable()) {
                String wasNotNull = newLabel();
                if (isNullRes == null) {
                    isNullRes =
                        builder.newLocal(CalcProgramBuilder.OpType.Bool, -1);
                }
                CalcProgramBuilder.boolNativeIsNull.add(builder,
                    isNullRes,
                    res);
                builder.addLabelJumpFalse(wasNotNull, isNullRes);
                CalcProgramBuilder.raise.add(
                    builder,
                    builder.newVarcharLiteral(
                        SqlStateCodes.NullValueNotAllowed.getState(),
                        5));
                builder.addReturn();
                builder.addLabel(wasNotNull);
            }

            // if ok, assign result to output by reference
            CalcReg outReg = builder.newOutput(desc);
            builder.addRef(outReg, res);
        }

        builder.addReturn();
        return builder.getProgram();
    }

    /**
     * Translates an array of project expressions and an optional filter
     * expression into a {@link CalcProgramBuilder} calculator program using a
     * depth-first recursive algorithm when there are aggregate functions.
     *
     * <p>This method is NOT stateless. TODO: Make method stateless -- so you
     * can call this method several times with different inputs -- and therefore
     * the translator is re-usable.
     *
     * @param program Program, containing pre-expressions, aggregate
     * expressions, post-expressions, and optionally a filter.
     * @param aggOp Aggregate operation (INIT, ADD, or DROP), must not be null.
     */
    public String getAggProgram(
        RexProgram program,
        AggOp aggOp)
    {
        // The input expressions for the case where there are calculator
        // expressions that should be computed prior to aggregate function
        // calculation. For example, in SUM(a + b), inputExprs is (a + b).
        RexNode [] inputExprs = null; // todo: remove
        RexNode [] aggs = null;

        // Array of expressions to be projected. Must not be null, may be
        // empty.
        RexLocalRef [] projectExprs =
            program.getProjectList().toArray(
                new RexLocalRef[program.getProjectList().size()]);

        // Filter expression. May be null.
        RexLocalRef conditionExp = program.getCondition();

        RelDataType inputRowType = program.getInputRowType();

        clearProgram(program);

        // Inner Calc Rel has information about expressions needed for arguments
        // to the aggregate function. So it has RexInputRef's and RexCall's.
        // This information is needed for add and drop programs.
        //
        // Windowed Agg Rel has information about the aggregate functions alone. It
        // has RexCall's that calls the interface methods for SUM, COUNT.
        //
        // Outer Calc Rel has information about the output expressions. It has
        // RexInputRef's and RexCall's. This information is needed for output
        // program alone. It also needs the outputs of Windowed Agg Rel as
        // inputs.

        this.aggOp = aggOp;
        assert aggOp != AggOp.None;
        this.inputExprs = inputExprs;

        // Validate aggOp.
        assert aggOp != null;
        switch (aggOp.getOrdinal()) {
        case AggOp.None_ordinal:
        case AggOp.Init_ordinal:
        case AggOp.Add_ordinal:
        case AggOp.Drop_ordinal:
            break;
        default:
            throw aggOp.unexpected();
        }

        // Step 0. Create input fields.
        // Create a calculator input for each field in the input relation,
        // regardless of whether the calculator program uses them.
        if (inputRowType != null) {
            final RexInputRef [] inputRefs = RexUtil.toInputRefs(inputRowType);
            for (int i = 0; i < inputRefs.length; i++) {
                RexInputRef inputRef = inputRefs[i];
                implement(inputRef);
            }

            // Additional inputs for the aggregate functions.
            switch (aggOp.getOrdinal()) {
            case AggOp.None_ordinal:
                List<RexNode> al = new ArrayList<RexNode>(1);
                Map<String,RexNode> dups = new HashMap<String, RexNode>();

                // Change the projectExprs to map to the correct input refs.
                fixOutputExps(al,
                    inputRefs.length,
                    inputExprs,
                    aggs,
                    projectExprs,
                    dups);
                if (conditionExp != null) {
                    fixOutputExps(
                        al,
                        inputRefs.length,
                        inputExprs,
                        aggs,
                        new RexNode[] { conditionExp },
                        dups);
                }
                for (int i = 0; i < al.size(); i++) {
                    implementNode(al.get(i));
                }
                break;
            default:
                break;
            }
        }

        // Step 1: implement all the filtering logic
        if (conditionExp != null) {
            CalcReg filterResult =
                implementNode(conditionExp);
            assert CalcProgramBuilder.OpType.Bool == filterResult.getOpType() :
                "Condition must be boolean: " + conditionExp;

            //step 2: report the status of the filtering
            CalcReg statusReg =
                builder.newStatus(CalcProgramBuilder.OpType.Bool, -1);

            //step 2-1: figure out the status
            String prepareOutput = newLabel();
            builder.addLabelJumpTrue(prepareOutput, filterResult);

            // row didnt match
            CalcProgramBuilder.move.add(builder, statusReg, trueReg);
            builder.addReturn();
            builder.addLabel(prepareOutput);
            CalcProgramBuilder.move.add(builder, statusReg, falseReg);
        }

        // row matched. Now calculate all the outputs
        // The projectExprs are the overs for a given partition.
        for (int i = 0; i < projectExprs.length; i++) {
            RexNode node = projectExprs[i];
            implementNode(node);
        }

        // Ref instructions for output program.
        switch (aggOp.getOrdinal()) {
        case AggOp.None_ordinal:
            for (int i = 0; i < projectExprs.length; i++) {
                RexNode node = projectExprs[i];
                CalcProgramBuilder.RegisterDescriptor desc =
                    getCalcRegisterDescriptor(node);
                CalcReg res = getResult(node, true);

                //if ok, assign result to output by reference
                CalcReg outReg = builder.newOutput(desc);
                builder.addRef(outReg, res);
            }
            break;
        default:
            break;
        }

        builder.addReturn();
        return builder.getProgram();
    }

    private void fixOutputExps(
        List<RexNode> extInputs,
        int start,
        RexNode [] inputExps,
        RexNode [] aggs,
        RexNode [] outputExps,
        Map<String,RexNode> dups)
    {
        // Review (murali 2005/08/03): Notice that we are changing the contents
        // of the outputExps here. It may be better to pass a copy of the
        // outputExps to this method if FennelWindowRel's member variable
        // contents need to be preserved.
        for (int i = 0; i < outputExps.length; i++) {
            RexNode node = outputExps[i];
            if (node instanceof RexInputRef) {
                RexNode aggNode = aggs[((RexInputRef) node).getIndex()];
                if (aggNode instanceof RexInputRef) {
                    // Need to get the external input.
                    RexNode inpNode =
                        inputExps[((RexInputRef) aggNode).getIndex()];
                    assert inpNode instanceof RexInputRef;

                    // Review (murali 2005/08/03): Should we clone the inpNode?
                    outputExps[i] = inpNode;
                } else if (aggNode instanceof RexOver) {
                    // This should be aggregate input. Create a new input ref
                    // with the next location.
                    String key = getKey(aggNode);
                    if (dups.containsKey(key)) {
                        outputExps[i] = dups.get(key);
                        continue;
                    }
                    RexNode extInput =
                        new RexInputRef(
                            extInputs.size() + start,
                            aggNode.getType());
                    extInputs.add(extInput);
                    outputExps[i] = extInput;
                    dups.put(key, extInput);
                }
            } else if (node instanceof RexCall) {
                fixOutputExps(
                    extInputs,
                    start,
                    inputExps,
                    aggs,
                    ((RexCall) node).getOperands(),
                    dups);
            }
        }
    }

    /**
     * Adds instructions to implement an expression to the program, and returns
     * the register which returns the result. If the expression has already been
     * implemented, does not add more instructions, just returns the existing
     * register.
     *
     * @param node Expression
     *
     * @return Register which holds the result of evaluating the expression,
     * never null, and always equivalent to calling {@link #getResult}.
     *
     * @post result != null
     * @post result == getResult(node)
     */
    public CalcReg implementNode(RexNode node)
    {
        return implementNode(node, true);
    }

    /**
     * Adds instructions to implement an expression to the program, and returns
     * the register which returns the result.
     *
     * <p>The <code>useCache</code> parmaeter controls whether to return the
     * existing register if the expression has already been implemented.
     *
     * @param node Expression
     * @param useCache Whether to look up the expression in the cache of already
     * implemented expressions
     *
     * @return Register which holds the result of evaluating the expression,
     * never null, and always equivalent to calling {@link #getResult}.
     *
     * @post result != null
     * @post result == getResult(node)
     */
    public CalcReg implementNode(
        RexNode node,
        boolean useCache)
    {
        CalcReg result;
        if (useCache) {
            result = getResult(node, false);
            if (result != null) {
                return result;
            }
        } else {
            boolean added = inProgressNodeSet.add(node);
            assert added;
        }
        node.accept(this);
        if (!useCache) {
            final boolean removed = inProgressNodeSet.remove(node);
            assert removed;
        }
        result = getResult(node, true);
        assert result != null : "post: result != null";
        return result;
    }

    public CalcReg visitInputRef(RexInputRef inputRef)
    {
        return implement(inputRef);
    }

    public CalcReg visitLocalRef(RexLocalRef localRef)
    {
        return implement(localRef);
    }

    public CalcReg visitLiteral(RexLiteral literal)
    {
        final CalcReg register = scope.get(getKey(literal));
        if (register != null) {
            return register;
        }
        return implement(literal);
    }

    public CalcReg visitCall(RexCall call)
    {
        CalcReg register = scope.get(getKey(call));
        if (register != null) {
            return register;
        }
        int before = builder.getCurrentLineNumber();
        register = implement(call);
        int after = builder.getCurrentLineNumber();
        assert (after >= before);
        if (0 != (after - before)) {
            builder.addComment(call.toString());
        }
        return register;
    }

    public CalcReg visitOver(RexOver over)
    {
        throw FarragoResource.instance().ProgramImplementationError.ex(
            "Don't know how to implement rex node=" + over);
    }

    public CalcReg visitCorrelVariable(
        RexCorrelVariable correlVariable)
    {
        return implement(correlVariable);
    }

    public CalcReg visitDynamicParam(
        RexDynamicParam dynamicParam)
    {
        throw FarragoResource.instance().ProgramImplementationError.ex(
            "Don't know how to implement rex node=" + dynamicParam);
    }

    public CalcReg visitRangeRef(RexRangeRef rangeRef)
    {
        throw FarragoResource.instance().ProgramImplementationError.ex(
            "Don't know how to implement rex node=" + rangeRef);
    }

    public CalcReg visitFieldAccess(
        RexFieldAccess fieldAccess)
    {
        final RexNode expr = fieldAccess.getReferenceExpr();
        if (expr instanceof RexCorrelVariable) {
            return implement(fieldAccess);
        }
        throw FarragoResource.instance().ProgramImplementationError.ex(
            "Don't know how to implement rex node=" + fieldAccess);
    }

    private CalcReg implementShortCircuit(RexCall call)
    {
        assert (call.operands.length == 2) : "not a binary operator";
        if (containsResult(call)) {
            throw new AssertionError(
                "Shouldn't call this function directly;"
                + " use implementNode(RexNode) instead");
        }
        SqlOperator op = call.getOperator();

        if (op.getKind().isA(SqlKind.And) || (op.getKind().isA(SqlKind.Or))) {
            //first operand of AND/OR
            CalcReg reg0 = implementNode(call.operands[0]);
            String shortCut = newLabel();

            //Check if we can make a short cut
            if (op.getKind().isA(SqlKind.And)) {
                builder.addLabelJumpFalse(shortCut, reg0);
            } else {
                builder.addLabelJumpTrue(shortCut, reg0);
            }

            //second operand
            CalcReg reg1 = implementNode(call.operands[1]);
            CalcReg result =
                builder.newLocal(CalcProgramBuilder.OpType.Bool, -1);
            assert result.getOpType().getOrdinal()
                == getCalcRegisterDescriptor(call).getType().getOrdinal();
            CalcProgramBuilder.move.add(builder, result, reg1);

            String restOfInstructions = newLabel();
            builder.addLabelJump(restOfInstructions);
            builder.addLabel(shortCut);

            if (op.getKind().isA(SqlKind.And)) {
                CalcProgramBuilder.move.add(builder, result, falseReg);
            } else {
                CalcProgramBuilder.move.add(builder, result, trueReg);
            }

            setResult(call, result);

            //WARNING this assumes that more instructions will follow.
            //Return is currently always at the end.
            builder.addLabel(restOfInstructions);
            return result;
        } else {
            throw FarragoResource.instance().ProgramImplementationError.ex(
                op.toString());
        }
    }

    private CalcReg implement(RexCall call)
    {
        if (containsResult(call)) {
            throw new AssertionError(
                "Shouldn't call this function directly;"
                + " use implementNode(RexNode) instead");
        }

        SqlOperator op = call.getOperator();

        // Check if and/or/xor should short circuit.
        if (generateShortCircuit
            && (op.getKind().isA(SqlKind.And)
                || op.getKind().isA(SqlKind.Or))) {
            return implementShortCircuit(call);
        }

        // Do table-driven implementation if possible.
        // TODO: Put ALL operator implementation code in this table, except
        //   perhaps for the most fundamental and idiosyncratic operators.
        CalcReg resultOfCall = null;
        CalcProgramBuilder.RegisterDescriptor resultDesc =
            getCalcRegisterDescriptor(call);

        /** SPECIAL CASE.  =,<>,>,<,>-=,<= already are defined in
         * {@link CalcRexImplementorTableImpl}
         * but need to do some acrobatics since those calc instructions are not defined
         * against varchars
         */
        if (isStrCmp(call)) {
            CalcReg reg1 = implementNode(call.operands[0]);
            CalcReg reg2 = implementNode(call.operands[1]);

            CalcReg [] convRegs = { reg1, reg2 };
            implementConversionIfNeeded(call.operands[0],
                call.operands[1],
                convRegs,
                false);
            reg1 = convRegs[0];
            reg2 = convRegs[1];

            resultOfCall = builder.newLocal(resultDesc);

            assert resultDesc.getType() == CalcProgramBuilder.OpType.Bool;
            CalcReg strCmpResult =
                builder.newLocal(CalcProgramBuilder.OpType.Int4, -1);
            if (SqlTypeUtil.inCharFamily(call.operands[0].getType())) {
                String collationToUse =
                    SqlCollation.getCoercibilityDyadicComparison(
                        call.operands[0].getType().getCollation(),
                        call.operands[1].getType().getCollation());
                CalcReg colReg = builder.newVarcharLiteral(collationToUse);

                // TODO: this is only for ascii cmp. Need to pump in colReg
                // when a fennel function that can take it is born.
                ExtInstructionDefTable.strCmpA.add(
                    builder,
                    new CalcReg[] {
                        strCmpResult, reg1, reg2
                    });
            } else {
                ExtInstructionDefTable.strCmpOct.add(
                    builder,
                    new CalcReg[] {
                        strCmpResult, reg1, reg2
                    });
            }

            CalcReg zero = builder.newInt4Literal(0);
            CalcReg [] regs =
                {
                    resultOfCall, strCmpResult, zero
                };
            if (op.getKind().isA(SqlKind.Equals)) {
                CalcProgramBuilder.boolNativeEqual.add(builder, regs);
            } else if (op.getKind().isA(SqlKind.NotEquals)) {
                CalcProgramBuilder.boolNativeNotEqual.add(builder, regs);
            } else if (op.getKind().isA(SqlKind.GreaterThan)) {
                CalcProgramBuilder.boolNativeGreaterThan.add(builder, regs);
            } else if (op.getKind().isA(SqlKind.LessThan)) {
                CalcProgramBuilder.boolNativeLessThan.add(builder, regs);
            } else if (op.getKind().isA(SqlKind.GreaterThanOrEqual)) {
                CalcProgramBuilder.boolNativeGreaterOrEqualThan.add(builder,
                    regs);
            } else if (op.getKind().isA(SqlKind.LessThanOrEqual)) {
                CalcProgramBuilder.boolNativeLessOrEqualThan.add(builder, regs);
            } else {
                throw Util.newInternal("Unknown op " + op);
            }
            setResult(call, resultOfCall);
            return resultOfCall;
        }

        // Ask implementor table for if op exists.
        // There are some special cases: see above and below.
        CalcRexImplementor implementor = implementorTable.get(op);
        if ((implementor != null) && implementor.canImplement(call)) {
            CalcReg reg = implementor.implement(call, this);
            setResult(call, reg);
            return reg;
        }

        // Maybe it's an aggregate function.
        if (op instanceof SqlAggFunction) {
            SqlAggFunction aggFun = (SqlAggFunction) op;
            CalcRexAggImplementor aggImplementor =
                implementorTable.getAgg(aggFun);
            if (aggImplementor != null) {
                // Create an output register to be the accumulator. We want to
                // avoid creating local registers and moving them into output
                // registers.
                CalcReg register =
                    builder.newOutput(getCalcRegisterDescriptor(call));
                switch (aggOp.getOrdinal()) {
                case AggOp.None_ordinal:
                    throw Util.newInternal(
                        "Cannot generate calc program: Aggregate call "
                        + call + " found in non-aggregating context");
                case AggOp.Init_ordinal:
                    aggImplementor.implementInitialize(call, register, this);
                    return setResult(call, register);
                case AggOp.Add_ordinal:
                    aggImplementor.implementAdd(call, register, this);
                    return setResult(call, register);
                case AggOp.Drop_ordinal:
                    aggImplementor.implementDrop(call, register, this);
                    return setResult(call, register);
                default:
                    throw aggOp.unexpected();
                }
            }
        }

        throw FarragoResource.instance().ProgramImplementationError.ex(
            "Unknown operator " + op);
    }

    private boolean isStrCmp(RexCall call)
    {
        SqlOperator op = call.getOperator();
        if (op.getKind().isA(SqlKind.Equals)
            || op.getKind().isA(SqlKind.NotEquals)
            || op.getKind().isA(SqlKind.GreaterThan)
            || op.getKind().isA(SqlKind.LessThan)
            || op.getKind().isA(SqlKind.GreaterThanOrEqual)
            || op.getKind().isA(SqlKind.LessThanOrEqual)) {
            RelDataType t0 = call.operands[0].getType();
            RelDataType t1 = call.operands[1].getType();

            return
                (SqlTypeUtil.inCharFamily(t0) && SqlTypeUtil.inCharFamily(t1))
                || (isOctetString(t0) && isOctetString(t1));
        }
        return false;
    }

    private static boolean isOctetString(RelDataType t)
    {
        switch (t.getSqlTypeName().getOrdinal()) {
        case SqlTypeName.Varbinary_ordinal:
        case SqlTypeName.Binary_ordinal:
            return true;
        }
        return false;
    }

    /**
     * If conversion is needed between the two operands this function inserts a
     * call to the calculator convert function and silently updates the result
     * register of the operands as needed. The new updated registers are
     * returned in the regs array.
     *
     * @param op1 first operand
     * @param op2 second operand
     * @param regs at the time of calling this methods this array must contain
     * the result registers of op1 and op2. The new updated registers are
     * returned in the regs array.
     * @param keepVartypes - indicates when choosing between VARCHAR and CHAR,
     * to convert to VARCHAR or CHAR. If true, converts to VARCHAR.
     */
    void implementConversionIfNeeded(
        RexNode op1,
        RexNode op2,
        CalcReg [] regs,
        boolean keepVartypes)
    {
        if (SqlTypeUtil.inCharFamily(op1.getType())
            && (
                op1.getType().getSqlTypeName() != op2.getType()
                .getSqlTypeName()
               )) {
            // Need to perform a cast.
            CalcReg newReg;
            SqlTypeName replaceType =
                (keepVartypes) ? SqlTypeName.Char : SqlTypeName.Varchar;
            if (op1.getType().getSqlTypeName() == replaceType) {
                // cast op1 to op2's type but use op1's precision
                CalcProgramBuilder.RegisterDescriptor reg1Desc =
                    getCalcRegisterDescriptor(op1.getType());
                CalcProgramBuilder.RegisterDescriptor reg2Desc =
                    getCalcRegisterDescriptor(op2.getType());
                newReg =
                    builder.newLocal(
                        reg2Desc.getType(),
                        reg1Desc.getBytes());

                ExtInstructionDefTable.castA.add(
                    builder,
                    new CalcReg[] { newReg, regs[0] });

                regs[0] = newReg;
            } else {
                // cast op2 to op1's type but use op2's precision
                CalcProgramBuilder.RegisterDescriptor reg1Desc =
                    getCalcRegisterDescriptor(op1.getType());
                CalcProgramBuilder.RegisterDescriptor reg2Desc =
                    getCalcRegisterDescriptor(op2.getType());
                newReg =
                    builder.newLocal(
                        reg1Desc.getType(),
                        reg2Desc.getBytes());

                ExtInstructionDefTable.castA.add(
                    builder,
                    new CalcReg[] { newReg, regs[1] });

                regs[1] = newReg;
            }
        }

        // REVIEW: SZ: 8/11/2004: Something similar to the above
        // probably needs to be done for BINARY vs. VARBINARY.
    }

    private CalcReg implement(RexLiteral node)
    {
        if (containsResult(node)) {
            throw new AssertionError(
                "Shouldn't call this function directly;"
                + " use implementNode(RexNode) instead");
        }

        Object value = node.getValue2();
        CalcProgramBuilder.RegisterDescriptor desc =
            getCalcRegisterDescriptor(node);
        final CalcReg register =
            builder.newLiteral(desc, value);
        setResult(node, register);
        return register;
    }

    private CalcReg implement(RexInputRef node)
    {
        CalcReg register = scope.get(getKey(node));
        if (register != null) {
            return register; // this method is idempotent
        }
        final int index = node.getIndex();
        assert index < program.getInputRowType().getFields().length;
        register = builder.newInput(getCalcRegisterDescriptor(node));
        setResult(node, register);
        return register;
    }

    private CalcReg implement(RexLocalRef node)
    {
        assert !containsResult(node) : "Shouldn't call this function directly;"
            + " use implementNode(RexNode) instead";

        // Evaluate the expression and assign to a new result.
        final int index = node.getIndex();
        final RexNode expr = program.getExprList().get(index);
        expr.accept(this);
        final CalcReg result = getResult(expr, true);
        setResult(node, result);
        return result;
    }

    private CalcReg implement(RexFieldAccess node)
    {
        if (containsResult(node)) {
            throw new AssertionError(
                "Shouldn't call this function directly;"
                + " use implementNode(RexNode) instead");
        }

        final RexCorrelVariable accessedNode =
            (RexCorrelVariable) node.getReferenceExpr();
        return implement(accessedNode);
    }

    private CalcReg implement(RexCorrelVariable node)
    {
        final int id = RelOptQuery.getCorrelOrdinal(node.getName());
        CalcReg idReg = builder.newInt4Literal(id);
        CalcReg result =
            builder.newLocal(getCalcRegisterDescriptor(node));
        ExtInstructionDefTable.dynamicVariable.add(
            builder,
            new CalcReg[] { result, idReg });
        setResult(node, result);
        return result;
    }

    /**
     * @param generateShortCircuit If true, tells the code generator to short
     * circuit logical operators<br>
     * The default valude is <emp>false</emp>
     */
    public void setGenerateShortCircuit(boolean generateShortCircuit)
    {
        this.generateShortCircuit = generateShortCircuit;
    }

    public void setGenerateComments(boolean outputComments)
    {
        builder.setOutputComments(outputComments);
    }

    /**
     * Generates the three programs -- init, add, and drop -- for an array of
     * calls to aggregate functions.
     *
     * <p>The aggregate expressions are represented by two {@link RexProgram}
     * objects. The lower <code>inputProgram</code> computes the input
     * expressions to the calculator; the upper <code>aggProgram</code> contains
     * calls to aggregate functions. For example, the expressions
     *
     * <pre>Aggs = {
     *    SUM(a + b),
     *    SUM(a + b + c),
     *    COUNT(a + b) * d
     *    SUM(a + b) + 4
     * }</pre>
     *
     * would be represented by the program
     *
     * <pre>aggProgram = {
     *    exprs = {
     *       $0,         // a
     *       $1,         // b
     *       $2,         // c
     *       $3,         // d
     *       $0 + $1,    // a + b
     *       $4 + $2     // (a + b) + c
     *       SUM($4),    // SUM(a + b)
     *       SUM($5),    // SUM(a + b + c)
     *       COUNT($4),  // COUNT(a + b)
     *       $8 * $3     // COUNT(a + b) * d
     *       SUM($4),    // SUM(a + b)
     *       $10 + 4,    // SUM(a + b) + 4
     *    },
     *    projectRefs = {
     *       $6,         // SUM(a + b)
     *       $7,         // SUM(a + b + c)
     *       $9,         // COUNT(a + b) * d
     *       $11,        // SUM(a + b) + 4
     *    },
     *    conditionRef = null
     * }</pre>
     *
     * @param program Aggregate expressions to calculate. The program can
     * compute the inputs to the aggregates, the aggregates themselves, output
     * expressions, and an output filter.
     * @param programs Output array of programs.
     *
     * @pre programs.length == 3
     */
    public void getAggProgram(
        final RexProgram program,
        String [] programs)
    {
        Util.pre(programs.length == 3, "programs.length == 3");
        programs[0] = getAggProgram(program, AggOp.Init);
        programs[1] = getAggProgram(program, AggOp.Add);
        programs[2] = getAggProgram(program, AggOp.Drop);
    }

    public RexLiteral getLiteral(RexNode expr)
    {
        expr = resolve(expr);
        assert expr instanceof RexLiteral : expr;
        return (RexLiteral) expr;
    }

    public RexNode resolve(RexNode expr)
    {
        if (expr instanceof RexLocalRef) {
            final int index = ((RexLocalRef) expr).getIndex();
            expr = program.getExprList().get(index);
        }
        return expr;
    }

    /**
     * Creates a new expression scope
     */
    public void newScope()
    {
        scope = scope.newScope();
    }

    /**
     * Pops the previous expression scope
     */
    public void popScope()
    {
        scope = scope.popScope();
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Supports scoping rules for calculator programs. Conditional code, not
     * visible outside a block of code, belongs in its own scope.
     */
    public class ExpressionScope
    {
        private final ExpressionScope parent;
        private final Map<String, CalcReg> results =
            new HashMap<String, CalcReg>();

        private ExpressionScope(ExpressionScope parent)
        {
            this.parent = parent;
        }

        /**
         * Constructs a new scope
         */
        public ExpressionScope()
        {
            parent = null;
        }

        /**
         * Returns a new scope extending the current scope. Must always be
         * matched with popScope or mysterious errors will arise.
         */
        public ExpressionScope newScope()
        {
            return new ExpressionScope(this);
        }

        /**
         * Returns the parent scope
         */
        public ExpressionScope popScope()
        {
            assert (parent != null) : "attempted to pop global scope";
            return parent;
        }

        public CalcReg get(String key)
        {
            CalcReg result = results.get(key);
            if ((result == null) && (parent != null)) {
                result = parent.get(key);
            }
            return result;
        }

        public void set(String key, CalcReg result)
        {
            results.put(key, result);
        }

        public void clear()
        {
            results.clear();
        }
    }

    private static class TypePair
    {
        private final RelDataType relDataType;
        private final CalcProgramBuilder.OpType opType;

        TypePair(
            RelDataType saffron,
            CalcProgramBuilder.OpType op)
        {
            relDataType = saffron;
            opType = op;
        }
    }

    /**
     * Trivial exception thrown when {@link TranslationTester} finds a node it
     * cannot translate.
     */
    private static class TranslationException
        extends RuntimeException
    {
    }

    /**
     * Visitor which walks over a {@link RexNode row expression} and throws
     * {@link TranslationException} if it finds a node which cannot be
     * implemented.
     */
    private class TranslationTester
        extends RexVisitorImpl<Void>
    {
        private final RexToCalcTranslator translator;

        /**
         * Creates a TranslationTester.
         *
         * @param translator Translator, which provides a table mapping
         * operators to implementations.
         * @param deep If true, tests whether all of the child expressions can
         * be implemented
         */
        TranslationTester(
            RexToCalcTranslator translator,
            boolean deep)
        {
            super(deep);
            this.translator = translator;
        }

        public Void visitCall(RexCall call)
        {
            final SqlOperator op = call.getOperator();
            CalcRexImplementor implementor =
                translator.implementorTable.get(op);
            if ((implementor == null) || !implementor.canImplement(call)) {
                throw new TranslationException();
            }

            return super.visitCall(call);
        }

        public Void visitOver(RexOver over)
        {
            // Matches RexToCalcTranslator.visitOver()
            throw new RexToCalcTranslator.TranslationException();
        }

        public Void visitDynamicParam(RexDynamicParam dynamicParam)
        {
            // Matches RexToCalcTranslator.visitDynamicParam()
            throw new RexToCalcTranslator.TranslationException();
        }

        public Void visitRangeRef(RexRangeRef rangeRef)
        {
            // Matches RexToCalcTranslator.visitRangeRef()
            throw new RexToCalcTranslator.TranslationException();
        }
    }

    /**
     * Enumeration of aggregate operations.
     */
    public static class AggOp
        extends EnumeratedValues.BasicValue
    {
        private static final int None_ordinal = 0;
        public static final AggOp None = new AggOp("None", None_ordinal);
        private static final int Init_ordinal = 1;
        public static final AggOp Init = new AggOp("Init", Init_ordinal);
        private static final int Add_ordinal = 2;
        public static final AggOp Add = new AggOp("Add", Add_ordinal);
        private static final int Drop_ordinal = 3;
        public static final AggOp Drop = new AggOp("Drop", Drop_ordinal);

        private AggOp(String name, int ordinal)
        {
            super(name, ordinal, null);
        }
    }
}

// End RexToCalcTranslator.java
