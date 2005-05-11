/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
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

import net.sf.farrago.resource.FarragoResource;
import org.eigenbase.relopt.RelOptQuery;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.type.SqlTypeUtil;
import org.eigenbase.util.EnumeratedValues;
import org.eigenbase.util.SaffronProperties;
import org.eigenbase.util.Util;

import java.util.HashMap;
import java.util.Map;


/**
 * Converts expressions in logical format ({@link RexNode}) into calculator
 * assembly-language programs.
 *
 * @see CalcProgramBuilder
 *
 * @author Wael Chatila
 * @since Feb 5, 2004
 * @version $Id$
 */
public class RexToCalcTranslator implements RexVisitor
{
    //~ Instance fields -------------------------------------------------------

    // The following 3 fields comprise the program; they are reset each time a
    // new program is started.
    final CalcProgramBuilder builder = new CalcProgramBuilder();
    CalcProgramBuilder.Register trueReg = builder.newBoolLiteral(true);
    CalcProgramBuilder.Register falseReg = builder.newBoolLiteral(false);

    final SqlStdOperatorTable opTab = SqlStdOperatorTable.instance();
    protected final CalcRexImplementorTable implementorTable =
        CalcRexImplementorTableImpl.threadInstance();

    /**
     * Eliminates common-subexpressions, by mapping every expression (the key
     * is generated from a {@link RexNode} using {@link #getKey(RexNode)}) to
     * the {@link CalcProgramBuilder.Register} which holds its value.
     */
    private final Map results = new HashMap();
    protected final RexBuilder rexBuilder;
    /**
     * Whether the code generator should short-circuit logical operators.
     * The default value is <em>false</em>.
     */
    protected boolean generateShortCircuit = false;
    protected int labelOrdinal = 0;

    /**
     * Ordered mapping from Saffron types (representing a family of types)
     * to the corresponding calculator type. The ordering ensures
     * determinacy.
     */
    private final TypePair [] knownTypes;
    private AggOp aggOp;

    //~ Constructors ----------------------------------------------------------

    public RexToCalcTranslator(
        RexBuilder rexBuilder)
    {
        this.rexBuilder = rexBuilder;

        setGenerateComments(
            SaffronProperties.instance().generateCalcProgramComments.get());
        RelDataTypeFactory fac = this.rexBuilder.getTypeFactory();
        knownTypes =
            new TypePair [] {
                new TypePair(
                    fac.createSqlType(SqlTypeName.Tinyint),
                    CalcProgramBuilder.OpType.Int1),
                new TypePair(
                    fac.createSqlType(SqlTypeName.Smallint),
                    CalcProgramBuilder.OpType.Int2),
                new TypePair(
                    fac.createSqlType(SqlTypeName.Integer),
                    CalcProgramBuilder.OpType.Int4),
                new TypePair(
                    fac.createSqlType(SqlTypeName.Bigint),
                    CalcProgramBuilder.OpType.Int8),
                new TypePair(
                    fac.createSqlType(SqlTypeName.Float),
                    CalcProgramBuilder.OpType.Double),
                new TypePair(
                    fac.createSqlType(SqlTypeName.Double),
                    CalcProgramBuilder.OpType.Double),
                new TypePair(
                    fac.createSqlType(SqlTypeName.Real),
                    CalcProgramBuilder.OpType.Real),
                new TypePair(
                    fac.createSqlType(SqlTypeName.Varbinary, 0),
                    CalcProgramBuilder.OpType.Varbinary),
                new TypePair(
                    fac.createSqlType(SqlTypeName.Varchar, 0),
                    CalcProgramBuilder.OpType.Varchar),
                new TypePair(
                    fac.createSqlType(SqlTypeName.Boolean),
                    CalcProgramBuilder.OpType.Bool),


                // FIXME: not right for T/w TZ.
                new TypePair(
                    fac.createSqlType(SqlTypeName.Date),
                    CalcProgramBuilder.OpType.Int8),
                new TypePair(
                    fac.createSqlType(SqlTypeName.Time),
                    CalcProgramBuilder.OpType.Int8),
                new TypePair(
                    fac.createSqlType(SqlTypeName.Timestamp),
                    CalcProgramBuilder.OpType.Int8),

                new TypePair(
                    fac.createJavaType(Byte.class),
                    CalcProgramBuilder.OpType.Int1),
                new TypePair(
                    fac.createJavaType(byte.class),
                    CalcProgramBuilder.OpType.Int1),
                new TypePair(
                    fac.createJavaType(Short.class),
                    CalcProgramBuilder.OpType.Int2),
                new TypePair(
                    fac.createJavaType(short.class),
                    CalcProgramBuilder.OpType.Int2),
                new TypePair(
                    fac.createJavaType(Integer.class),
                    CalcProgramBuilder.OpType.Int4),
                new TypePair(
                    fac.createJavaType(int.class),
                    CalcProgramBuilder.OpType.Int4),
                new TypePair(
                    fac.createJavaType(Long.class),
                    CalcProgramBuilder.OpType.Int8),
                new TypePair(
                    fac.createJavaType(long.class),
                    CalcProgramBuilder.OpType.Int8),
                new TypePair(
                    fac.createJavaType(Double.class),
                    CalcProgramBuilder.OpType.Double),
                new TypePair(
                    fac.createJavaType(double.class),
                    CalcProgramBuilder.OpType.Double),
                new TypePair(
                    fac.createJavaType(Float.class),
                    CalcProgramBuilder.OpType.Real),
                new TypePair(
                    fac.createJavaType(float.class),
                    CalcProgramBuilder.OpType.Real),
                new TypePair(
                    fac.createJavaType(String.class),
                    CalcProgramBuilder.OpType.Varchar),
                new TypePair(
                    fac.createJavaType(Boolean.class),
                    CalcProgramBuilder.OpType.Bool),
                new TypePair(
                    fac.createJavaType(boolean.class),
                    CalcProgramBuilder.OpType.Bool),
            };
    }

    private void clearProgram()
    {
        builder.clear();
        trueReg = builder.newBoolLiteral(true);
        falseReg = builder.newBoolLiteral(false);
        results.clear();
    }

    //~ Methods ---------------------------------------------------------------

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
     * Returns whether every expression in a list, and an optional expression,
     * can be translated.
     */
    public boolean canTranslate(RexNode[] exprs, RexNode expr)
    {
        boolean deep = true;
        final TranslationTester tester = new TranslationTester(this, deep);
        try {
            for (int i = 0; i < exprs.length; i++) {
                exprs[i].accept(tester);
            }
            if (expr != null) {
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
             return new CalcProgramBuilder.RegisterDescriptor(
                 CalcProgramBuilder.OpType.Varbinary, 4096);
        }
        for (int i = 0; i < knownTypes.length; i++) {
            TypePair knownType = knownTypes[i];
            if (SqlTypeUtil.sameNamedType(relDataType, knownType.relDataType)) {
                calcType = knownType.opType;
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

    private Object getKey(RexNode node)
    {
        if (node instanceof RexInputRef) {
            return ((RexInputRef) node).getName();
        }
        return node.toString() + node.getType().toString();
    }

    void setResult(
        RexNode node,
        CalcProgramBuilder.Register register)
    {
        results.put(
            getKey(node),
            register);
    }

    /**
     * Returns the register which contains the result of evaluating the given
     * expression. The node must already have been implemented.
     *
     * <p>To check whether the result exists, use
     * {@link #containsResult(RexNode)}.
     */
    CalcProgramBuilder.Register getResult(RexNode node)
    {
        CalcProgramBuilder.Register found =
            (CalcProgramBuilder.Register) results.get(getKey(node));
        if (found == null) {
            throw Util.newInternal("Expression " + node +
                " has not been implemented as a register");
        }
        return found;
    }

    /**
     * Returns whether a given expression has been implemented as a register.
     *
     * @see #getResult(RexNode)
     */
    boolean containsResult(RexNode node)
    {
        return results.get(getKey(node)) != null;
    }

    /**
     * Translates an array of project expressions and an optional filter
     * expression into a {@link CalcProgramBuilder} calculator program
     * using a depth-first recursive algorithm.
     *
     * <p>This method is NOT stateless.
     * TODO: Make method stateless -- so you can call this method several times
     * with different inputs -- and therefore the translator is re-usable.
     *
     * @param inputRowType The type of the input row to the calculator.
     *   If <code>inputRowType</code> is not null, the program contains
     *   an input register for every field in the input row type; otherwise
     *   it contains inputs for only those fields used.
     * @param projectExps Array of expressions to be projected. Must not be
     *   null, may be empty.
     * @param conditionExp Filter expression. May be null.
     */
    public String getProgram(
        RelDataType inputRowType,
        RexNode [] projectExps,
        RexNode conditionExp)
    {
        return getProgram(inputRowType, projectExps, conditionExp, AggOp.None);
    }

    private String getProgram(
        RelDataType inputRowType,
        final RexNode[] projectExps,
        RexNode conditionExp,
        AggOp aggOp)
    {
        clearProgram();

        // Stop -1. Figure out the input row type.
        this.aggOp = aggOp;
        switch (aggOp.ordinal) {
        case AggOp.None_ordinal:
        case AggOp.Init_ordinal:
            break;
        case AggOp.Add_ordinal:
        case AggOp.Drop_ordinal:
            inputRowType =
                rexBuilder.getTypeFactory().createStructType(
                    new RelDataTypeFactory.FieldInfo()
                    {
                        public int getFieldCount()
                        {
                            return projectExps.length;
                        }

                        public String getFieldName(int index)
                        {
                            return "input$" + index;
                        }

                        public RelDataType getFieldType(int index)
                        {
                            return projectExps[index].getType();
                        }
                    });
            break;
        default:
            throw aggOp.unexpected();
        }
        // Step 0. Create input fields.
        // Create a calculator input for each field in the input relation,
        // regardless of whether the calcualtor program uses them.
        if (inputRowType != null) {
            final RexInputRef[] inputRefs = RexUtil.toInputRefs(inputRowType);
            for (int i = 0; i < inputRefs.length; i++) {
                RexInputRef inputRef = inputRefs[i];
                implementNode(inputRef);
            }
        }

        // Step 1: implement all the filtering logic
        if (conditionExp != null) {
            CalcProgramBuilder.Register filterResult =
                implementNode(conditionExp);
            assert CalcProgramBuilder.OpType.Bool == filterResult.getOpType() :
                "Condition must be boolean: " + conditionExp;

            //step 2: report the status of the filtering
            CalcProgramBuilder.Register statusReg =
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
        for (int i = 0; i < projectExps.length; i++) {
            RexNode node = projectExps[i];

            // This could probably be optimized by writing to the outputs
            // directly instead of temp registers but harder to (java)
            // implement.
            implementNode(node);
        }

        // all outputs calculated, now assign results to outputs by reference
        CalcProgramBuilder.Register isNullRes = null;
        for (int i = 0; i < projectExps.length; i++) {
            RexNode node = projectExps[i];
            CalcProgramBuilder.RegisterDescriptor desc =
                getCalcRegisterDescriptor(node);
            CalcProgramBuilder.Register res = getResult(node);

            // check and assert that if type was declared as NOT NULLABLE, value
            // is not null
            if (!node.getType().isNullable()) {
                String wasNotNull = newLabel();
                if (isNullRes == null) {
                    isNullRes =
                        builder.newLocal(CalcProgramBuilder.OpType.Bool, -1);
                }
                CalcProgramBuilder.boolNativeIsNull.add(builder, isNullRes,
                    res);
                builder.addLabelJumpFalse(wasNotNull, isNullRes);
                CalcProgramBuilder.raise.add(
                    builder,
                    builder.newVarcharLiteral(
                        SqlStateCodes.NullValueNotAllowed.getState(), 5));
                builder.addReturn();
                builder.addLabel(wasNotNull);
            }

            //if ok, assign result to output by reference
            CalcProgramBuilder.Register outReg = builder.newOutput(desc);
            builder.addRef(outReg, res);
        }

        builder.addReturn();
        return builder.getProgram();
    }

    /**
     * Adds instructions to implement an expression to the program,
     * and returns the register which returns the result.
     * If the expression has already been implemented, does not add more
     * instructions, just returns the existing register.
     *
     * @param node Expression
     * @return Register which holds the result of evaluating the expression,
     *   never null, and always equivalent to calling {@link #getResult}.
     * @post result != null
     * @post result == getResult(node)
     */
    CalcProgramBuilder.Register implementNode(RexNode node)
    {
        if (!containsResult(node)) {
            node.accept(this);
        }
        final CalcProgramBuilder.Register result = getResult(node);
        Util.pre(result != null, "result != null");
        return result;
    }

    public void visitInputRef(RexInputRef inputRef)
    {
        implementNode(inputRef);
    }

    public void visitLiteral(RexLiteral literal)
    {
        implementNode(literal);
    }

    public void visitCall(RexCall call)
    {
        int before = builder.getCurrentLineNumber();
        implementNode(call);
        int after = builder.getCurrentLineNumber();
        assert (after >= before);
        if (0 != (after - before)) {
            builder.addComment(call.toString());
        }
    }

    public void visitOver(RexOver over)
    {
        throw FarragoResource.instance().newProgramImplementationError(
            "Don't know how to implement rex node=" + over);
    }

    public void visitCorrelVariable(RexCorrelVariable correlVariable)
    {
        implementNode(correlVariable);
    }

    public void visitDynamicParam(RexDynamicParam dynamicParam)
    {
        throw FarragoResource.instance().newProgramImplementationError(
            "Don't know how to implement rex node=" + dynamicParam);
    }

    public void visitRangeRef(RexRangeRef rangeRef)
    {
        throw FarragoResource.instance().newProgramImplementationError(
            "Don't know how to implement rex node=" + rangeRef);
    }

    public void visitFieldAccess(RexFieldAccess fieldAccess)
    {
        final RexNode expr = fieldAccess.getReferenceExpr();
        if (expr instanceof RexCorrelVariable) {
            implementNode(fieldAccess);
            return;
        }
        throw FarragoResource.instance().newProgramImplementationError(
            "Don't know how to implement rex node=" + fieldAccess);
    }

    private void implementShortCircuit(RexCall call)
    {
        assert (call.operands.length == 2) : "not a binary operator";
        if (containsResult(call)) {
            throw new AssertionError("Shouldn't call this function directly;"
                + " use implementNode(RexNode) instead");
        }
        SqlOperator op = call.op;

        if (op.kind.isA(SqlKind.And) || (op.kind.isA(SqlKind.Or))) {
            //first operand of AND/OR
            CalcProgramBuilder.Register reg0 = implementNode(call.operands[0]);
            String shortCut = newLabel();

            //Check if we can make a short cut
            if (op.kind.isA(SqlKind.And)) {
                builder.addLabelJumpFalse(shortCut, reg0);
            } else {
                builder.addLabelJumpTrue(shortCut, reg0);
            }

            //second operand
            CalcProgramBuilder.Register reg1 = implementNode(call.operands[1]);
            CalcProgramBuilder.Register result =
                builder.newLocal(CalcProgramBuilder.OpType.Bool, -1);
            assert result.getOpType().getOrdinal() == getCalcRegisterDescriptor(call)
                .getType().getOrdinal();
            CalcProgramBuilder.move.add(builder, result, reg1);

            String restOfInstructions = newLabel();
            builder.addLabelJump(restOfInstructions);
            builder.addLabel(shortCut);

            if (op.kind.isA(SqlKind.And)) {
                CalcProgramBuilder.move.add(builder, result, falseReg);
            } else {
                CalcProgramBuilder.move.add(builder, result, trueReg);
            }

            setResult(call, result);

            //WARNING this assumes that more instructions will follow.
            //Return is currently always at the end.
            builder.addLabel(restOfInstructions);
        } else {
            throw FarragoResource.instance().newProgramImplementationError(
                op.toString());
        }
    }

    private void implementNode(RexCall call)
    {
        if (containsResult(call)) {
            throw new AssertionError("Shouldn't call this function directly;"
                + " use implementNode(RexNode) instead");
        }

        SqlOperator op = call.op;

        //check if and/or/xor should short circuit
        if (generateShortCircuit
                && (op.kind.isA(SqlKind.And) || op.kind.isA(SqlKind.Or)) /* ||
            op.kind.isA(SqlKind.Xor) */) {
            implementShortCircuit(call);
            return;
        }

        // Do table-driven implementation if possible.
        // TODO: Put ALL operator implementation code in this table, except
        //   perhaps for the most fundamental and idiosyncratic operators.
        CalcProgramBuilder.Register resultOfCall = null;
        CalcProgramBuilder.RegisterDescriptor resultDesc =
            getCalcRegisterDescriptor(call);

        /** SPECIAL CASE.  =,<>,>,<,>-=,<= already are defined in
         * {@link CalcRexImplementorTableImpl}
         * but need to do some acrobatics since those calc instructions are not defined
         * against varchars
         */
        if (isStrCmp(call)) {
            CalcProgramBuilder.Register reg1 = implementNode(call.operands[0]);
            CalcProgramBuilder.Register reg2 = implementNode(call.operands[1]);

            CalcProgramBuilder.Register [] convRegs = { reg1, reg2 };
            implementConversionIfNeeded(call.operands[0],
                call.operands[1], convRegs);
            reg1 = convRegs[0];
            reg2 = convRegs[1];

            resultOfCall = builder.newLocal(resultDesc);

            assert resultDesc.getType() == CalcProgramBuilder.OpType.Bool;
            CalcProgramBuilder.Register strCmpResult =
                builder.newLocal(CalcProgramBuilder.OpType.Int4, -1);
            if (SqlTypeUtil.inCharFamily(call.operands[0].getType())) {
                String collationToUse =
                    SqlCollation.getCoercibilityDyadicComparison(
                        call.operands[0].getType().getCollation(),
                        call.operands[1].getType().getCollation());
                CalcProgramBuilder.Register colReg =
                    builder.newVarcharLiteral(collationToUse);

                //TODO this is only for ascci cmp. Need to pump in colReg when a
                //fennel function that can take it is born
                ExtInstructionDefTable.strCmpA.add(
                    builder,
                    new CalcProgramBuilder.Register [] { strCmpResult, reg1, reg2 });
            } else {
                ExtInstructionDefTable.strCmpOct.add(
                    builder,
                    new CalcProgramBuilder.Register [] { strCmpResult, reg1, reg2 });
            }

            CalcProgramBuilder.Register zero = builder.newInt4Literal(0);
            CalcProgramBuilder.Register [] regs =
            { resultOfCall, strCmpResult, zero };
            if (op.kind.isA(SqlKind.Equals)) {
                CalcProgramBuilder.boolNativeEqual.add(builder, regs);
            } else if (op.kind.isA(SqlKind.NotEquals)) {
                CalcProgramBuilder.boolNativeNotEqual.add(builder, regs);
            } else if (op.kind.isA(SqlKind.GreaterThan)) {
                CalcProgramBuilder.boolNativeGreaterThan.add(builder, regs);
            } else if (op.kind.isA(SqlKind.LessThan)) {
                CalcProgramBuilder.boolNativeLessThan.add(builder, regs);
            } else if (op.kind.isA(SqlKind.GreaterThanOrEqual)) {
                CalcProgramBuilder.boolNativeGreaterOrEqualThan.add(builder,
                    regs);
            } else if (op.kind.isA(SqlKind.LessThanOrEqual)) {
                CalcProgramBuilder.boolNativeLessOrEqualThan.add(builder, regs);
            } else {
                throw Util.newInternal("Unknown op " + op);
            }
            setResult(call, resultOfCall);
            return;
        }

        // Ask implementor table for if op exists.
        // There are some special cases: see above and below.
        CalcRexImplementor implementor = implementorTable.get(op);
        if ((implementor != null) && implementor.canImplement(call)) {
            CalcProgramBuilder.Register reg =
                implementor.implement(call, this);
            setResult(call, reg);
            return;
        }

        // Maybe it's an aggregate function.
        if (op instanceof SqlAggFunction) {
            SqlAggFunction aggFun = (SqlAggFunction) op;
            CalcRexAggImplementor aggImplementor =
                implementorTable.getAgg(aggFun);
            if (aggImplementor != null) {
                // Create a local register to be the accumulator. It is the
                // output of the 'init' code, and both the input and the output
                // for the 'add' and 'drop' code.
                CalcProgramBuilder.Register register =
                    builder.newLocal(getCalcRegisterDescriptor(call));
                switch (aggOp.ordinal) {
                case AggOp.None_ordinal:
                    throw Util.newInternal(
                        "Cannot generate calc program: Aggregate call " +
                        call + " found in non-aggregating context");
                case AggOp.Init_ordinal:
                    aggImplementor.implementInitialize(call, register, this);
                    setResult(call, register);
                    return;
                case AggOp.Add_ordinal:
                    aggImplementor.implementAdd(call, register, this);
                    setResult(call, register);
                    return;
                case AggOp.Drop_ordinal:
                    aggImplementor.implementDrop(call, register, this);
                    setResult(call, register);
                    return;
                default:
                    throw aggOp.unexpected();
                }

            }
        }

        throw FarragoResource.instance()
            .newProgramImplementationError("Unknown operator " + op);
    }

    private boolean isStrCmp(RexCall call)
    {
        SqlOperator op = call.op;
        if (op.kind.isA(SqlKind.Equals) || op.kind.isA(SqlKind.NotEquals)
                || op.kind.isA(SqlKind.GreaterThan)
                || op.kind.isA(SqlKind.LessThan)
                || op.kind.isA(SqlKind.GreaterThanOrEqual)
                || op.kind.isA(SqlKind.LessThanOrEqual)) {
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
        switch (t.getSqlTypeName().ordinal) {
        case SqlTypeName.Varbinary_ordinal:
        case SqlTypeName.Binary_ordinal:
            return true;
        }
        return false;
    }

    /**
     * If conversion is needed between the two operands this function
     * inserts a call to the calculator convert function and silently
     * updates the result register of the operands as needed.
     * The new updated registers are returned in the regs array.
     * @param op1 first operand
     * @param op2 second operand
     * @param regs at the time of calling this methods this array must contain
     *        the result registers of op1 and op2.
     *        The new updated registers are returned in the regs array.
     *
     */
    void implementConversionIfNeeded(
        RexNode op1,
        RexNode op2,
        CalcProgramBuilder.Register [] regs)
    {
        if (SqlTypeUtil.inCharFamily(op1.getType())
                && (op1.getType().getSqlTypeName() != op2.getType()
                .getSqlTypeName())) {
            // Need to perform a cast.
            CalcProgramBuilder.Register newReg;
            if (op1.getType().getSqlTypeName() == SqlTypeName.Varchar) {
                // cast op1 to op2's type but use op1's precision
                CalcProgramBuilder.RegisterDescriptor reg1Desc =
                    getCalcRegisterDescriptor(op1.getType());
                CalcProgramBuilder.RegisterDescriptor reg2Desc =
                    getCalcRegisterDescriptor(op2.getType());
                newReg =
                    builder.newLocal(reg2Desc.getType(), reg1Desc.getBytes());

                ExtInstructionDefTable.castA.add(
                    builder,
                    new CalcProgramBuilder.Register [] { newReg, regs[0] });

                regs[0] = newReg;
            } else {
                // cast op2 to op1's type but use op2's precision
                CalcProgramBuilder.RegisterDescriptor reg1Desc =
                    getCalcRegisterDescriptor(op1.getType());
                CalcProgramBuilder.RegisterDescriptor reg2Desc =
                    getCalcRegisterDescriptor(op2.getType());
                newReg =
                    builder.newLocal(reg1Desc.getType(), reg2Desc.getBytes());

                ExtInstructionDefTable.castA.add(
                    builder,
                    new CalcProgramBuilder.Register [] { newReg, regs[1] });

                regs[1] = newReg;
            }
        }

        // REVIEW: SZ: 8/11/2004: Something similar to the above
        // probably needs to be done for BINARY vs. VARBINARY.
    }

    private void implementNode(RexLiteral node)
    {
        if (containsResult(node)) {
            throw new AssertionError("Shouldn't call this function directly;"
                + " use implementNode(RexNode) instead");
        }

        Object value = node.getValue2();
        CalcProgramBuilder.RegisterDescriptor desc =
            getCalcRegisterDescriptor(node);
        setResult(
            node,
            builder.newLiteral(desc, value));
    }

    private void implementNode(RexInputRef node)
    {
        if (containsResult(node)) {
            throw new AssertionError("Shouldn't call this function directly;"
                + " use implementNode(RexNode) instead");
        }
        setResult(
            node,
            builder.newInput(getCalcRegisterDescriptor(node)));
    }

    private void implementNode(RexFieldAccess node)
    {
        if (containsResult(node)) {
            throw new AssertionError("Shouldn't call this function directly;"
                + " use implementNode(RexFieldAccess) instead");
        }

        final RexNode accessedNode = node.getReferenceExpr();
        assert(accessedNode instanceof RexCorrelVariable);
        implementNode((RexCorrelVariable) accessedNode);
    }

    private void implementNode(RexCorrelVariable node)
    {
        final int id = RelOptQuery.getCorrelOrdinal(node.getName());
        CalcProgramBuilder.Register idReg = builder.newInt4Literal(id);
        CalcProgramBuilder.Register result =
            builder.newLocal(getCalcRegisterDescriptor(node));
        ExtInstructionDefTable.dynamicVariable.add(
            builder,
            new CalcProgramBuilder.Register [] { result, idReg});
        setResult(node, result);
    }


    /**
     * @param generateShortCircuit If true, tells the code generator
     * to short circuit logical operators<br>
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
     * @param inputType The type of the input record.
     * @param aggCalls Array of calls to aggregate functions. Each must be a
     *    call to a {@link SqlAggFunction}, and have precisely one argument
     *    of type {@link RexInputRef}.
     * @param programs Output array of programs.
     *
     * @pre programs.length == 3
     * @pre aggCalls[i].op instanceof SqlAggFunction
     * @pre aggCalls[i].operands[j] instanceof RexInputRef
     */
    public void getAggProgram(
        RelDataType inputType,
        final RexCall[] aggCalls,
        String[] programs)
    {
        Util.pre(programs.length == 3, "programs.length == 3");
        for (int i = 0; i < aggCalls.length; i++) {
            RexCall aggCall = aggCalls[i];
            Util.pre(aggCall.op instanceof SqlAggFunction,
                "aggCalls[i].op instanceof SqlAggFunction");
            for (int j = 0; j < aggCall.operands.length; j++) {
                RexNode operand = aggCall.operands[j];
                Util.pre(operand instanceof RexInputRef,
                    "aggCalls[i].operands[j] instanceof RexInputRef");
            }
        }
        programs[0] = getProgram(inputType, aggCalls, null, AggOp.Init);
        programs[1] = getProgram(inputType, aggCalls, null, AggOp.Add);
        programs[2] = getProgram(inputType, aggCalls, null, AggOp.Drop);
    }

    //~ Inner Classes ---------------------------------------------------------

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
     * Trivial exception thrown when {@link TranslationTester} finds a node
     * it cannot translate.
     */
    private static class TranslationException extends RuntimeException
    {
    }

    /**
     * Visitor which walks over a {@link RexNode row expression} and throws
     * {@link TranslationException} if it finds a node which cannot be
     * implemented.
     */
    private class TranslationTester extends RexVisitorImpl
    {
        private final RexToCalcTranslator translator;

        /**
         * Creates a TranslationTester.
         *
         * @param translator Translator, which provides a table mapping
         *   operators to implementations.
         * @param deep If true, tests whether all of the child expressions can
         *   be implemented
         */
        TranslationTester(
            RexToCalcTranslator translator,
            boolean deep)
        {
            super(deep);
            this.translator = translator;
        }

        public void visitCall(RexCall call)
        {
            final SqlOperator op = call.op;
            CalcRexImplementor implementor =
                translator.implementorTable.get(op);
            if ((implementor == null) || !implementor.canImplement(call)) {
                throw new TranslationException();
            }

            super.visitCall(call);
        }

        public void visitOver(RexOver over)
        {
            // Matches RexToCalcTranslator.visitOver()
            throw new RexToCalcTranslator.TranslationException();
        }

        public void visitDynamicParam(RexDynamicParam dynamicParam)
        {
            // Matches RexToCalcTranslator.visitDynamicParam()
            throw new RexToCalcTranslator.TranslationException();
        }

        public void visitRangeRef(RexRangeRef rangeRef)
        {
            // Matches RexToCalcTranslator.visitRangeRef()
            throw new RexToCalcTranslator.TranslationException();
        }
    }

    private static class AggOp extends EnumeratedValues.BasicValue
    {
        private AggOp(String name, int ordinal)
        {
            super(name, ordinal, null);
        }

        private static final int None_ordinal = 0;
        public static final AggOp None = new AggOp("None", None_ordinal);
        private static final int Init_ordinal = 1;
        public static final AggOp Init = new AggOp("Init", Init_ordinal);
        private static final int Add_ordinal = 2;
        public static final AggOp Add = new AggOp("Add", Add_ordinal);
        private static final int Drop_ordinal = 3;
        public static final AggOp Drop = new AggOp("Drop", Drop_ordinal);
    }
}


// End RexToCalcTranslator.java
