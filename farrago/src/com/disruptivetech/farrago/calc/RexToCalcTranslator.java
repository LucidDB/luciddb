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

import java.util.HashMap;
import java.util.List;

import net.sf.farrago.resource.*;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeFactoryImpl;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.SaffronProperties;
import org.eigenbase.util.Util;
import org.eigenbase.relopt.RelOptQuery;
import org.eigenbase.rel.CorrelatorRel;


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

    protected final RexNode [] projectExps;
    protected final RexNode conditionExp;
    final CalcProgramBuilder builder = new CalcProgramBuilder();
    final CalcProgramBuilder.Register trueReg = builder.newBoolLiteral(true);
    final CalcProgramBuilder.Register falseReg = builder.newBoolLiteral(false);
    final SqlStdOperatorTable opTab = SqlStdOperatorTable.instance();
    final CalcRexImplementorTable implementorTable =
        CalcRexImplementorTableImpl.threadInstance();

    /**
     * Maps all results from every result returnable RexNode to a register.
     *
     * <p>Key: {@link RexNode};
     * value: {@link CalcProgramBuilder.Register}.
     * See {@link #getKey} for computing the key
     **/
    private HashMap results = new HashMap();
    protected final RexBuilder rexBuilder;
    /**
     * Tells the code generator to short circuit logical operators.<br>
     * The default valude is <emp>false</emp>.
     */
    protected boolean generateShortCircuit = false;
    protected int labelOrdinal = 0;

    /**
     * Ordered mapping from Saffron types (representing a family of types)
     * to the corresponding calculator type. The ordering ensures
     * determinacy.
     */
    private final TypePair [] knownTypes;

    //~ Constructors ----------------------------------------------------------

    public RexToCalcTranslator(
        RexBuilder rexBuilder,
        RexNode [] projectExps,
        RexNode conditionExp)
    {
        this.rexBuilder = rexBuilder;
        this.projectExps = projectExps;
        this.conditionExp = conditionExp;

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
     * Gets the result in form of a reference to register for the given rex node.
     * If node hasn't been implmented no result will be found and this function
     * asserts.
     * Therefore don't use this function to check if result exists,
     * see {@link #containsResult} for that.
     * @param node
     * @return
     */
    CalcProgramBuilder.Register getResult(RexNode node)
    {
        CalcProgramBuilder.Register found =
            (CalcProgramBuilder.Register) results.get(getKey(node));
        assert (null != found);
        return found;
    }

    boolean containsResult(RexNode node)
    {
        return results.get(getKey(node)) != null;
    }

    /**
     * Translates a RexNode contained in a FilterRel into a
     * {@link CalcProgramBuilder} calculator program
     * using a depth-first recursive algorithm.
     *
     * @param inputRowType The type of the input row to the calculator.
     *   If <code>inputRowType</code> is not null, the program contains
     *   an input register for every field in the input row type; otherwise
     *   it contains inputs for only those fields used.
     */
    public String getProgram(RelDataType inputRowType)
    {
        // Step 0. Create input fields.
        // Create a calculator input for each field in the input relation,
        // regardless of whether the calcualtor program uses them.
        if (inputRowType != null) {
            final RelDataTypeField [] fields = inputRowType.getFields();
            for (int i = 0; i < fields.length; i++) {
                RexInputRef rexInputRef =
                    new RexInputRef(i,
                        fields[i].getType());
                implementNode(rexInputRef);
            }
        }

        //step 1: implement all the filtering logic
        if (conditionExp != null) {
            CalcProgramBuilder.Register filterResult =
                implementNode(conditionExp);
            assert CalcProgramBuilder.OpType.Bool == filterResult.getOpType() : "Condition must be boolean: "
            + conditionExp;

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
                CalcProgramBuilder.Raise.add(
                    builder,
                    builder.newVarcharLiteral("22004", 5));
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

    public void visitCorrelVariable(RexCorrelVariable correlVariable)
    {
        implementNode(correlVariable);
    }

    public void visitDynamicParam(RexDynamicParam dynamicParam)
    {
        throw FarragoResource.instance().newProgramImplementationError("Don't know how to implement rex node="
            + dynamicParam);
    }

    public void visitRangeRef(RexRangeRef rangeRef)
    {
        throw FarragoResource.instance().newProgramImplementationError("Don't know how to implement rex node="
            + rangeRef);
    }

    public void visitFieldAccess(RexFieldAccess fieldAccess)
    {
        final RexNode expr = fieldAccess.getReferenceExpr();
        if (expr instanceof RexCorrelVariable) {
            implementNode(fieldAccess);
            return;
        }
        throw FarragoResource.instance().newProgramImplementationError("Don't know how to implement rex node="
            + fieldAccess);
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

        throw FarragoResource.instance().newProgramImplementationError("Unknown operator "
            + op);
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
    private class TranslationTester implements RexVisitor
    {
        private final RexToCalcTranslator translator;
        private final boolean deep;

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
            this.translator = translator;
            this.deep = deep;
        }

        public void visitInputRef(RexInputRef inputRef)
        {
        }

        public void visitLiteral(RexLiteral literal)
        {
        }

        public void visitCall(RexCall call)
        {
            final SqlOperator op = call.op;
            CalcRexImplementor implementor =
                translator.implementorTable.get(op);
            if ((implementor == null) || !implementor.canImplement(call)) {
                throw new TranslationException();
            }

            if (!deep) {
                return;
            }

            final RexNode [] operands = call.getOperands();
            for (int i = 0; i < operands.length; i++) {
                RexNode operand = operands[i];
                operand.accept(this);
            }
        }

        public void visitCorrelVariable(RexCorrelVariable correlVariable)
        {
        }

        public void visitDynamicParam(RexDynamicParam dynamicParam)
        {
            // Matches RexToCalcTranslator.visitDynamicParam()
            throw new TranslationException();
        }

        public void visitRangeRef(RexRangeRef rangeRef)
        {
            // Matches RexToCalcTranslator.visitRangeRef()
            throw new TranslationException();
        }

        public void visitFieldAccess(RexFieldAccess fieldAccess)
        {
            if (!deep) {
                return;
            }
            final RexNode expr = fieldAccess.getReferenceExpr();
            expr.accept(this);
        }
    }
}


// End RexToCalcTranslator.java
