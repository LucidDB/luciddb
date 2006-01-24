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

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.*;
import org.eigenbase.sql.SqlAggFunction;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.SqlStateCodes;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.fun.SqlTrimFunction;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.type.SqlTypeUtil;
import org.eigenbase.util.DoubleKeyMap;
import org.eigenbase.util.Util;
import org.eigenbase.util14.*;

import java.math.*;
import java.util.*;


/**
 * Implementation of {@link CalcRexImplementorTable}, containing
 * implementations for all standard functions.
 *
 * @author jhyde
 * @since June 2nd, 2004
 * @version $Id$
 */
public class CalcRexImplementorTableImpl implements CalcRexImplementorTable
{
    //~ Static fields/initializers --------------------------------------------

    protected static final SqlStdOperatorTable opTab =
        SqlStdOperatorTable.instance();
    private static final CalcRexImplementorTableImpl std =
        new CalcRexImplementorTableImpl(null).initStandard();
    private static final Integer integer0 = new Integer(0);
    private static final Integer integer1 = new Integer(1);

    /**
     * Collection of per-thread instances of {@link CalcRexImplementorTable}.
     * If a thread does not call {@link #setThreadInstance}, the default is
     * {@link #std}.
     */

    // REVIEW jhyde, 2004/4/12 The implementor table, like the operator table,
    //   should be a property of the session
    private static final ThreadLocal threadLocal =
        new ThreadLocal() {
            protected Object initialValue()
            {
                return std;
            }
        };


    //~ Instance fields -------------------------------------------------------

    /**
     * Parent implementor table, may be null.
     */
    private final CalcRexImplementorTable parent;

    /**
     * Maps {@link SqlOperator} to {@link CalcRexImplementor}.
     */
    private final Map operatorImplementationMap = new HashMap();

    /**
     * Maps {@link SqlAggFunction} to {@link CalcRexAggImplementor}.
     */
    private final Map aggImplementationMap = new HashMap();

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates an empty table which delegates to another table.
     *
     * @see {@link #std}
     */
    public CalcRexImplementorTableImpl(CalcRexImplementorTable parent)
    {
        this.parent = parent;
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Returns the table of implementations of all of the
     * standard SQL functions and operators.
     */
    public static CalcRexImplementorTable std()
    {
        return std;
    }

    /**
     * Registers an operator and its implementor.
     *
     * <p>It is an error if the operator already has an implementor.
     * But if the operator has an implementor in a parent table, it is
     * simply overridden.
     *
     * @pre op != null
     * @pre impl != null
     * @pre !operatorImplementationMap.containsKey(op)
     */
    public void register(
        SqlOperator op,
        CalcRexImplementor impl)
    {
        Util.pre(op != null, "op != null");
        Util.pre(impl != null, "impl != null");
        Util.pre(!operatorImplementationMap.containsKey(op),
            "!operatorImplementationMap.containsKey(op)");
        operatorImplementationMap.put(op, impl);
    }

    /**
     * Registers an operator which is implemented in a trivial way by a
     * single calculator instruction.
     *
     * @pre op != null
     * @pre instrDef != null
     */
    private void registerInstr(
        SqlOperator op,
        CalcProgramBuilder.InstructionDef instrDef)
    {
        Util.pre(instrDef != null, "instrDef != null");
        register(
            op,
            new InstrDefImplementor(instrDef));
    }

    /**
     * Registers an aggregate function and its implementor.
     *
     * <p>It is an error if the aggregate function already has an implementor.
     * But if the operator has an implementor in a parent table, it is
     * simply overridden.
     *
     * @pre op != null
     * @pre impl != null
     * @pre !operatorImplementationMap.containsKey(op)
     */
    public void registerAgg(
        SqlAggFunction agg,
        CalcRexAggImplementor impl)
    {
        Util.pre(agg != null, "agg != null");
        Util.pre(impl != null, "impl != null");
        Util.pre(!aggImplementationMap.containsKey(agg),
            "!aggImplementationMap.containsKey(op)");
        aggImplementationMap.put(agg, impl);
    }

    // NOTE jvs 16-June-2004:  There's a reason I use the convention
    // implements CalcRexImplementorTable
    // which is that it keeps jalopy from supplying the missing
    // method comment, while not preventing javadoc from inheriting
    // the comment from super

    /** implement interface CalcRexImplementorTable */
    public CalcRexImplementor get(SqlOperator op)
    {
        CalcRexImplementor implementor =
            (CalcRexImplementor) operatorImplementationMap.get(op);
        if ((implementor == null) && (parent != null)) {
            implementor = parent.get(op);
        }
        return implementor;
    }

    public CalcRexAggImplementor getAgg(SqlAggFunction op)
    {
        CalcRexAggImplementor implementor =
            (CalcRexAggImplementor) aggImplementationMap.get(op);
        if (implementor == null && parent != null) {
            implementor = parent.getAgg(op);
        }
        return implementor;
    }

    // helper methods

    /**
     * Converts a list of registers to an array.
     *
     * @pre regList != null
     * @post return != null
     */
    protected static CalcProgramBuilder.Register [] regListToRegArray(
        ArrayList regList)
    {
        Util.pre(regList != null, "regList != null");
        CalcProgramBuilder.Register [] regs =
            (CalcProgramBuilder.Register []) regList.toArray(
                new CalcProgramBuilder.Register[regList.size()]);
        return regs;
    }

    /**
     * Creates a register to hold the result of a call.
     *
     * @param translator Translator
     * @param call Call
     * @return A register
     */
    protected static CalcProgramBuilder.Register createResultRegister(
        RexToCalcTranslator translator,
        RexCall call)
    {
        CalcProgramBuilder.RegisterDescriptor resultDesc =
            translator.getCalcRegisterDescriptor(call);
        CalcProgramBuilder.Register resultOfCall =
            translator.builder.newLocal(resultDesc);
        return resultOfCall;
    }

    /**
     * Implements all operands to a call, and returns a list of the registers
     * which hold the results.
     *
     * @post return != null
     */
    protected static ArrayList implementOperands(
        RexCall call,
        RexToCalcTranslator translator)
    {
        return implementOperands(call, 0, call.operands.length, translator);
    }

    /**
     * Implements all operands to a call between start (inclusive) and
     * stop (exclusive), and returns a list of the registers which hold the
     * results.
     *
     * @post return != null
     */
    protected static ArrayList implementOperands(
        RexCall call,
        int start,
        int stop,
        RexToCalcTranslator translator)
    {
        ArrayList regList = new ArrayList();
        for (int i = start; i < stop; i++) {
            RexNode operand = call.operands[i];
            CalcProgramBuilder.Register reg =
                translator.implementNode(operand);
            regList.add(reg);
        }
        return regList;
    }

    /**
     * Implements a call by invoking a given instruction.
     *
     * @param instr Instruction
     * @param translator Translator
     * @param call Call to translate
     * @return Register which contains result, never null
     */
    private static CalcProgramBuilder.Register implementUsingInstr(
        CalcProgramBuilder.InstructionDef instr,
        RexToCalcTranslator translator,
        RexCall call)
    {
        CalcProgramBuilder.Register [] regs =
            new CalcProgramBuilder.Register[call.operands.length + 1];

        for (int i = 0; i < call.operands.length; i++) {
            RexNode operand = call.operands[i];
            regs[i + 1] = translator.implementNode(operand);
        }

        regs[0] = createResultRegister(translator, call);

        instr.add(translator.builder, regs);
        return regs[0];
    }

    /**
     * Converts a binary call (two regs as operands) by converting the first
     * operand to type {@link CalcProgramBuilder.OpType#Int8} for exact types
     * and {@link CalcProgramBuilder.OpType#Double} for approximate types and
     * then back again. Logically it will do something like<br>
     * t0 = type of first operand<br>
     * CAST(CALL(CAST(op0 as INT8), op1) as t0)<br>
     * If t0 is not a numeric or is
     * already is INT8 or DOUBLE, the CALL is simply returned as is.
     */
    private static RexCall implementFirstOperandWithDoubleOrInt8(
        RexCall call,
        RexToCalcTranslator translator,
        RexNode typeNode,
        int i,
        boolean castBack)
    {
        CalcProgramBuilder.RegisterDescriptor rd =
            translator.getCalcRegisterDescriptor(typeNode);
        if (rd.getType().isExact()) {
            return implementFirstOperandWithInt8(
                    call, translator, typeNode, i, castBack);
        } else if (rd.getType().isApprox()) {
            return implementFirstOperandWithDouble(
                    call, translator, typeNode, i, castBack);

        }
        return call;
    }

    /**
     * Converts a binary call (two regs as operands) by converting the first
     * operand to type {@link CalcProgramBuilder.OpType#Int8} if needed and
     * then back again. Logically it will do something like<br>
     * t0 = type of first operand<br>
     * CAST(CALL(CAST(op0 as INT8), op1) as t0)<br>
     * If t0 is not an exact type or is already is INT8,
     *  the CALL is simply returned as is.
     */
    private static RexCall implementFirstOperandWithInt8(
        RexCall call,
        RexToCalcTranslator translator,
        RexNode typeNode,
        int i,
        boolean castBack)
    {
        CalcProgramBuilder.RegisterDescriptor rd =
            translator.getCalcRegisterDescriptor(typeNode);
        if (rd.getType().isExact()
            && !rd.getType().equals(CalcProgramBuilder.OpType.Int8)) {
            RelDataType oldType = typeNode.getType();
            RelDataTypeFactory fac = translator.rexBuilder.getTypeFactory();

            //todo do a reverse lookup on OpType.Int8 instead
            RelDataType int8 = fac.createSqlType(SqlTypeName.Bigint);
            RexNode castCall1 =
                translator.rexBuilder.makeCast(int8, call.operands[i]);

            RexNode newCall;
            if (SqlStdOperatorTable.castFunc.equals(call.getOperator())) {
                newCall =
                    translator.rexBuilder.makeCast(
                        call.getType(),
                        castCall1);
            } else {
                RexNode [] args = new RexNode[call.operands.length];
                System.arraycopy(call.operands, 0, args, 0,
                    call.operands.length);
                args[i] = castCall1;
                newCall = translator.rexBuilder.makeCall(
                    call.getOperator(), args);
            }

            if (castBack) {
                newCall = translator.rexBuilder.makeCast(oldType, newCall);
            }
            return (RexCall) newCall;
        }
        return call;
    }

    /**
     * Same as {@link #implementFirstOperandWithInt8} but with
     * {@link CalcProgramBuilder.OpType#Double} instead
     * TODO need to abstract and merge functionality with
     * {@link #implementFirstOperandWithInt8} since they both contain nearly the
     * same code
     */
    private static RexCall implementFirstOperandWithDouble(
        RexCall call,
        RexToCalcTranslator translator,
        RexNode typeNode,
        int i,
        boolean castBack)
    {
        //TODO this method needs cleanup, it contains redundant code
        CalcProgramBuilder.RegisterDescriptor rd =
            translator.getCalcRegisterDescriptor(typeNode);
        if (rd.getType().isApprox()
            && !rd.getType().equals(CalcProgramBuilder.OpType.Double)) {
            RelDataType oldType = typeNode.getType();
            RelDataTypeFactory fac = translator.rexBuilder.getTypeFactory();

            //todo do a reverse lookup on OpType.Double instead
            RelDataType db = fac.createSqlType(SqlTypeName.Double);
            RexNode castCall1 =
                translator.rexBuilder.makeCast(db, call.operands[i]);

            RexNode newCall;
            if (SqlStdOperatorTable.castFunc.equals(call.getOperator())) {
                newCall =
                    translator.rexBuilder.makeCast(
                        call.getType(),
                        castCall1);
            } else {
                RexNode [] args = new RexNode[call.operands.length];
                System.arraycopy(call.operands, 0, args, 0,
                    call.operands.length);
                args[i] = castCall1;
                newCall = translator.rexBuilder.makeCall(
                    call.getOperator(), args);
            }

            if (castBack) {
                newCall = translator.rexBuilder.makeCast(oldType, newCall);
            }
            return (RexCall) newCall;
        }
        return call;
    }

    public static CalcRexImplementorTable threadInstance()
    {
        return (CalcRexImplementorTable) threadLocal.get();
    }

    public static void setThreadInstance(CalcRexImplementorTable table)
    {
        threadLocal.set(table);
    }

    /**
     * Registers the standard set of functions.
     */
    private CalcRexImplementorTableImpl initStandard()
    {
        register(
            SqlStdOperatorTable.absFunc,
            new InstrDefImplementor(ExtInstructionDefTable.abs) {
                public CalcProgramBuilder.Register implement(
                    RexCall call,
                    RexToCalcTranslator translator)
                {
                    RexCall newCall =
                        implementFirstOperandWithDoubleOrInt8(call, translator,
                            call.operands[0], 0, true);
                    if (newCall.equals(call)) {
                        return super.implement(call, translator);
                    }
                    return translator.implementNode(newCall);
                }
            });

        register(
            SqlStdOperatorTable.plusOperator,
            new BinaryNumericMakeSametypeImplementor(
                CalcProgramBuilder.nativeAdd));

        registerInstr(
            SqlStdOperatorTable.andOperator,
            CalcProgramBuilder.integralNativeAnd);

        register(
            SqlStdOperatorTable.divideOperator,
            new BinaryNumericMakeSametypeImplementor(
                CalcProgramBuilder.nativeDiv));

        register(
            SqlStdOperatorTable.caseOperator,
            new CaseImplementor());

        register(
            SqlStdOperatorTable.castFunc,
            new CastImplementor());

        if (false) {
            // TODO eventually need an extra argument for charset, in which
            // case we will use the following code
            register(
                SqlStdOperatorTable.characterLengthFunc,
                new AddCharSetNameInstrImplementor("CHAR_LENGTH", -1, 3));
        } else {
            registerInstr(
                SqlStdOperatorTable.characterLengthFunc,
                ExtInstructionDefTable.charLength);
        }

        // CHAR_LENGTH shares CHARACTER_LENGTH's implementation.
        // TODO: Combine the CHAR_LENGTH and CHARACTER_LENGTH at the
        //   RexNode level (they should remain separate functions at the
        //   SqlNode level).
        register(
            SqlStdOperatorTable.charLengthFunc,
            get(SqlStdOperatorTable.characterLengthFunc));

        register(
            SqlStdOperatorTable.concatOperator,
            new ConcatImplementor());

        register(
            SqlStdOperatorTable.equalsOperator,
            new BinaryNumericMakeSametypeImplementor(
                CalcProgramBuilder.boolNativeEqual));

        register(
            SqlStdOperatorTable.greaterThanOperator,
            new BinaryNumericMakeSametypeImplementor(
                CalcProgramBuilder.boolNativeGreaterThan));

        register(
            SqlStdOperatorTable.greaterThanOrEqualOperator,
            new BinaryNumericMakeSametypeImplementor(
                CalcProgramBuilder.boolNativeGreaterOrEqualThan));

        registerInstr(
            SqlStdOperatorTable.isNullOperator,
            CalcProgramBuilder.boolNativeIsNull);

        registerInstr(
            SqlStdOperatorTable.isNotNullOperator,
            CalcProgramBuilder.boolNativeIsNotNull);

        register(
            SqlStdOperatorTable.isTrueOperator,
            new IsBoolImplementor(true));

        register(
            SqlStdOperatorTable.isNotTrueOperator,
            new IsNotBoolImplementor(true));

        register(
            SqlStdOperatorTable.isFalseOperator,
            new IsBoolImplementor(false));

        register(
            SqlStdOperatorTable.isNotFalseOperator,
            new IsNotBoolImplementor(false));

        register(
            SqlStdOperatorTable.lessThanOperator,
            new BinaryNumericMakeSametypeImplementor(
                CalcProgramBuilder.boolNativeLessThan));

        register(
            SqlStdOperatorTable.lessThanOrEqualOperator,
            new BinaryNumericMakeSametypeImplementor(
                CalcProgramBuilder.boolNativeLessOrEqualThan));

        registerInstr(
            SqlStdOperatorTable.likeOperator,
            ExtInstructionDefTable.like);

        register(
            SqlStdOperatorTable.lnFunc,
            new MakeOperandsDoubleImplementor(ExtInstructionDefTable.log));

        register(
            SqlStdOperatorTable.log10Func,
            new MakeOperandsDoubleImplementor(ExtInstructionDefTable.log10));

        // TODO: need to know charset as well. When ready,
        // use same construct as with CHAR_LENGTH above
        registerInstr(
            SqlStdOperatorTable.lowerFunc,
            ExtInstructionDefTable.lower);

        register(
            SqlStdOperatorTable.minusOperator,
            new BinaryNumericMakeSametypeImplementor(
                CalcProgramBuilder.nativeMinus));

        register(
            SqlStdOperatorTable.modFunc,               
            new BinaryNumericMakeSametypeImplementor(
                CalcProgramBuilder.integralNativeMod, true));

        register(
            SqlStdOperatorTable.multiplyOperator,
            new BinaryNumericMakeSametypeImplementor(
                CalcProgramBuilder.integralNativeMul));

        register(
            SqlStdOperatorTable.notEqualsOperator,
            new BinaryNumericMakeSametypeImplementor(
                CalcProgramBuilder.boolNativeNotEqual));

        // todo: optimization. If using 'NOT' in front of 'IS NULL',
        // create a call to the calc instruction 'ISNOTNULL'
        registerInstr(
            SqlStdOperatorTable.notOperator,
            CalcProgramBuilder.boolNot);

        registerInstr(
            SqlStdOperatorTable.orOperator,
            CalcProgramBuilder.boolOr);

        register(
            SqlStdOperatorTable.overlayFunc,
            new BinaryStringMakeSametypeImplementor(
                ExtInstructionDefTable.overlay, 0, 1));

        register(
            SqlStdOperatorTable.positionFunc,
            new BinaryStringMakeSametypeImplementor(
                ExtInstructionDefTable.position));

        register(
            SqlStdOperatorTable.powFunc,
            new MakeOperandsDoubleImplementor(ExtInstructionDefTable.pow));

        registerInstr(
            SqlStdOperatorTable.prefixMinusOperator,
            CalcProgramBuilder.nativeNeg);

        register(
            SqlStdOperatorTable.prefixPlusOperator,
            new IdentityImplementor());

        register(
            opTab.reinterpretOperator,
            new ReinterpretCastImplementor());

        registerInstr(
            SqlStdOperatorTable.similarOperator,
            ExtInstructionDefTable.similar);

        registerInstr(
            SqlStdOperatorTable.substringFunc,
            ExtInstructionDefTable.substring);

        registerInstr(
            SqlStdOperatorTable.throwOperator,
            CalcProgramBuilder.raise);

        register(
            SqlStdOperatorTable.trimFunc,
            new TrimImplementor());

        // TODO: need to know charset aswell. When ready,
        // use same construct as with CHAR_LENGTH above
        registerInstr(
            SqlStdOperatorTable.upperFunc,
            ExtInstructionDefTable.upper);

        registerInstr(
            SqlStdOperatorTable.localTimeFunc,
            ExtInstructionDefTable.localTime);

        registerInstr(
            SqlStdOperatorTable.localTimestampFunc,
            ExtInstructionDefTable.localTimestamp);

        registerInstr(
            SqlStdOperatorTable.currentTimeFunc,
            ExtInstructionDefTable.currentTime);

        registerInstr(
            SqlStdOperatorTable.currentTimestampFunc,
            ExtInstructionDefTable.currentTimestamp);

        register(
            SqlStdOperatorTable.sliceOp,
            new SliceImplementor());

        // Register agg functions.
        registerAgg(
            SqlStdOperatorTable.sumOperator,
            new SumCalcRexImplementor());
        registerAgg(
            SqlStdOperatorTable.countOperator,
            new CountCalcRexImplementor());
        registerAgg(
            SqlStdOperatorTable.lastValueOperator,
            new LastValueCalcRexImplementor());
        if (false) {
            // TODO:
            registerAgg(
                SqlStdOperatorTable.minOperator,
                new SumCalcRexImplementor());
            registerAgg(
                SqlStdOperatorTable.maxOperator,
                new SumCalcRexImplementor());
        }

        return this;
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Abstract base class for classes which implement
     * {@link CalcRexImplementor}.
     */
    public abstract static class AbstractCalcRexImplementor
        implements CalcRexImplementor
    {
        public boolean canImplement(RexCall call)
        {
            if (RexUtil.requiresDecimalExpansion(call, true)) {
                return false;
            }
            return true;
        }
    }

    /**
     * Generic implementor that takes a {@link CalcProgramBuilder.InstructionDef}
     * which implements an operator by generating a call
     * to a given instruction.
     *
     * <p>If you need to tweak the arguments to the instruction, you can
     * override {@link #makeRegList}.
     */
    public static class InstrDefImplementor extends AbstractCalcRexImplementor
    {
        /**
         * The instruction with which to implement this operator.
         */
        protected final CalcProgramBuilder.InstructionDef instr;

        /**
         * Creates an instruction implementor
         * @pre null != instr
         * @param instr The instruction with which to implement this operator,
         *   must not be null
         * @pre instr != null
         */
        InstrDefImplementor(CalcProgramBuilder.InstructionDef instr)
        {
            Util.pre(null != instr, "null != instr");
            this.instr = instr;
        }

        public CalcProgramBuilder.Register implement(
            RexCall call,
            RexToCalcTranslator translator)
        {
            ArrayList regList = makeRegList(translator, call);
            CalcProgramBuilder.Register [] regs = regListToRegArray(regList);

            instr.add(translator.builder, regs);
            return regs[0];
        }

        /**
         * Creates the list of registers which will be arguments to the
         * instruction call. i.e implment all the operands of the call and
         * create a result register for the call.
         *
         * <p>The 0th argument is assumed to hold the result of the call.
         */
        protected ArrayList makeRegList(
            RexToCalcTranslator translator,
            RexCall call)
        {
            ArrayList regList = implementOperands(call, translator);

            // the result is implemented last in order to avoid changing the tests
            regList.add(
                0,
                createResultRegister(translator, call));

            return regList;
        }
    }

    /**
     * Implements "IS TRUE" and "IS FALSE" operators.
     */
    private static class IsBoolImplementor implements CalcRexImplementor
    {
        private boolean boolType;
        protected RexNode res;

        IsBoolImplementor(boolean boolType)
        {
            this.boolType = boolType;
        }

        public boolean canImplement(RexCall call)
        {
            return true;
        }

        public CalcProgramBuilder.Register implement(
            RexCall call,
            RexToCalcTranslator translator)
        {
            RexNode operand = call.operands[0];
            translator.implementNode(operand);

            if (operand.getType().isNullable()) {
                RexNode notNullCall =
                    translator.rexBuilder.makeCall(
                        SqlStdOperatorTable.isNotNullOperator,
                        operand);
                RexNode eqCall =
                    translator.rexBuilder.makeCall(
                        SqlStdOperatorTable.equalsOperator,
                        operand,
                        translator.rexBuilder.makeLiteral(boolType));
                res = translator.rexBuilder.makeCall(
                    SqlStdOperatorTable.andOperator,
                    notNullCall, eqCall);
            } else {
                res = translator.rexBuilder.makeCall(
                    SqlStdOperatorTable.equalsOperator,
                    operand,
                    translator.rexBuilder.makeLiteral(boolType));
            }
            return translator.implementNode(res);
        }
    }

    /**
     * Implements "IS NOT TRUE" and "IS NOT FALSE" operators.
     */
    private static class IsNotBoolImplementor extends IsBoolImplementor
    {
        IsNotBoolImplementor(boolean boolType)
        {
            super(boolType);
        }

        public CalcProgramBuilder.Register implement(
            RexCall call,
            RexToCalcTranslator translator)
        {
            super.implement(call, translator);
            res = translator.rexBuilder.makeCall(
                SqlStdOperatorTable.notOperator, res);
            return translator.implementNode(res);
        }
    }

    /**
     * A Class that gets a specified operand of a call and
     * retrieves its charset name and add it as a vc literal to the program.
     * This of course assumes the operand is a chartype.
     * If this is not the case an assert is fired.
     */
    private static class AddCharSetNameInstrImplementor
        extends InstrDefImplementor
    {
        int operand;

        AddCharSetNameInstrImplementor(
            String extCall,
            int regCount,
            int operand)
        {
            super(new CalcProgramBuilder.ExtInstrDef(extCall, regCount));
            this.operand = operand;
        }

        /**
         * @pre SqlTypeUtil.inCharFamily(call.operands[operand].getType())
         */
        protected ArrayList makeRegList(
            RexToCalcTranslator translator,
            RexCall call)
        {
            Util.pre(
                SqlTypeUtil.inCharFamily(call.operands[operand].getType()),
                "SqlTypeUtil.inCharFamily(call.operands[operand].getType()");

            ArrayList regList = super.makeRegList(translator, call);
            CalcProgramBuilder.Register charSetName =
                translator.builder.newVarcharLiteral(
                    call.operands[operand].getType().getCharset().name());
            regList.add(charSetName);
            return regList;
        }
    }

    /**
     * Implements a call by invoking a given instruction.
     */
    private static class UsingInstrImplementor
        extends AbstractCalcRexImplementor
    {
        CalcProgramBuilder.InstructionDef instr;

        /**
         * @pre null != instr
         */
        UsingInstrImplementor(CalcProgramBuilder.InstructionDef instr)
        {
            Util.pre(null != instr, "null != instr");
            this.instr = instr;
        }

        public CalcProgramBuilder.Register implement(
            RexCall call,
            RexToCalcTranslator translator)
        {
            return implementUsingInstr(instr, translator, call);
        }
    }

    /**
     * Implementor that will convert a {@link RexCall}'s operands to
     * approx DOUBLE if needed
     */
    private static class MakeOperandsDoubleImplementor
        extends InstrDefImplementor
    {
        MakeOperandsDoubleImplementor(CalcProgramBuilder.InstructionDef instr)
        {
            super(instr);
        }

        public CalcProgramBuilder.Register implement(
            RexCall call,
            RexToCalcTranslator translator)
        {
            call = (RexCall) call.clone();
            for (int i = 0; i < call.operands.length; i++) {
                RexNode operand = call.operands[i];
                if (!operand.getType().getSqlTypeName().equals(SqlTypeName.Double)) {
                    RelDataType oldType = operand.getType();
                    RelDataTypeFactory fac =
                        translator.rexBuilder.getTypeFactory();

                    //todo do a reverse lookup on OpType.Double instead
                    RelDataType doubleType =
                        fac.createSqlType(SqlTypeName.Double);
                    doubleType =
                        fac.createTypeWithNullability(
                            doubleType,
                            oldType.isNullable());
                    RexNode castCall =
                        translator.rexBuilder.makeCast(doubleType,
                            call.operands[i]);
                    call.operands[i] = castCall;
                }
            }
            assert (0 != call.operands.length);
            return super.implement(call, translator);
        }
    }

    /**
     * Implementor for CAST operator.
     */
    private static class CastImplementor extends AbstractCalcRexImplementor
    {
        private static DoubleKeyMap doubleKeyMap;

        static {
            doubleKeyMap = new DoubleKeyMap();
            doubleKeyMap.setEnforceUniqueness(true);

            doubleKeyMap.put(
                SqlTypeName.intTypes,
                SqlTypeName.intTypes,
                new UsingInstrImplementor(CalcProgramBuilder.Cast));
            doubleKeyMap.put(
                SqlTypeName.intTypes,
                SqlTypeName.approxTypes,
                new UsingInstrImplementor(CalcProgramBuilder.Cast));
            doubleKeyMap.put(
                SqlTypeName.datetimeTypes,
                SqlTypeName.Bigint,
                new UsingInstrImplementor(
                    ExtInstructionDefTable.castDateToMillis));
            doubleKeyMap.put(
                SqlTypeName.booleanTypes,
                SqlTypeName.charTypes,
                new UsingInstrImplementor(
                    ExtInstructionDefTable.castA));
            doubleKeyMap.put(
                SqlTypeName.intTypes,
                SqlTypeName.charTypes,
                new UsingInstrImplementor(ExtInstructionDefTable.castA) {
                    public CalcProgramBuilder.Register implement(
                        RexCall call,
                        RexToCalcTranslator translator)
                    {
                        RexCall newCall =
                            implementFirstOperandWithInt8(call, translator,
                                call.operands[0], 0, false);
                        if (newCall.equals(call)) {
                            return super.implement(call, translator);
                        }
                        return translator.implementNode(newCall);
                    }
                });

            doubleKeyMap.put(
                SqlTypeName.approxTypes,
                SqlTypeName.approxTypes,
                new UsingInstrImplementor(CalcProgramBuilder.Cast));
            doubleKeyMap.put(
                SqlTypeName.approxTypes,
                SqlTypeName.charTypes,
                new UsingInstrImplementor(ExtInstructionDefTable.castA) {
                    public CalcProgramBuilder.Register implement(
                        RexCall call,
                        RexToCalcTranslator translator)
                    {
                        RexCall newCall =
                            implementFirstOperandWithDouble(call, translator,
                                call.operands[0], 0, false);
                        if (newCall.equals(call)) {
                            return super.implement(call, translator);
                        }
                        return translator.implementNode(newCall);
                    }
                });
            doubleKeyMap.put(
                SqlTypeName.approxTypes,
                SqlTypeName.intTypes,
                new AbstractCalcRexImplementor() {
                    public CalcProgramBuilder.Register implement(
                        RexCall call,
                        RexToCalcTranslator translator)
                    {
                        assert (call.isA(RexKind.Cast));
                        CalcProgramBuilder.Register beforeRound =
                            translator.implementNode(call.operands[0]);
                        CalcProgramBuilder.RegisterDescriptor regDesc =
                            translator.getCalcRegisterDescriptor(call.operands[0]);
                        CalcProgramBuilder.Register afterRound =
                            translator.builder.newLocal(regDesc);
                        CalcProgramBuilder.round.add(
                            translator.builder,
                            new CalcProgramBuilder.Register [] {
                                afterRound, beforeRound
                            });
                        CalcProgramBuilder.Register res =
                            createResultRegister(translator, call);
                        CalcProgramBuilder.Cast.add(
                            translator.builder,
                            new CalcProgramBuilder.Register [] { res, afterRound });
                        return res;
                    }
                });

            doubleKeyMap.put(
                SqlTypeName.charTypes,
                SqlTypeName.Date,
                new UsingInstrImplementor(
                    ExtInstructionDefTable.castStrAToDate));
            doubleKeyMap.put(
                SqlTypeName.Date,
                SqlTypeName.charTypes,
                new UsingInstrImplementor(ExtInstructionDefTable.castDateToStr));

            doubleKeyMap.put(
                SqlTypeName.charTypes,
                SqlTypeName.Time,
                new UsingInstrImplementor(
                    ExtInstructionDefTable.castStrAToTime));
            doubleKeyMap.put(
                SqlTypeName.Time,
                SqlTypeName.charTypes,
                new UsingInstrImplementor(ExtInstructionDefTable.castTimeToStr));

            doubleKeyMap.put(
                SqlTypeName.charTypes,
                SqlTypeName.Timestamp,
                new UsingInstrImplementor(
                    ExtInstructionDefTable.castStrAToTimestamp));
            doubleKeyMap.put(
                SqlTypeName.Timestamp,
                SqlTypeName.charTypes,
                new UsingInstrImplementor(
                    ExtInstructionDefTable.castTimestampToStr));

            doubleKeyMap.put(
                SqlTypeName.charTypes,
                SqlTypeName.booleanTypes,
                new UsingInstrImplementor(
                    ExtInstructionDefTable.castA));

            doubleKeyMap.put(
                SqlTypeName.charTypes,
                SqlTypeName.intTypes,
                new UsingInstrImplementor(ExtInstructionDefTable.castA) {
                    public CalcProgramBuilder.Register implement(
                        RexCall call,
                        RexToCalcTranslator translator)
                    {
                        RexCall newCall =
                            implementFirstOperandWithInt8(call, translator,
                                call, 0, false);
                        if (newCall.equals(call)) {
                            return super.implement(call, translator);
                        }
                        return translator.implementNode(newCall);
                    }
                });
            doubleKeyMap.put(
                SqlTypeName.charTypes,
                SqlTypeName.approxTypes,
                new UsingInstrImplementor(ExtInstructionDefTable.castA) {
                    public CalcProgramBuilder.Register implement(
                        RexCall call,
                        RexToCalcTranslator translator)
                    {
                        RexCall newCall =
                            implementFirstOperandWithDouble(call, translator,
                                call, 0, false);
                        if (newCall.equals(call)) {
                            return super.implement(call, translator);
                        }
                        return translator.implementNode(newCall);
                    }
                });

            doubleKeyMap.put(
                SqlTypeName.charTypes,
                SqlTypeName.charTypes,
                new UsingInstrImplementor(ExtInstructionDefTable.castA));
            
            doubleKeyMap.put(
                SqlTypeName.Decimal,
                SqlTypeName.charTypes,
                new CastDecimalImplementor(
                    ExtInstructionDefTable.castADecimal));
            doubleKeyMap.put(
                SqlTypeName.charTypes,
                SqlTypeName.Decimal,
                new CastDecimalImplementor(
                    ExtInstructionDefTable.castADecimal));
        }

        public CalcProgramBuilder.Register implement(
            RexCall call,
            RexToCalcTranslator translator)
        {
            Util.pre(call.operands.length == 1, "call.operands.length == 1");

            final RexNode operand = translator.resolve(call.operands[0]);
            if (RexLiteral.isNullLiteral(operand)) {
                CalcProgramBuilder.RegisterDescriptor resultDesc =
                    translator.getCalcRegisterDescriptor(call);

                // If the type is one which requires an explicit storage
                // specification, tell it we need 0 bytes;
                // otherwise keep it at -1.
                if (resultDesc.getBytes() >= 0) {
                    resultDesc =
                        new CalcProgramBuilder.RegisterDescriptor(
                            resultDesc.getType(),
                            0);
                }

                return translator.builder.newLiteral(resultDesc, null);
            }

            // Figure out the source and destination types.
            RelDataType fromType = operand.getType();
            SqlTypeName fromTypeName = fromType.getSqlTypeName();
            RelDataType toType = call.getType();
            SqlTypeName toTypeName = toType.getSqlTypeName();

            CalcRexImplementor implentor =
                (CalcRexImplementor) doubleKeyMap.get(fromTypeName, toTypeName);
            if (null != implentor) {
                return implentor.implement(call, translator);
            }

            if (SqlTypeUtil.sameNamedType(toType, fromType)) {
                RexNode op = operand;
                return translator.implementNode(op);
            }

            throw Util.needToImplement("Cast from '" + fromType.toString()
                + "' to '" + toType.toString() + "'");
        }
        
        /** Implementor for casting between char and decimal types */
        private static class CastDecimalImplementor extends InstrDefImplementor
        {
            CastDecimalImplementor(CalcProgramBuilder.InstructionDef instr) 
            {
                super(instr);
            }
            
            // refine InstrDefImplementor
            protected ArrayList makeRegList(
                RexToCalcTranslator translator,
                RexCall call)
            {
                RelDataType decimalType;
                Util.pre(SqlTypeUtil.isDecimal(call.getType())
                    || SqlTypeUtil.isDecimal(call.operands[0].getType()),
                    "CastDecimalImplementor can only cast decimal types");
                if (SqlTypeUtil.isDecimal(call.getType())) {
                    Util.pre(
                        SqlTypeUtil.inCharFamily(call.operands[0].getType()),
                        "CalRex cannot cast non char type to decimal");
                    decimalType = call.getType();
                } else {
                    Util.pre(
                        SqlTypeUtil.inCharFamily(call.getType()),
                        "CalRex cannot cast from decimal to non char type");
                    decimalType = call.operands[0].getType();
                }
                RexLiteral precision = translator.rexBuilder
                    .makeExactLiteral(
                        BigDecimal.valueOf(decimalType.getPrecision()));
                RexLiteral scale = translator.rexBuilder
                    .makeExactLiteral(
                        BigDecimal.valueOf(decimalType.getScale()));
                
                ArrayList regList = implementOperands(call, translator);
                regList.add(translator.implementNode(precision));
                regList.add(translator.implementNode(scale));
                regList.add(0, createResultRegister(translator, call));
                return regList;
            }
        }
    }

    /**
     * Implementor for REINTERPRET operator.
     */
    private static class ReinterpretCastImplementor extends AbstractCalcRexImplementor
    {
        public boolean canImplement(RexCall call)
        {
            return (call.isA(RexKind.Reinterpret));
        }
        
        public CalcProgramBuilder.Register implement(
            RexCall call,
            RexToCalcTranslator translator)
        {
            RexNode valueArg = call.operands[0];
            boolean checkOverflow = RexUtil.canReinterpretOverflow(call);
            
            CalcProgramBuilder.Register value = 
                translator.implementNode(valueArg);
            if (checkOverflow) {
                if (! SqlTypeUtil.isIntType(valueArg.getType())) {
                    valueArg = 
                        translator.rexBuilder.makeReinterpretCast(
                            call.getType(),
                            valueArg, 
                            translator.rexBuilder.makeLiteral(false));
                }
                // perform overflow check:
                //     if (value is null) goto [endCheck]
                //     bool overflowed = ( abs(value) >= overflowValue )
                //     if (!overflowed) goto [endCheck]
                //     throw overflow exception
                // [endCheck]
                String endCheck = translator.newLabel();
                if (valueArg.getType().isNullable()) {
                    RexNode nullCheck = 
                        translator.rexBuilder.makeCall(
                            opTab.isNullOperator,
                            valueArg);
                    CalcProgramBuilder.Register isNull = 
                        translator.implementNode(nullCheck);
                    translator.builder.addLabelJumpTrue(endCheck, isNull);
                }
                RexNode overflowValue = 
                    translator.rexBuilder.makeExactLiteral(
                        new BigDecimal(
                            NumberUtil.getMaxUnscaled(
                                call.getType().getPrecision())));
                RexNode comparison = translator.rexBuilder.makeCall(
                    opTab.greaterThanOperator,
                    translator.rexBuilder.makeCall(
                        opTab.absFunc,
                        valueArg),
                    overflowValue);
                CalcProgramBuilder.Register overflowed = 
                    translator.implementNode(comparison);
                translator.builder.addLabelJumpFalse(endCheck, overflowed);
                CalcProgramBuilder.Register errorMsg =
                    translator.builder.newVarcharLiteral(
                        SqlStateCodes.NumericValueOutOfRange.getState());
                CalcProgramBuilder.raise.add(
                    translator.builder, errorMsg);
                translator.builder.addLabel(endCheck);
            }
            return value;
        }
    }

    /**
     * Makes all numeric types the same before calling a given
     * {@link CalcProgramBuilder.InstructionDef instruction}. The way to make the
     * types the same is by inserting appropiate calls to cast functions
     * depending on the types. The "biggest" (least restrictive) type will
     * always win and other types will be conveted into that bigger type.
     * For example.
     * In the expression <code>1.0+2</code>, the '+' instruction is potentially called
     * with types <code>(DOUBLE) + (INTEGER)</code> which is illegal (in terms of the calculator).
     * Therefore the expression's implementation will logically end looking something like
     * <code>1.0 + CAST(2 AS DOUBLE)</code>
     * LIMITATION: For now only Binary operators are supported with numeric types
     * If any operand is of any other type than a numeric one, the base class
     * {@link InstrDefImplementor#implement implementation} will be called
     */
    private static class BinaryNumericMakeSametypeImplementor
        extends InstrDefImplementor
    {
        // Set this to true if the result type need to be same as operands
        boolean useSameTypeResult = false;
        
        public BinaryNumericMakeSametypeImplementor(
            CalcProgramBuilder.InstructionDef instr)
        {
            super(instr);
        }

        public BinaryNumericMakeSametypeImplementor(
            CalcProgramBuilder.InstructionDef instr,
            boolean useSameTypeResult)
        {
            this(instr);
            this.useSameTypeResult = useSameTypeResult;
        }

        private int getRestrictiveness(
            CalcProgramBuilder.RegisterDescriptor rd)
        {
            switch (rd.getType().getOrdinal()) {
            case CalcProgramBuilder.OpType.Bool_ordinal:
                return 5;
            case CalcProgramBuilder.OpType.Uint1_ordinal:
                return 10;
            case CalcProgramBuilder.OpType.Int1_ordinal:
                return 20;
            case CalcProgramBuilder.OpType.Uint2_ordinal:
                return 30;
            case CalcProgramBuilder.OpType.Int2_ordinal:
                return 40;
            case CalcProgramBuilder.OpType.Uint4_ordinal:
                return 50;
            case CalcProgramBuilder.OpType.Int4_ordinal:
                return 60;
            case CalcProgramBuilder.OpType.Uint8_ordinal:
                return 70;
            case CalcProgramBuilder.OpType.Int8_ordinal:
                return 80;
            case CalcProgramBuilder.OpType.Real_ordinal:
                return 1000;
            case CalcProgramBuilder.OpType.Double_ordinal:
                return 1010;
            default:
                throw rd.getType().unexpected();
            }
        }

        public CalcProgramBuilder.Register implement(
            RexCall call,
            RexToCalcTranslator translator)
        {
            assert (2 == call.operands.length);
            ArrayList regs = implementOperands(call, translator);
            CalcProgramBuilder.RegisterDescriptor rd0 =
                translator.getCalcRegisterDescriptor(call.operands[0]);
            CalcProgramBuilder.RegisterDescriptor rd1 =
                translator.getCalcRegisterDescriptor(call.operands[1]);

            if (!rd0.getType().isNumeric() || !rd1.getType().isNumeric()) {
                return super.implement(call, translator);
            }

            CalcProgramBuilder.RegisterDescriptor rd = null;
            int d = getRestrictiveness(rd0) - getRestrictiveness(rd1);
            if (d != 0) {
                int small;
                if (d > 0) {
                    small = 1;
                    rd = rd0;
                } else {
                    small = 0;
                    rd = rd1;
                }

                RexNode castCall =
                    translator.rexBuilder.makeCast(
                        call.operands[(small + 1) % 2].getType(),
                        call.operands[small]);
                CalcProgramBuilder.Register newOp =
                    translator.implementNode(castCall);
                regs.set(small, newOp);
            }

            if (useSameTypeResult && rd != null) {
                // Need to use the same type for the result as the operands
                CalcProgramBuilder.RegisterDescriptor rdCall =
                    translator.getCalcRegisterDescriptor(call.getType());
                if (rdCall.getType().isNumeric() &&
                    (getRestrictiveness(rd) - getRestrictiveness(rdCall) != 0)) {
                    // Do operation using same type as operands
                    CalcProgramBuilder.Register tmpRes =
                        translator.builder.newLocal(rd);
                    regs.add(0, tmpRes);

                    instr.add(translator.builder, regs);

                    // Cast back to real result type
                    // TODO: Use real cast (that handles rounding) instead of
                    // calculator cast that truncates
                    CalcProgramBuilder.Register res =
                        createResultRegister(translator, call);

                    ArrayList castRegs = new ArrayList(2);
                    castRegs.add(res);
                    castRegs.add(tmpRes);
                    CalcProgramBuilder.Cast.add(translator.builder, castRegs);

                    return res;
                }
            }

            CalcProgramBuilder.Register res =
                createResultRegister(translator, call);
            regs.add(0, res);

            instr.add(translator.builder, regs);
            return res;
        }
    }

    /**
     * Makes all string types the same before calling a given
     * {@link CalcProgramBuilder.InstructionDef instruction}. The way to make the
     * types the same is by inserting appropiate calls to cast functions
     * depending on the types. The "biggest" (least restrictive) type will
     * always win and other types will be conveted into that bigger type.
     * For example.
     * In the expression <code>POSITION('varchar' in 'char')</code>, will end up
     * looking something like
     * <code>POSITION('varchar' in CAST('char' AS VARCHAR))</code>
     * LIMITATION: For now only char types are supported
     */
    private static class BinaryStringMakeSametypeImplementor
        extends InstrDefImplementor
    {
        private int iFirst;
        private int iSecond;

        /**
         * @param iFirst Indicates which two operands in the call list that should be made
         * the same type.
         * @param iSecond Indicates which two operands in the call list that should be made
         * the same type.
         */
        public BinaryStringMakeSametypeImplementor(
            CalcProgramBuilder.InstructionDef instr, int iFirst, int iSecond)
        {
            super(instr);
            assert(iFirst!=iSecond);
            this.iFirst = iFirst;
            this.iSecond = iSecond;
        }

        /**
         * Convenience constructor that calls
         * {@link #BinaryStringMakeSametypeImplementor(com.disruptivetech.farrago.calc.CalcProgramBuilder.InstructionDef, int, int)}
         * with the iFirst=0 and iSecond=1
         */
        public BinaryStringMakeSametypeImplementor(
            CalcProgramBuilder.InstructionDef instr)
        {
            this(instr, 0, 1);
        }

        public CalcProgramBuilder.Register implement(
            RexCall call,
            RexToCalcTranslator translator)
        {
            ArrayList regs = implementOperands(call, translator);
            CalcProgramBuilder.Register[] newRegs = new CalcProgramBuilder.Register[2];
            newRegs[0] = (CalcProgramBuilder.Register) regs.get(iFirst);
            newRegs[1] = (CalcProgramBuilder.Register) regs.get(iSecond);

            translator.implementConversionIfNeeded(
                call.operands[iFirst], call.operands[iSecond], newRegs);
            regs.set(iFirst, newRegs[0]);
            regs.set(iSecond, newRegs[1]);

            CalcProgramBuilder.Register res =
                createResultRegister(translator, call);
            regs.add(0, res);

            instr.add(translator.builder, regs);
            return res;
        }
    }

    /**
     * Implementor for CASE operator.
     */
    private static class CaseImplementor extends AbstractCalcRexImplementor
    {
        public CalcProgramBuilder.Register implement(
            RexCall call,
            RexToCalcTranslator translator)
        {
            Util.pre(call.operands.length > 1, "call.operands.length>1");
            Util.pre((call.operands.length & 1) == 1,
                "(call.operands.length&1)==1");
            CalcProgramBuilder.Register resultOfCall =
                createResultRegister(translator, call);
            String endOfCase = translator.newLabel();
            String next;
            boolean elseClauseOptimizedAway = false;
            for (int i = 0; i < (call.operands.length - 1); i += 2) {
                next = translator.newLabel();
                CalcProgramBuilder.Register compareResult =
                    translator.implementNode(call.operands[i]);
                assert (compareResult.getOpType().equals(CalcProgramBuilder.OpType.Bool));
                if (!compareResult.getRegisterType().equals(CalcProgramBuilder.RegisterSetType.Literal)) {
                    translator.builder.addLabelJumpFalse(next, compareResult);

                    // todo optimize away null check if type known to be non null
                    // same applies the other way (if we have a null literal or a cast(null as xxx))
                    translator.builder.addLabelJumpNull(next, compareResult);
                    implementCaseValue(
                        translator,
                        resultOfCall, 
                        call.operands[i + 1]);
                    translator.builder.addLabelJump(endOfCase);
                    translator.builder.addLabel(next);
                } else {
                    // we can do some optimizations
                    Boolean val = (Boolean) compareResult.getValue();
                    if (val.booleanValue()) {
                        implementCaseValue(
                            translator,
                            resultOfCall, 
                            call.operands[i + 1]);
                        if (i != 0) {
                            translator.builder.addLabelJump(endOfCase);
                        }
                        translator.builder.addLabel(next);
                        elseClauseOptimizedAway = true;
                        break;
                    }

                    // else we dont need to do anything
                }
            }

            if (!elseClauseOptimizedAway) {
                int elseIndex = call.operands.length - 1;
                implementCaseValue(
                    translator,
                    resultOfCall, 
                    call.operands[elseIndex]);
            }
            translator.builder.addLabel(endOfCase); //this assumes that more instructions will follow
            return resultOfCall;
        }
        
        private void implementCaseValue(
            RexToCalcTranslator translator, 
            CalcProgramBuilder.Register resultOfCall,
            RexNode value)
        {
            translator.newScope();
            try {
                CalcProgramBuilder.Register operand =
                    translator.implementNode(value);
                if ((resultOfCall.getOpType() != operand.getOpType())
                    || (resultOfCall.storageBytes != operand.storageBytes))
                {
                    ExtInstructionDefTable.castA.add(
                        translator.builder,
                        new CalcProgramBuilder.Register [] { 
                            resultOfCall,
                            operand });
                } else {
                    CalcProgramBuilder.move.add(
                        translator.builder,
                        resultOfCall,
                        operand);
                }
            } finally {
                translator.popScope();
            }
        }
    }

    /**
     * Implements the identity operator.
     *
     * <p>The prefix plus operator uses this implementor, because "+ x" is
     * always the same as "x".
     */
    private static class IdentityImplementor implements CalcRexImplementor
    {
        public CalcProgramBuilder.Register implement(
            RexCall call,
            RexToCalcTranslator translator)
        {
            return translator.implementNode(call.operands[0]);
        }

        public boolean canImplement(RexCall call)
        {
            return true;
        }
    }

    /**
     * Implements the TRIM function.
     */
    private static class TrimImplementor extends InstrDefImplementor
    {
        public TrimImplementor()
        {
            super(ExtInstructionDefTable.trim);
        }

        protected ArrayList makeRegList(
            RexToCalcTranslator translator,
            RexCall call)
        {
            ArrayList regList = new ArrayList();

            CalcProgramBuilder.Register resultOfCall =
                createResultRegister(translator, call);
            RexNode op0 = call.operands[0];
            final RexLiteral literal = translator.getLiteral(op0);
            SqlTrimFunction.Flag flag =
                (SqlTrimFunction.Flag) literal.getValue();

            regList.add(resultOfCall);
            regList.add(translator.implementNode(call.operands[2])); //str to trim from
            regList.add(translator.implementNode(call.operands[1])); //trim char
            regList.add(translator.builder.newInt4Literal(flag.getLeft()));
            regList.add(translator.builder.newInt4Literal(flag.getRight()));

            return regList;
        }
    }

    private static class ConcatImplementor extends AbstractCalcRexImplementor
    {
        public CalcProgramBuilder.Register implement(
            RexCall call,
            RexToCalcTranslator translator)
        {
            assert (!translator.containsResult(call)); //avoid reentrancy
            CalcProgramBuilder.Register resultReg =
                createResultRegister(translator, call);
            return implement(call, translator, resultReg);
        }

        private CalcProgramBuilder.Register implement(
            RexCall call,
            RexToCalcTranslator translator,
            CalcProgramBuilder.Register resultRegister)
        {
            assert (2 == call.operands.length);
            List regList = new ArrayList();
            regList.add(resultRegister);

            if ((!(call.operands[0] instanceof RexCall)
                || !((RexCall) call.operands[0]).getOperator().equals(
                    SqlStdOperatorTable.concatOperator))) {
                regList.add(translator.implementNode(call.operands[0]));
            } else {
                //recursively calling this method again
                implement((RexCall) call.operands[0], translator,
                    resultRegister);
            }

            regList.add(translator.implementNode(call.operands[1]));
            assert(regList.size()>1);
            boolean castToVarchar = false;
            if (!CalcProgramBuilder.OpType.Char.equals(
                    resultRegister.getOpType()))
            {
                assert(CalcProgramBuilder.OpType.Varchar.equals(
                           resultRegister.getOpType()));
                castToVarchar = true;
            }
            for (int i = 1; i < regList.size(); i++) {
                CalcProgramBuilder.Register reg=
                    (CalcProgramBuilder.Register) regList.get(i);

                if (castToVarchar && !reg.getOpType().equals(
                    CalcProgramBuilder.OpType.Varchar)) {
                    // cast to varchar call must be of type varchar.
                    CalcProgramBuilder.Register newReg =
                        translator.builder.newLocal(
                        translator.getCalcRegisterDescriptor(call));

                    ExtInstructionDefTable.castA.add(
                        translator.builder,
                        new CalcProgramBuilder.Register [] { newReg, reg });
                    regList.set(i, newReg);
                }
            }

            ExtInstructionDefTable.concat.add(translator.builder, regList);
            return resultRegister;
        }
    }

    /**
     * Abstract base class for classes which implement
     * {@link CalcRexAggImplementor}.
     */
    public static abstract class AbstractCalcRexAggImplementor
        implements CalcRexAggImplementor
    {
        public boolean canImplement(RexCall call)
        {
            return true;
        }
    }

    /**
     * Implementation of the <code>COUNT</code> aggregate function,
     * {@link SqlStdOperatorTable#countOperator}.
     */
    private static class CountCalcRexImplementor
        extends AbstractCalcRexAggImplementor
    {
        public void implementInitialize(
            RexCall call,
            CalcProgramBuilder.Register accumulatorRegister,
            RexToCalcTranslator translator)
        {
            // O s8; V 0; T; MOVE O0, C0;
            assert call.operands.length == 0 || call.operands.length == 1;

            final CalcProgramBuilder.Register zeroReg =
                translator.builder.newLiteral(
                translator.getCalcRegisterDescriptor(call),
                integer0);
            CalcProgramBuilder.move.add(
                translator.builder,
                accumulatorRegister,
                zeroReg);
        }

        public void implementAdd(
            RexCall call,
            CalcProgramBuilder.Register accumulatorRegister,
            RexToCalcTranslator translator)
        {
            // I s8; V 1; T; ADD O0, O0, C0;
            assert call.operands.length == 0 || call.operands.length == 1;

            final CalcProgramBuilder.Register oneReg =
                translator.builder.newLiteral(
                translator.getCalcRegisterDescriptor(call),
                integer1);

            // If operand is null, then it is like count(*).
            // Otherwise, it is like count(x).
            RexNode operand = null;
            if (call.operands.length == 1) {
                operand = call.operands[0];
            }

            // Minor optimization where count(*) = count(x) if x
            // cannot be null. See the help for SumCalcRexImplementor.implementAdd()
            if (operand != null && operand.getType().isNullable()) {
                int ordinal = translator.getNullRegisterOrdinal();
                CalcProgramBuilder.Register isNullReg = null;
                String wasNotNull = translator.newLabel();
                String next = translator.newLabel();
                if (ordinal == -1) {
                    isNullReg = translator.builder.
                            newLocal(CalcProgramBuilder.OpType.Bool, -1);
                    translator.setNullRegisterOrdinal(translator.builder.registerSets.getSet
                            (CalcProgramBuilder.RegisterSetType.LocalORDINAL).size()-1);
                } else {
                    isNullReg = translator.builder.getRegister
                            (ordinal, CalcProgramBuilder.RegisterSetType.Local);
                }

                CalcProgramBuilder.Register input = null;
                final RexNode[] inputExps = translator.getInputExprs();
                if (inputExps != null) {
                    RexNode arg = inputExps[((RexInputRef) operand).getIndex()];
                    input = translator.implementNode(arg);
                } else {
                    input = translator.implementNode(operand);
                }

                CalcProgramBuilder.boolNativeIsNull.add(translator.builder,
                        isNullReg, input);
                translator.builder.addLabelJumpFalse(wasNotNull, isNullReg);
                translator.builder.addLabelJump(next);
                translator.builder.addLabel(wasNotNull);
                CalcProgramBuilder.nativeAdd.add(
                    translator.builder,
                    accumulatorRegister,
                    accumulatorRegister,
                    oneReg);
                translator.builder.addLabel(next);
            } else {
                CalcProgramBuilder.nativeAdd.add(
                    translator.builder,
                    accumulatorRegister,
                    accumulatorRegister,
                    oneReg);
            }
        }

        public void implementDrop(
            RexCall call,
            CalcProgramBuilder.Register accumulatorRegister,
            RexToCalcTranslator translator)
        {
            // I s8; V 1; T; SUB O0, O0, C0;
            assert call.operands.length == 0 || call.operands.length == 1;

            final CalcProgramBuilder.Register oneReg =
                translator.builder.newLiteral(
                translator.getCalcRegisterDescriptor(call),
                integer1);

            // If operand is null, then it is like count(*).
            // Otherwise, it is like count(x).
            RexNode operand = null;
            if (call.operands.length == 1) {
                operand = call.operands[0];
            }
            // Minor optimization where count(*) = count(x) if x
            // cannot be null. See the help for SumCalcRexImplementor.implementDrop()
            if (operand != null && operand.getType().isNullable()) {
                int ordinal = translator.getNullRegisterOrdinal();
                CalcProgramBuilder.Register isNullReg = null;
                String wasNotNull = translator.newLabel();
                String next = translator.newLabel();
                if (ordinal == -1) {
                    isNullReg = translator.builder.
                            newLocal(CalcProgramBuilder.OpType.Bool, -1);
                    translator.setNullRegisterOrdinal(translator.builder.registerSets.getSet
                            (CalcProgramBuilder.RegisterSetType.LocalORDINAL).size()-1);
                } else {
                    isNullReg = translator.builder.getRegister
                            (ordinal, CalcProgramBuilder.RegisterSetType.Local);
                }

                CalcProgramBuilder.Register input = null;
                final RexNode[] inputExps = translator.getInputExprs();
                if (inputExps != null) {
                    RexNode arg = inputExps[((RexInputRef) operand).getIndex()];
                    input = translator.implementNode(arg);
                } else {
                    input = translator.implementNode(operand);
                }

                CalcProgramBuilder.boolNativeIsNull.add(translator.builder,
                        isNullReg, input);
                translator.builder.addLabelJumpFalse(wasNotNull, isNullReg);
                translator.builder.addLabelJump(next);
                translator.builder.addLabel(wasNotNull);
                CalcProgramBuilder.nativeMinus.add(
                    translator.builder,
                    accumulatorRegister,
                    accumulatorRegister,
                    oneReg);
                translator.builder.addLabel(next);
            } else {
                CalcProgramBuilder.nativeMinus.add(
                    translator.builder,
                    accumulatorRegister,
                    accumulatorRegister,
                    oneReg);
            }
        }
    }

    /**
     * Implementation of the <code>SUM</code> aggregate function,
     * {@link SqlStdOperatorTable#sumOperator}.
     */
    private static class SumCalcRexImplementor
        extends AbstractCalcRexAggImplementor
    {
        public void implementInitialize(
            RexCall call,
            CalcProgramBuilder.Register accumulatorRegister,
            RexToCalcTranslator translator)
        {
            // O s8; V 0; T; MOVE O0, C0;
            assert call.operands.length == 1;

            final CalcProgramBuilder.Register zeroReg =
                translator.builder.newLiteral(
                translator.getCalcRegisterDescriptor(call),
                integer0);
            CalcProgramBuilder.move.add(
                translator.builder,
                accumulatorRegister,
                zeroReg);
        }

        public void implementAdd(
            RexCall call,
            CalcProgramBuilder.Register accumulatorRegister,
            RexToCalcTranslator translator)
        {
            assert call.operands.length == 1;
            final RexNode operand = call.operands[0];

            CalcProgramBuilder.Register input = null;
            final RexNode[] inputExps = translator.getInputExprs();
            if (inputExps != null) {
                RexNode arg = inputExps[((RexInputRef) operand).getIndex()];
                input = translator.implementNode(arg);
            } else {
                input = translator.implementNode(operand);
            }

            // If the input can be null, then we check if the value is in fact
            // null and then skip adding it to the total. If, however, the value
            // cannot be null, we can simply perform the add operation.
            // One important point to remember here is that we should design
            // this such that multiple aggregations generate valid code too,
            // i.e., sum(col1), sum(col2) should generate correct code by
            // computing both the sums (so we cannot have return statements and
            // jump statements in sum(col1) should jump correctly to the next
            // instruction row of a subsequent call to this method needed for
            // sum(col2).
            // Here is the pseudo code:
            // ISNULL nullReg, col1    /* 0 */
            // JMPF @3, nullReg        /* 1 */
            // JMP @4                  /* 2 */
            // ADD O0, O0, col1        /* 3 */
            //                         /* 4 */
            // Note that a label '4' is created for a row that doesn't have any
            // instruction. This is critical both when there is sum(col2) or
            // simply a return statement.
            if (operand.getType().isNullable()) {
                int ordinal = translator.getNullRegisterOrdinal();
                CalcProgramBuilder.Register isNullReg = null;
                String wasNotNull = translator.newLabel();
                String next = translator.newLabel();
                if (ordinal == -1) {
                    isNullReg = translator.builder.
                            newLocal(CalcProgramBuilder.OpType.Bool, -1);
                    translator.setNullRegisterOrdinal(translator.builder.registerSets.getSet
                            (CalcProgramBuilder.RegisterSetType.LocalORDINAL).size()-1);
                } else {
                    isNullReg = translator.builder.getRegister
                            (ordinal, CalcProgramBuilder.RegisterSetType.Local);
                }
                CalcProgramBuilder.boolNativeIsNull.add(translator.builder, isNullReg,
                    translator.implementNode(operand));
                translator.builder.addLabelJumpFalse(wasNotNull, isNullReg);
                translator.builder.addLabelJump(next);
                translator.builder.addLabel(wasNotNull);
                CalcProgramBuilder.nativeAdd.add(
                                translator.builder,
                                accumulatorRegister,
                                accumulatorRegister,
                                input);
                translator.builder.addLabel(next);
            } else {
                CalcProgramBuilder.nativeAdd.add(
                    translator.builder,
                    accumulatorRegister,
                    accumulatorRegister,
                    input);
            }
        }

        public void implementDrop(
            RexCall call,
            CalcProgramBuilder.Register accumulatorRegister,
            RexToCalcTranslator translator)
        {
            assert call.operands.length == 1;
            final RexNode operand = call.operands[0];

            CalcProgramBuilder.Register input = null;
            final RexNode[] inputExps = translator.getInputExprs();
            if (inputExps != null) {
                RexNode arg = inputExps[((RexInputRef) operand).getIndex()];
                input = translator.implementNode(arg);
            } else {
                input = translator.implementNode(operand);
            }

            // Refer to the comments for implementAdd method above.
            // Here is the pseudo code:
            // ISNULL nullReg, col1    /* 0 */
            // JMPF @3, nullReg        /* 1 */
            // JMP @4                  /* 2 */
            // SUB O0, O0, col1        /* 3 */
            //                         /* 4 */
            if (operand.getType().isNullable() && inputExps == null) {
                int ordinal = translator.getNullRegisterOrdinal();
                CalcProgramBuilder.Register isNullReg = null;
                String wasNotNull = translator.newLabel();
                String next = translator.newLabel();
                if (ordinal == -1) {
                    isNullReg = translator.builder.
                            newLocal(CalcProgramBuilder.OpType.Bool, -1);
                    translator.setNullRegisterOrdinal(translator.builder.registerSets.getSet
                            (CalcProgramBuilder.RegisterSetType.LocalORDINAL).size()-1);
                } else {
                    isNullReg = translator.builder.getRegister
                            (ordinal, CalcProgramBuilder.RegisterSetType.Local);
                }
                CalcProgramBuilder.boolNativeIsNull.add(translator.builder, isNullReg,
                    translator.implementNode(operand));
                translator.builder.addLabelJumpFalse(wasNotNull, isNullReg);
                translator.builder.addLabelJump(next);
                translator.builder.addLabel(wasNotNull);
                CalcProgramBuilder.nativeMinus.add(
                                translator.builder,
                                accumulatorRegister,
                                accumulatorRegister,
                                input);
                translator.builder.addLabel(next);
            } else {
                CalcProgramBuilder.nativeMinus.add(
                    translator.builder,
                    accumulatorRegister,
                    accumulatorRegister,
                    input);
            }
        }
    }

    /**
     * Implementation of the <code>LAST_VALUE</code> aggregate function,
     * {@link SqlStdOperatorTable#lastValueOperator}.
     */
    private static class LastValueCalcRexImplementor
        extends AbstractCalcRexAggImplementor
    {
        public void implementInitialize(
            RexCall call,
            CalcProgramBuilder.Register accumulatorRegister,
            RexToCalcTranslator translator)
        {
            // Return null during the initialization.
            assert call.operands.length == 1;

            // TODO: Fix this... Supposed to be null instead of zero.
            final CalcProgramBuilder.Register zeroReg =
                translator.builder.newLiteral(
                translator.getCalcRegisterDescriptor(call),
                integer0);
            CalcProgramBuilder.move.add(
                            translator.builder,
                            accumulatorRegister,
                            zeroReg);
        }

        public void implementAdd(
            RexCall call,
            CalcProgramBuilder.Register accumulatorRegister,
            RexToCalcTranslator translator)
        {
            assert call.operands.length == 1;
            final RexNode operand = call.operands[0];

            CalcProgramBuilder.Register input = null;
            final RexNode[] inputExps = translator.getInputExprs();
            if (inputExps != null) {
                RexNode arg = inputExps[((RexInputRef) operand).getIndex()];
                input = translator.implementNode(arg);
            } else {
                input = translator.implementNode(operand);
            }

            // Just return the operand of the current row as the output.
            CalcProgramBuilder.move.add(
                            translator.builder,
                            accumulatorRegister,
                            input);
        }

        public void implementDrop(
            RexCall call,
            CalcProgramBuilder.Register accumulatorRegister,
            RexToCalcTranslator translator)
        {
            // Do nothing when a row is dropped.
            assert call.operands.length == 1;
        }
    }

    /**
     * Implements the internal {@link SqlStdOperatorTable#sliceOp $SLICE}
     * operator.
     */
    private static class SliceImplementor extends AbstractCalcRexImplementor
    {
        public CalcProgramBuilder.Register implement(
            RexCall call, RexToCalcTranslator translator)
        {
            throw new UnsupportedOperationException();
        }
    }
}


// End CalcRexImplementorTableImpl.java
