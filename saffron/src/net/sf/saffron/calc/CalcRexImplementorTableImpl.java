/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2004-2004 Disruptive Tech
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
package net.sf.saffron.calc;

import net.sf.saffron.sql.fun.SqlStdOperatorTable;
import net.sf.saffron.sql.fun.SqlTrimFunction;
import net.sf.saffron.sql.*;
import net.sf.saffron.sql.type.SqlTypeName;
import net.sf.saffron.rex.RexCall;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.rex.RexLiteral;
import net.sf.saffron.rex.RexKind;
import net.sf.saffron.util.Util;
import net.sf.saffron.util.MultiMap;
import net.sf.saffron.util.DoubleKeyMap;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.core.SaffronTypeFactory;

import java.util.HashMap;
import java.util.ArrayList;

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
    /**
     * Parent implementor table, may be null.
     */
    private final CalcRexImplementorTable parent;
    /**
     * Maps {@link SqlOperator} to {@link CalcRexImplementor}.
     */
    private final HashMap operatorImplementationMap = new HashMap();

    protected static final SqlStdOperatorTable opTab =
            SqlStdOperatorTable.std();
    private static final CalcRexImplementorTableImpl std =
            new CalcRexImplementorTableImpl(null).initStandard();
    /**
     * Collection of per-thread instances of {@link CalcRexImplementorTable}.
     * If a thread does not call {@link #setThreadInstance}, the default is
     * {@link #std}.
     */
    // REVIEW jhyde, 2004/4/12 The implementor table, like the operator table,
    //   should be a property of the session
    private static final ThreadLocal threadLocal = new ThreadLocal() {
        protected Object initialValue() {
            return std;
        }
    };

    /**
     * Creates an empty table which delegates to another table.
     *
     * @see {@link #std}
     */
    public CalcRexImplementorTableImpl(CalcRexImplementorTable parent) {
        this.parent = parent;
    }

    /**
     * Returns the table of implementations of all of the
     * standard SQL functions and operators.
     */
    public static CalcRexImplementorTable std() {
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
    public void register(SqlOperator op, CalcRexImplementor impl) {

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
    private void registerInstr(SqlOperator op,
            CalcProgramBuilder.InstructionDef instrDef) {

        Util.pre(instrDef != null, "instrDef != null");
        register(op, new InstrDefImplementor(instrDef));
    }

    // NOTE jvs 16-June-2004:  There's a reason I use the convention
    // implements CalcRexImplementorTable
    // which is that it keeps jalopy from supplying the missing
    // method comment, while not preventing javadoc from inheriting
    // the comment from super
    /** implement interface CalcRexImplementorTable */
    public CalcRexImplementor get(SqlOperator op) {
        CalcRexImplementor implementor = (CalcRexImplementor)
                operatorImplementationMap.get(op);
        if (implementor == null && parent != null) {
            implementor = parent.get(op);
        }
        return implementor;
    }

    /**
     * Abstract base class for classes which implement
     * {@link CalcRexImplementor}.
     */
    public abstract static class AbstractCalcRexImplementor
            implements CalcRexImplementor
    {
        public boolean canImplement(RexCall call) {
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
        InstrDefImplementor(CalcProgramBuilder.InstructionDef instr) {
            Util.pre(null != instr, "null != instr");
            this.instr = instr;
        }

        public CalcProgramBuilder.Register implement(RexCall call,
                RexToCalcTranslator translator) {

            ArrayList regList = makeRegList(translator, call);
            CalcProgramBuilder.Register[] regs = regListToRegArray(regList);

            instr.add(translator._builder, regs);
            return regs[0];
        }

        /**
         * Creates the list of registers which will be arguments to the
         * instruction call. i.e implment all the operands of the call and
         * create a result register for the call.
         *
         * <p>The 0th argument is assumed to hold the result of the call.
         */
        protected ArrayList makeRegList(RexToCalcTranslator translator,
                RexCall call) {

            ArrayList regList = implementOperands(call, translator);

            // the result is implemented last in order to avoid changing the tests
            regList.add(0, createResultRegister(translator, call));

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

        IsBoolImplementor(boolean boolType) {
            this.boolType = boolType;
        }

        public boolean canImplement(RexCall call) {
            return true;
        }

        public CalcProgramBuilder.Register implement(RexCall call,
                RexToCalcTranslator translator) {
            RexNode operand = call.operands[0];
            translator.implementNode(operand);


            if (operand.getType().isNullable()) {
                RexNode notNullCall = translator._rexBuilder.makeCall(
                        opTab.isNotNullOperator,
                        operand);
                RexNode eqCall = translator._rexBuilder.makeCall(
                        opTab.equalsOperator,
                        operand,
                        translator._rexBuilder.makeLiteral(boolType));
                res = translator._rexBuilder.makeCall(opTab.andOperator,
                        notNullCall,
                        eqCall);
            } else {
                res = translator._rexBuilder.makeCall(opTab.equalsOperator,
                        operand,
                        translator._rexBuilder.makeLiteral(boolType));
            }
            return translator.implementNode(res);
        }
    }

    /**
     * Implements "IS NOT TRUE" and "IS NOT FALSE" operators.
     */
    private static class IsNotBoolImplementor extends IsBoolImplementor
    {
        IsNotBoolImplementor(boolean boolType) {
            super(boolType);
        }

        public CalcProgramBuilder.Register implement(RexCall call, RexToCalcTranslator translator) {
            super.implement(call,translator);
            res = translator._rexBuilder.makeCall(opTab.notOperator,res);
            return translator.implementNode(res);
        }
    }

    /**
     * A Class that gets a specified operand of a call and
     * retrieves its charset name and add it as a vc literal to the program.
     * This of course assumes the operand is a chartype.
     * If this is not the case an assert is fired.
     */
    private static class AddCharSetNameInstrImplementor extends InstrDefImplementor
    {
        int operand;

        AddCharSetNameInstrImplementor(String extCall, int regCount, int operand) {
            super(new CalcProgramBuilder.ExtInstrDef(extCall, regCount));
            this.operand = operand;
        }

        /**
         * @pre call.operands[operand].getType().isCharType()
         */
        protected ArrayList makeRegList(RexToCalcTranslator translator,
                RexCall call) {

            Util.pre(call.operands[operand].getType().isCharType(),
                    "call.operands[operand].getType().isCharType()");

            ArrayList regList = super.makeRegList(translator, call);
            CalcProgramBuilder.Register charSetName =
                    translator._builder.newVarcharLiteral(
                            call.operands[operand].getType().getCharset().name());
            regList.add(charSetName);
            return regList;
        }
    }


    /**
     * Implements a call by invoking a given instruction.
     */
    private static class UsingInstrImplementor extends AbstractCalcRexImplementor
    {
        CalcProgramBuilder.InstructionDef instr;

        /**
         * @pre null != instr
         */
        UsingInstrImplementor(CalcProgramBuilder.InstructionDef instr) {
            Util.pre(null != instr,"null != instr");
            this.instr = instr;
        }


        public CalcProgramBuilder.Register implement(RexCall call,
            RexToCalcTranslator translator) {
            return implementUsingInstr(instr, translator, call);
        }
    }

    // helper methods

    /**
     * Converts a list of registers to an array.
     *
     * @pre regList != null
     * @post return != null
     */
    protected static CalcProgramBuilder.Register[] regListToRegArray(
            ArrayList regList) {
        Util.pre(regList != null, "regList != null");
        CalcProgramBuilder.Register[] regs =
                (CalcProgramBuilder.Register[]) regList.toArray(
                        new CalcProgramBuilder.Register[
                                regList.size()]);
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
            RexToCalcTranslator translator, RexCall call) {
        CalcProgramBuilder.RegisterDescriptor resultDesc =
                translator.getCalcRegisterDescriptor(call);
        CalcProgramBuilder.Register resultOfCall =
                 translator._builder.newLocal(resultDesc);
        return resultOfCall;
    }

    /**
     * Implements all operands to a call, and returns a list of the registers
     * which hold the results.
     *
     * @post return != null
     */
    protected static ArrayList implementOperands(RexCall call,
            RexToCalcTranslator translator) {
        return implementOperands(call, 0, call.operands.length, translator);
    }

    /**
     * Implements all operands to a call between start (inclusive) and
     * stop (exclusive), and returns a list of the registers which hold the
     * results.
     *
     * @post return != null
     */
    protected static ArrayList implementOperands(RexCall call,
            int start, int stop, RexToCalcTranslator translator) {
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
            RexCall call) {
        CalcProgramBuilder.Register[] regs =
                new CalcProgramBuilder.Register[call.operands.length + 1];

        for (int i = 0; i < call.operands.length; i++) {
            RexNode operand = call.operands[i];
            regs[i + 1] = translator.implementNode(operand);
        }

        regs[0] = createResultRegister(translator, call);

        instr.add(translator._builder, regs);
        return regs[0];
    }

    /**
     * Converts a binary call (two regs as operands) by converting the first
     * operand to type {@link CalcProgramBuilder.OpType#Int8} if needed and
     * then back again. Logically it will do something like<br>
     * t0 = type of first operand<br>
     * CAST(CALL(CAST(op0 as INT8), op1) as t0)<br>
     * If t0 already is INT8, the CALL is simply returned as is.
     */
    private static RexCall implementFirstOperandWithInt8(
                                RexCall call,
                                RexToCalcTranslator translator,
                                RexNode typeNode,
                                int i,
                                boolean castBack) {

        CalcProgramBuilder.RegisterDescriptor rd =
            translator.getCalcRegisterDescriptor(typeNode);
        if (rd.getType().isExact() &&
            !rd.getType().equals(CalcProgramBuilder.OpType.Int8)) {

            SaffronType oldType = typeNode.getType();
            SaffronTypeFactory fac =
                translator._rexBuilder.getTypeFactory();
            //todo do a reverse lookup on OpType.Int8 instead
            SaffronType int8 = fac.createSqlType(SqlTypeName.Bigint);
            RexNode castCall1 =
                translator._rexBuilder.makeCast(int8, call.operands[i]);

            RexNode newCall;
            if (opTab.castFunc.equals(call.op)) {
                newCall = translator._rexBuilder.makeCast(call.getType(), castCall1);
            } else {
                RexNode[] args = new RexNode[call.operands.length];
                System.arraycopy(call.operands, 0, args, 0, call.operands.length);
                args[i] = castCall1;
                newCall = translator._rexBuilder.makeCall(call.op,args);
            }

            if (castBack) {
                newCall = translator._rexBuilder.makeCast(oldType, newCall);
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
                                boolean castBack) {

        //TODO this method needs cleanup, it contains redundant code
        CalcProgramBuilder.RegisterDescriptor rd =
            translator.getCalcRegisterDescriptor(typeNode);
        if (rd.getType().isApprox() &&
            !rd.getType().equals(CalcProgramBuilder.OpType.Double)) {

            SaffronType oldType = typeNode.getType();
            SaffronTypeFactory fac =
                translator._rexBuilder.getTypeFactory();
            //todo do a reverse lookup on OpType.Double instead
            SaffronType db = fac.createSqlType(SqlTypeName.Double);
            RexNode castCall1 =
                translator._rexBuilder.makeCast(db, call.operands[i]);

            RexNode newCall;
            if (opTab.castFunc.equals(call.op)) {
                newCall = translator._rexBuilder.makeCast(call.getType(), castCall1);
            } else {
                RexNode[] args = new RexNode[call.operands.length];
                System.arraycopy(call.operands, 0, args, 0, call.operands.length);
                args[i] = castCall1;
                newCall = translator._rexBuilder.makeCall(call.op,args);
            }

            if (castBack) {
                newCall = translator._rexBuilder.makeCast(oldType, newCall);
            }
            return (RexCall) newCall;
        }
        return call;
    }

    public static CalcRexImplementorTable threadInstance() {
        return (CalcRexImplementorTable) threadLocal.get();
    }

    public static void setThreadInstance(CalcRexImplementorTable table) {
        threadLocal.set(table);
    }

    /**
     * Implementor that will convert a {@link RexCall}'s operands to
     * approx DOUBLE if needed
     */
    private static class MakeOperandsDoubleImplementor extends InstrDefImplementor {

        MakeOperandsDoubleImplementor(CalcProgramBuilder.InstructionDef instr) {
            super(instr);
        }

        public CalcProgramBuilder.Register implement(RexCall call,
            RexToCalcTranslator translator) {
            call = (RexCall) call.clone();
            for (int i = 0; i < call.operands.length; i++) {
                RexNode operand = call.operands[i];
                if (!operand.getType().getSqlTypeName().equals(SqlTypeName.Double)) {
                    SaffronType oldType = operand.getType();
                    SaffronTypeFactory fac =
                        translator._rexBuilder.getTypeFactory();
                    //todo do a reverse lookup on OpType.Double instead
                    SaffronType doubleType = fac.createSqlType(SqlTypeName.Double);
                    doubleType = fac.createTypeWithNullability(
                        doubleType, oldType.isNullable());
                    RexNode castCall =
                        translator._rexBuilder.makeCast(
                            doubleType, call.operands[i]);
                    call.operands[i] = castCall;
                }
            }
            assert(0 != call.operands.length);
            return super.implement(call, translator);
        }
    }

    /**
     * Implementor for CAST operator.
     */
    private static class CastImplementor extends AbstractCalcRexImplementor {
        private static DoubleKeyMap dm;


        static {
            dm = new DoubleKeyMap();
            dm.setEnforceUniqueness(true);

            Object[] exactTypes = { SqlTypeName.Tinyint, SqlTypeName.Smallint,
                                    SqlTypeName.Integer, SqlTypeName.Bigint};
            Object[] approxTypes = { SqlTypeName.Float,
                                     SqlTypeName.Real,
                                     SqlTypeName.Double};
            Object[] timeTypes = { SqlTypeName.Date,
                                   SqlTypeName.Time,
                                   SqlTypeName.Timestamp};
            Object[] charTypes = { SqlTypeName.Char,
                                   SqlTypeName.Varchar};
            //~ Exact To ... -------------------
            dm.put(exactTypes, exactTypes,
                new UsingInstrImplementor(CalcProgramBuilder.Cast));
            dm.put(exactTypes, approxTypes,
                new UsingInstrImplementor(CalcProgramBuilder.Cast));
            dm.put(timeTypes, SqlTypeName.Bigint,
                new UsingInstrImplementor(ExtInstructionDefTable.castDateToMillis));
            dm.put(exactTypes, charTypes,
                new UsingInstrImplementor(ExtInstructionDefTable.castA) {
                    public CalcProgramBuilder.Register implement(RexCall call,
                        RexToCalcTranslator translator) {
                        RexCall newCall = implementFirstOperandWithInt8(call, translator,call.operands[0],0,false);
                        if (newCall.equals(call)) {
                            return super.implement(call, translator);
                        }
                        return translator.implementNode(newCall);

                    }
                });

            //~ Approx To ... -------------------
            dm.put(approxTypes, approxTypes,
                new UsingInstrImplementor(CalcProgramBuilder.Cast));
            dm.put(approxTypes, charTypes,
                new UsingInstrImplementor(ExtInstructionDefTable.castA) {
                    public CalcProgramBuilder.Register implement(RexCall call,
                        RexToCalcTranslator translator) {
                        RexCall newCall = implementFirstOperandWithDouble(call, translator,call.operands[0],0,false);
                        if (newCall.equals(call)) {
                           return super.implement(call, translator);
                        }
                        return translator.implementNode(newCall);

                    }
                });
            dm.put(approxTypes, exactTypes,
                new AbstractCalcRexImplementor() {
                public CalcProgramBuilder.Register implement(RexCall call,
                    RexToCalcTranslator translator) {
                    assert(call.isA(RexKind.Cast));
                    CalcProgramBuilder.Register beforeRound =
                        translator.implementNode(call.operands[0]);
                    CalcProgramBuilder.RegisterDescriptor regDesc =
                        translator.getCalcRegisterDescriptor(call.operands[0]);
                    CalcProgramBuilder.Register afterRound =
                        translator._builder.newLocal(regDesc);
                    CalcProgramBuilder.Round.add(translator._builder,
                        new CalcProgramBuilder.Register[]{afterRound, beforeRound});
                    CalcProgramBuilder.Register res =
                        createResultRegister(translator, call);
                    CalcProgramBuilder.Cast.add(translator._builder,
                        new CalcProgramBuilder.Register[]{res, afterRound});
                    return res;
                }
            });

            //~ Date To ... -------------------
            dm.put(charTypes, SqlTypeName.Date,
                new UsingInstrImplementor(ExtInstructionDefTable.castStrAToDate));
            dm.put(SqlTypeName.Date, charTypes,
                new UsingInstrImplementor(ExtInstructionDefTable.castDateToStr));

            //~ Time To ... -------------------
            dm.put(charTypes, SqlTypeName.Time,
                new UsingInstrImplementor(ExtInstructionDefTable.castStrAToTime));
            dm.put(SqlTypeName.Time, charTypes,
                new UsingInstrImplementor(ExtInstructionDefTable.castTimeToStr));

            //~ TimeStamp To ... -------------------
            dm.put(charTypes, SqlTypeName.Timestamp,
                new UsingInstrImplementor(ExtInstructionDefTable.castStrAToTimestamp));
            dm.put(SqlTypeName.Timestamp, charTypes,
                new UsingInstrImplementor(ExtInstructionDefTable.castTimestampToStr));

            //~ CharTypes To ... -------------------
            dm.put(charTypes, exactTypes,
                new UsingInstrImplementor(ExtInstructionDefTable.castA) {
                    public CalcProgramBuilder.Register implement(RexCall call,
                        RexToCalcTranslator translator) {
                        RexCall newCall = implementFirstOperandWithInt8(call, translator,call,0,false);
                        if (newCall.equals(call)) {
                           return super.implement(call, translator);
                        }
                        return translator.implementNode(newCall);

                    }
                });
            dm.put(charTypes, approxTypes,
                new UsingInstrImplementor(ExtInstructionDefTable.castA) {
                    public CalcProgramBuilder.Register implement(RexCall call,
                        RexToCalcTranslator translator) {
                        RexCall newCall = implementFirstOperandWithDouble(call, translator,call,0,false);
                        if (newCall.equals(call)) {
                           return super.implement(call, translator);
                        }
                        return translator.implementNode(newCall);

                    }
                });

            dm.put(charTypes, charTypes, new AbstractCalcRexImplementor(){
                //REVIEW wael: Kinkoi is working on a char<->char cast instr in the calculator
                //this code below was just a quick ramen soup meal to get some basic queries working
                public CalcProgramBuilder.Register implement(RexCall call,
                    RexToCalcTranslator translator) {
                    CalcProgramBuilder.RegisterDescriptor resultDesc =
                    translator.getCalcRegisterDescriptor(call);

                    RexNode op = call.operands[0];
                    CalcProgramBuilder.Register opReg = translator.implementNode(op);
                    CalcProgramBuilder.RegisterDescriptor argDesc =
                        translator.getCalcRegisterDescriptor(op);

                    CalcProgramBuilder.Register resReg =
                        translator._builder.newLocal(resultDesc);

                    if (resultDesc.getBytes() < argDesc.getBytes()) {
                        CalcProgramBuilder.Register oneReg =
                            translator._builder.newInt4Literal(1);
                        CalcProgramBuilder.Register nReg =
                            translator._builder.
                            newInt4Literal(resultDesc.getBytes());
                        CalcProgramBuilder.Register[] regs =
                            new CalcProgramBuilder.Register[]
                            {resReg, opReg, oneReg, nReg};
                        ExtInstructionDefTable.substring.
                            add(translator._builder, regs);
                    } else {
                        CalcProgramBuilder.move.
                            add(translator._builder, resReg, opReg);
                    }
                    return resReg;
                }
            });
        }

        public CalcProgramBuilder.Register implement(RexCall call,
                RexToCalcTranslator translator) {

            Util.pre(call.operands.length == 1, "call.operands.length == 1");

            if (RexLiteral.isNullLiteral(call.operands[0])) {
                CalcProgramBuilder.RegisterDescriptor resultDesc =
                    translator.getCalcRegisterDescriptor(call);
                // If the type is one which requires an explicit storage
                // specification, tell it we need 0 bytes;
                // otherwise keep it at -1.
                if (resultDesc.getBytes() >= 0) {
                    resultDesc = new CalcProgramBuilder.RegisterDescriptor(
                            resultDesc.getType(), 0);
                }

                return translator._builder.newLiteral(resultDesc, null);
            }

            // Figure out the source and destination types.
            SaffronType fromType = call.operands[0].getType();
            SqlTypeName fromTypeName = fromType.getSqlTypeName();
            SaffronType toType = call.getType();
            SqlTypeName toTypeName = toType.getSqlTypeName();

            CalcRexImplementor implentor =
                (CalcRexImplementor) dm.get(fromTypeName, toTypeName);
            if (null != implentor) {
                return implentor.implement(call, translator);
            }

            if (translator.lhsTypeIsNullableRHSType(toType, fromType)) {
                RexNode op = call.operands[0];
                return translator.implementNode(op);
            }

            throw Util.needToImplement("Cast from '"
                    + fromType.toString() + "' to '"+toType.toString() + "'");
        }
    }

    /**
     * Makes all types the same before calling a given
     * {@link CalcProgramBuilder.InstructionDef instruction}. The way to make the
     * types the same is by inserting appropiate call to various cast functions
     * depending on the types. The "biggest" (least restrictive) type will
     * always win and other types will be conveted into that bigger type.
     * For example.
     * In the expression <code>1.0+2</code> the '+' instruction is potentially called
     * with types <code>(DOUBLE) + (INTEGER)</code> which is illegal.
     * Therefore the expression's implementation will logically end looking something like
     * <code>1.0 + CAST(2 AS DOUBLE)</code>
     * LIMITATION: For now only Binary operators are suppored with numeric types
     * If any operand is of any other type than a numeric one, the base class
     * {@link InstrDefImplementor#implement implementation} will be called
     */
    private static class BinaryNumericMakeSametypeImplementor extends InstrDefImplementor {

        public BinaryNumericMakeSametypeImplementor(CalcProgramBuilder.InstructionDef instr) {
            super(instr);
        }

        private int getRestrictiveness(CalcProgramBuilder.RegisterDescriptor rd) {
            switch (rd.getType().ordinal_) {
            case CalcProgramBuilder.OpType.Uint1_ordinal: return 10;
            case CalcProgramBuilder.OpType.Int1_ordinal:  return 20;
            case CalcProgramBuilder.OpType.Uint2_ordinal: return 30;
            case CalcProgramBuilder.OpType.Int2_ordinal:  return 40;
            case CalcProgramBuilder.OpType.Uint4_ordinal: return 50;
            case CalcProgramBuilder.OpType.Int4_ordinal:  return 60;
            case CalcProgramBuilder.OpType.Uint8_ordinal: return 70;
            case CalcProgramBuilder.OpType.Int8_ordinal:  return 80;

            case CalcProgramBuilder.OpType.Real_ordinal:  return 1000;
            case CalcProgramBuilder.OpType.Double_ordinal:return 1010;
            }

            assert(false);
            return -1;
        }

        public CalcProgramBuilder.Register implement(RexCall call,
            RexToCalcTranslator translator) {
            assert(2 == call.operands.length);
            ArrayList regs = implementOperands(call, translator);
            CalcProgramBuilder.RegisterDescriptor rd0 =
                translator.getCalcRegisterDescriptor(call.operands[0]);
            CalcProgramBuilder.RegisterDescriptor rd1 =
                translator.getCalcRegisterDescriptor(call.operands[1]);

            if (!rd0.getType().isNumeric() || !rd1.getType().isNumeric()) {
                return super.implement(call, translator);
            }

            int d = getRestrictiveness(rd0) - getRestrictiveness(rd1);
            if (d != 0) {
                int small;
                if (d > 0) {
                    small = 1;
                } else {
                    small = 0;
                }

                RexNode castCall = translator._rexBuilder.makeCast(
                    call.operands[(small+1)%2].getType(), call.operands[small]);
                CalcProgramBuilder.Register newOp =
                    translator.implementNode(castCall);
                regs.set(small, newOp);
            }

            CalcProgramBuilder.Register res =
                createResultRegister(translator, call);
            regs.add(0, res);

            instr.add(translator._builder, regs);
            return res;
        }
    }

    /**
     * Implementor for CASE operator.
     */
    private static class CaseImplementor extends AbstractCalcRexImplementor {
        public CalcProgramBuilder.Register implement(RexCall call,
                RexToCalcTranslator translator) {
            Util.pre(call.operands.length>1,"call.operands.length>1");
            Util.pre((call.operands.length&1)==1,"(call.operands.length&1)==1");
            CalcProgramBuilder.Register resultOfCall =
                    createResultRegister(translator,call);
            String endOfCase = translator.newLabel();
            String next;
            boolean elseClauseOptimizedAway=false;
            for (int i = 0;i < call.operands.length - 1; i += 2) {
                next = translator.newLabel();
                CalcProgramBuilder.Register compareResult =
                        translator.implementNode(call.operands[i]);
                assert(compareResult.getOpType().equals(CalcProgramBuilder.OpType.Bool));
                if (!compareResult.getRegisterType().equals(CalcProgramBuilder.RegisterSetType.Literal)) {
                    translator._builder.addLabelJumpFalse(next, compareResult);
                    // todo optimize away null check if type known to be non null
                    // same applies the other way (if we have a null literal or a cast(null as xxx))
                    translator._builder.addLabelJumpNull(next, compareResult);
                    CalcProgramBuilder.move.add(
                        translator._builder,
                        resultOfCall,
                            translator.implementNode(call.operands[i + 1]));
                    translator._builder.addLabelJump(endOfCase);
                    translator._builder.addLabel(next);
                } else {
                    // we can do some optimizations
                    Boolean val = (Boolean) compareResult.getValue();
                    if (val.booleanValue()) {
                        CalcProgramBuilder.move.add(
                                translator._builder,
                                resultOfCall,
                                translator.implementNode(call.operands[i + 1]));
                        if (i!=0) {
                            translator._builder.addLabelJump(endOfCase);
                        }
                        translator._builder.addLabel(next);
                        elseClauseOptimizedAway=true;
                        break;
                    }
                    // else we dont need to do anything
                }

            }

            if (!elseClauseOptimizedAway) {
                int elseIndex = call.operands.length - 1;
                CalcProgramBuilder.move.add(
                        translator._builder,resultOfCall,
                        translator.implementNode(call.operands[elseIndex]));
            }
            translator._builder.addLabel(endOfCase); //this assumes that more instructions will follow
            return resultOfCall;
        }
    }

    /**
     * Implements the identity operator.
     *
     * <p>The prefix plus operator uses this implementor, because "+ x" is
     * always the same as "x".
     */
    private static class IdentityImplementor implements CalcRexImplementor {
        public CalcProgramBuilder.Register implement(RexCall call,
                RexToCalcTranslator translator) {
            return translator.implementNode(call.operands[0]);
        }

        public boolean canImplement(RexCall call) {
            return true;
        }
    }

    /**
     * Implements the TRIM function.
     */
    private static class TrimImplementor extends InstrDefImplementor {
        public TrimImplementor() {
            super(ExtInstructionDefTable.trim);
        }

        protected ArrayList makeRegList(RexToCalcTranslator translator,
                RexCall call) {
            ArrayList regList = new ArrayList();

            CalcProgramBuilder.Register resultOfCall =
                    createResultRegister(translator, call);
            assert call.operands[0] instanceof RexLiteral :
                    call.operands[0];
            final RexLiteral literal = (RexLiteral)call.operands[0];
            SqlTrimFunction.Flag flag = (SqlTrimFunction.Flag)
                    literal.getValue();

            regList.add(resultOfCall);
            regList.add(translator.implementNode(call.operands[2])); //str to trim from
            regList.add(translator.implementNode(call.operands[1])); //trim char
            regList.add(translator._builder.newInt4Literal(flag._left));
            regList.add(translator._builder.newInt4Literal(flag._right));

            return regList;
        }
    }

    private static class ConcatImplementor extends AbstractCalcRexImplementor {

        public CalcProgramBuilder.Register implement(RexCall call,
                RexToCalcTranslator translator) {
            assert(!translator.containsResult(call)); //avoid reentrancy
            CalcProgramBuilder.Register resultReg =
                    createResultRegister(translator,call);
            return implement(call, translator, resultReg);
        }

        private CalcProgramBuilder.Register implement(RexCall call,
                RexToCalcTranslator translator,
                CalcProgramBuilder.Register resultRegister) {

            assert(2==call.operands.length);
            ArrayList regList = new ArrayList();
            regList.add(resultRegister);
            if ((!(call.operands[0] instanceof RexCall) ||
                    !((RexCall)call.operands[0]).op.equals(opTab.concatOperator)))
            {
                regList.add(translator.implementNode(call.operands[0]));
            } else {
                implement((RexCall)call.operands[0], translator, resultRegister);
            }

            regList.add(translator.implementNode(call.operands[1]));
            ExtInstructionDefTable.concat.add(translator._builder,regList);
            return resultRegister;
        }
    }

    /**
     * Registers the standard set of functions.
     */
    private CalcRexImplementorTableImpl initStandard() {

        //~ ABS ---------------
        register(opTab.absFunc, new InstrDefImplementor(ExtInstructionDefTable.abs) {

            public CalcProgramBuilder.Register implement(RexCall call,
                RexToCalcTranslator translator) {

                RexCall newCall =
                    implementFirstOperandWithInt8(call, translator, call.operands[0],0,true);
                if (newCall.equals(call)) {
                    return super.implement(call, translator);
                }
                return translator.implementNode(newCall);
            }
        });
        //~ ADD ---------------
        register(opTab.plusOperator,
            new BinaryNumericMakeSametypeImplementor(CalcProgramBuilder.nativeAdd));
        //~ AND ---------------
        registerInstr(opTab.andOperator, CalcProgramBuilder.integralNativeAnd);
        //~ DIV ---------------
        register(opTab.divideOperator,
            new BinaryNumericMakeSametypeImplementor(CalcProgramBuilder.nativeDiv));
        //~ CASE ---------------
        register(opTab.caseOperator, new CaseImplementor());
        //~ CAST  ---------
        register(opTab.castFunc, new CastImplementor());
        //~ CHARACTER_LENGTH ---------
        if (false) {
            // TODO eventually need an extra argument for charset, in which
            // case we will use the following code
            register(opTab.characterLengthFunc,
                    new AddCharSetNameInstrImplementor("CHAR_LENGTH", -1, 3));
        } else {
            registerInstr(opTab.characterLengthFunc,
                    ExtInstructionDefTable.charLength);
        }
        // CHAR_LENGTH shares CHARACTER_LENGTH's implementation.
        // TODO: Combine the CHAR_LENGTH and CHARACTER_LENGTH at the
        //   RexNode level (they should remain separate functions at the
        //   SqlNode level).
        register(opTab.charLengthFunc, get(opTab.characterLengthFunc));
        //~ CONCAT ---------------
        register(opTab.concatOperator, new ConcatImplementor());
        //~ EQUAL ---------------
        register(opTab.equalsOperator,
            new BinaryNumericMakeSametypeImplementor(
                CalcProgramBuilder.boolNativeEqual));
        //~ GREATER THAN ---------------
        register(opTab.greaterThanOperator,
            new BinaryNumericMakeSametypeImplementor(
                CalcProgramBuilder.boolNativeGreaterThan));
        //~ GREATER THAN OR EQUAL ---------------
        register(opTab.greaterThanOrEqualOperator,
            new BinaryNumericMakeSametypeImplementor(
                CalcProgramBuilder.boolNativeGreaterOrEqualThan));
        //~ IS NULL---------------
        registerInstr(opTab.isNullOperator, CalcProgramBuilder.boolNativeIsNull);
        //~ IS NOT NULL---------------
        registerInstr(opTab.isNotNullOperator, CalcProgramBuilder.boolNativeIsNotNull);
        //~ IS TRUE ---------------
        register(opTab.isTrueOperator, new IsBoolImplementor(true));
        //~ IS NOT TRUE ---------------
        register(opTab.isNotTrueOperator, new IsNotBoolImplementor(true));
        //~ IS FALSE`---------------
        register(opTab.isFalseOperator, new IsBoolImplementor(false));
        //~ IS NOT FALSE ---------------
        register(opTab.isNotFalseOperator, new IsNotBoolImplementor(false));
        //~ LESS THAN ---------------
        register(opTab.lessThanOperator,
            new BinaryNumericMakeSametypeImplementor(
                CalcProgramBuilder.boolNativeLessThan));
        //~ LESS THAN OR EQUAL ---------------
        register(opTab.lessThanOrEqualOperator,
            new BinaryNumericMakeSametypeImplementor(
                CalcProgramBuilder.boolNativeLessOrEqualThan));
        //~ LIKE ---------------
        registerInstr(opTab.likeOperator, ExtInstructionDefTable.like);
        //~ LOG ---------------
        register(opTab.lnFunc,
            new MakeOperandsDoubleImplementor(ExtInstructionDefTable.log));
        register(opTab.logFunc,
            new MakeOperandsDoubleImplementor(ExtInstructionDefTable.log10));
        //~ UPPER ---------
        // TODO: need to know charset aswell. When ready,
        // use same construct as with CHAR_LENGTH above
        registerInstr(opTab.lowerFunc, ExtInstructionDefTable.lower);
        //~ MINUS ---------------
        register(opTab.minusOperator,
            new BinaryNumericMakeSametypeImplementor(
                CalcProgramBuilder.nativeMinus));
        //~ MOD maps directly to a built in instruction---------------
        register(opTab.modFunc,
            new BinaryNumericMakeSametypeImplementor(
                CalcProgramBuilder.integralNativeMod));
        //~ MUL ---------------
        register(opTab.multiplyOperator,
            new BinaryNumericMakeSametypeImplementor(
                CalcProgramBuilder.integralNativeMul));
        //~ NOT EQUAL ---------------
        register(opTab.notEqualsOperator,
            new BinaryNumericMakeSametypeImplementor(
                CalcProgramBuilder.boolNativeNotEqual));
        //~ NOT  ---------------
        registerInstr(opTab.notOperator, CalcProgramBuilder.boolNot);
        // todo: optimization. If using not in front of IS NULL,
        // create a call to the calc instruction ISNOTNULL
        //~ OR ---------------
        registerInstr(opTab.orOperator, CalcProgramBuilder.boolOr);
        //~ OVERLAY ---------------
        registerInstr(opTab.overlayFunc, ExtInstructionDefTable.overlay);
        //~ POSITION ---------------
        registerInstr(opTab.positionFunc, ExtInstructionDefTable.position);
        //~ POWER ---------------
        register(opTab.powFunc,
            new MakeOperandsDoubleImplementor(ExtInstructionDefTable.pow));
        //~ PREFIX MINUS ---------------
        registerInstr(opTab.prefixMinusOperator, CalcProgramBuilder.nativeNeg);
        //~ PREFIX PLUS---------------
        register(opTab.prefixPlusOperator, new IdentityImplementor());
        //~ SIMILAR ---------------
        registerInstr(opTab.similarOperator, ExtInstructionDefTable.similar);
        //~ SUBSTRING ---------------
        registerInstr(opTab.substringFunc,ExtInstructionDefTable.substring);
        //~ TRIM ---------------
        register(opTab.trimFunc, new TrimImplementor());
        //~ UPPER ---------
        // TODO: need to know charset aswell. When ready,
        // use same construct as with CHAR_LENGTH above
        registerInstr(opTab.upperFunc, ExtInstructionDefTable.upper);

        registerInstr(opTab.localTimeFunc,ExtInstructionDefTable.localTime);
        registerInstr(opTab.localTimestampFunc,
                    ExtInstructionDefTable.localTimestamp);

        return this;
    }
}

// End CalcRexImplementorTableImpl.java

