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
import net.sf.saffron.rex.RexCall;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.rex.RexLiteral;
import net.sf.saffron.util.Util;

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
class CalcRexImplementorTableImpl implements CalcRexImplementorTable
{
    private final HashMap operatorImplementationMap = new HashMap();

    /**
     * Creates an empty table.
     *
     * You probably want to call the public method {@link #std}.
     */
    protected CalcRexImplementorTableImpl() {
    }

    /**
     * Creates a table and initializes it with implementations of all of the
     * standard SQL functions and operators.
     */
    public static CalcRexImplementorTable std(SqlStdOperatorTable opTab) {

        final CalcRexImplementorTableImpl
                table = new CalcRexImplementorTableImpl();
        table.initStandard(opTab);
        return table;
    }

    /**
     * Registers an operator and its implementor.
     *
     * @pre op != null
     * @pre impl != null
     * @pre !operatorImplementationMap.containsKey(op)
     */
    void register(SqlOperator op, CalcRexImplementor impl) {

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
        register(op, new InstrImplementor(instrDef));
    }

    // NOTE jvs 16-June-2004:  There's a reason I use the convention
    // implements CalcRexImplementorTable
    // which is that it keeps jalopy from supplying the missing
    // method comment, while not preventing javadoc from inheriting
    // the comment from super
    /** implement interface CalcRexImplementorTable */
    public CalcRexImplementor get(SqlOperator op) {
        return (CalcRexImplementor) operatorImplementationMap.get(op);
    }

    /**
     * Generic implementor, which implements an operator by generating a call
     * to a given instruction.
     *
     * <p>If you need to tweak the arguments to the instruction, you can
     * override {@link #makeRegList}.
     */
    public static class InstrImplementor implements CalcRexImplementor
    {
        /**
         * The instruction with which to implement this operator.
         */
        private final CalcProgramBuilder.InstructionDef instr;
        /**
         * Creates an instruction implementor
         *
         * @param instr The instruction with which to implement this operator
         */
        InstrImplementor(CalcProgramBuilder.InstructionDef instr) {
            this.instr = instr;
        }

        public boolean canImplement(RexCall call) {
            return true;
        }

        public void implement(RexCall call,
                RexToCalcTranslator translator) {

            ArrayList regList = makeRegList(translator, call);
            CalcProgramBuilder.Register[] regs = regListToRegArray(regList);

            instr.add(translator._builder, regs);
            final CalcProgramBuilder.Register resultReg = regs[0];
            translator.setResult(call, resultReg);
        }

        protected CalcProgramBuilder.Register[] regListToRegArray(ArrayList regList) {
            CalcProgramBuilder.Register[] regs =
                    (CalcProgramBuilder.Register[]) regList.toArray(
                            new CalcProgramBuilder.Register[
                                    regList.size()]);
            return regs;
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

        protected CalcProgramBuilder.Register createResultRegister(
                RexToCalcTranslator translator, RexCall call) {
            CalcProgramBuilder.RegisterDescriptor resultDesc =
                    translator.getCalcRegisterDescriptor(call);
            CalcProgramBuilder.Register resultOfCall =
                     translator._builder.newLocal(resultDesc);
            return resultOfCall;
        }

        protected ArrayList implementOperands(RexCall call, RexToCalcTranslator translator) {
            return implementOperands(call, 0, call.operands.length, translator);
        }

        protected ArrayList implementOperands(RexCall call, int start, int stop,
                RexToCalcTranslator translator) {
            ArrayList regList = new ArrayList();
            for (int i = start; i < stop; i++) {
                RexNode operand = call.operands[i];
                translator.implementNode(operand);
                regList.add(translator.getResult(operand));
            }
            return regList;
        }
    }

    /**
     * A Class that implements IS TRUE and IS FALSE
     */
    private static class IsBoolInstrImplementor implements CalcRexImplementor
    {
        private boolean boolType;
        protected final static SqlStdOperatorTable opTab = SqlStdOperatorTable.std();
        protected RexNode res;

        IsBoolInstrImplementor(boolean boolType) {
            this.boolType = boolType;
        }

        public boolean canImplement(RexCall call) {
            return true;
        }

        public void implement(RexCall call, RexToCalcTranslator translator) {
            translator.implementNode(call.operands[0]);

            RexNode operand = call.operands[0];

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
            translator.implementNode(res);
            translator.setResult(call, translator.getResult(res));
        }
    }

    /**
     * A Class that implements IS NOT TRUE and IS NOT FALSE
     */
    private static class IsNotBoolInstrImplementor extends IsBoolInstrImplementor
    {
        IsNotBoolInstrImplementor(boolean boolType) {
            super(boolType);
        }

        public void implement(RexCall call, RexToCalcTranslator translator) {
            super.implement(call,translator);
            res = translator._rexBuilder.makeCall(opTab.notOperator,res);
            translator.implementNode(res);
            translator.setResult(call, translator.getResult(res));
        }
    }

    /**
     * A Class that gets a specified operand of a call and
     * retrives its charset name and add it as a vc literal to the program.
     * This of course asumes the operand is a chartype. If this is not the case
     * an assert is fired.
     */
    private static class AddCharSetNameInstrImplementor extends InstrImplementor
    {
        int operand;

        AddCharSetNameInstrImplementor(String extCall, int regCount, int operand) {
            super(new CalcProgramBuilder.ExtInstrDef(extCall, regCount));
            this.operand = operand;
        }

        /**
         * @pre call.operands[operand].getType().isCharType()==true
         */
        protected ArrayList makeRegList(RexToCalcTranslator translator,
                RexCall call) {

            Util.pre(call.operands[operand].getType().isCharType(),
                    "call.operands[operand].getType().isCharType()==true");

            ArrayList regList = super.makeRegList(translator, call);
            CalcProgramBuilder.Register charSetName =
                    translator._builder.newVarcharLiteral(
                            call.operands[operand].getType().getCharset().name());
            regList.add(charSetName);
            return regList;
        }
    }

    /**
     * Registers the standard set of functions.
     * @param opTab
     */
    private void initStandard(final SqlStdOperatorTable opTab) {

        //~ ADD ---------------
        registerInstr(opTab.plusOperator, CalcProgramBuilder.nativeAdd);
        //~ AND ---------------
        registerInstr(opTab.andOperator, CalcProgramBuilder.integralNativeAnd);
        //~ DIV ---------------
        registerInstr(opTab.divideOperator, CalcProgramBuilder.nativeDiv);
        //~ CASE ---------------
        register(opTab.caseOperator,
                new InstrImplementor(null) {
                    protected ArrayList makeRegList(RexToCalcTranslator translator,
                            RexCall call) {
                        translator.implementNode(call.operands[0]);
                        ArrayList regList = new ArrayList(1);
                        regList.add(translator.getResult(call.operands[0]));
                        return regList;
                    }

                    public void implement(RexCall call,
                            RexToCalcTranslator translator) {
                        Util.pre(call.operands.length>1,"call.operands.length>1");
                        Util.pre((call.operands.length&1)==1,"(call.operands.length&1)==1");
                        CalcProgramBuilder.Register resultOfCall=
                                createResultRegister(translator,call);
                        String endOfCase = translator.newLabel();
                        String next;
                        for(int i=0;i<call.operands.length-1;i+=2) {
                            next = translator.newLabel();
                            translator.implementNode(call.operands[i]);
                            CalcProgramBuilder.Register compareResult =
                                    translator.getResult(call.operands[i]);
                            translator._builder.addLabelJumpFalse(next,
                                    compareResult);
                            translator._builder.addLabelJumpNull(next,
                                    compareResult);
                            translator.implementNode(call.operands[i+1]);
                            CalcProgramBuilder.move.add(
                                    translator._builder,
                                    resultOfCall,
                                    translator.getResult(call.operands[i+1]));
                            translator._builder.addLabelJump(endOfCase);
                            translator._builder.addLabel(next);
                        }
                        int elseIndex = call.operands.length-1;
                        translator.implementNode(call.operands[elseIndex]);
                        CalcProgramBuilder.move.add(
                                translator._builder,resultOfCall,
                                translator.getResult(call.operands[elseIndex]));
                        translator._builder.addLabel(endOfCase); //this assumes that more instructions will follow
                        translator.setResult(call,resultOfCall);
                    }
                });
        //~ CHARACTER_LENGTH ---------
        // TODO eventually need an extra argument for charset.
        // use AddCharSetNameInstrImplementor("CHAR_LENGTH", 3) instead of ExtInstrDef
        registerInstr(opTab.characterLengthFunc,
                ExtInstructionDefTable.charLength);
        // CHAR_LENGTH shares CHARACTER_LENGTH's implementation.
        // TODO: Combine the CHAR_LENGTH and CHARACTER_LENGTH at the
        //   RexNode level (they should remain separate functions at the
        //   SqlNode level).
        register(opTab.charLengthFunc, get(opTab.characterLengthFunc));
        //~ CONCAT ---------------
        register(opTab.concatOperator,
                new InstrImplementor(null) {
                    public void implement(RexCall call,
                            RexToCalcTranslator translator) {
                        ArrayList regList = implementOperands(call,translator);
                        //optimization, if the first operand to this concat is
                        //another concat use strCatA2
                        if ((!(call.operands[0] instanceof RexCall) ||
                            !((RexCall)call.operands[0]).op.equals(opTab.concatOperator)))
                        {
                            regList.add(0,createResultRegister(translator,call));
                        }
                        ExtInstructionDefTable.concat.
                                add(translator._builder,regList);
                        translator.setResult(call,
                                (CalcProgramBuilder.Register) regList.get(0));
                    }
                });
        //~ EQUAL ---------------
        registerInstr(opTab.equalsOperator, CalcProgramBuilder.boolNativeEqual);
        //~ GREATER THAN ---------------
        registerInstr(opTab.greaterThanOperator,
                CalcProgramBuilder.boolNativeGreaterThan);
        //~ GREATER THAN OR EQUAL ---------------
        registerInstr(opTab.greaterThanOrEqualOperator,
                CalcProgramBuilder.boolNativeGreaterOrEqualThan);
        //~ IS NULL---------------
        registerInstr(opTab.isNullOperator, CalcProgramBuilder.boolNativeIsNull);
        //~ IS NOT NULL---------------
        registerInstr(opTab.isNotNullOperator, CalcProgramBuilder.boolNativeIsNotNull);
        //~ IS TRUE ---------------
        register(opTab.isTrueOperator, new IsBoolInstrImplementor(true));
        //~ IS NOT TRUE ---------------
        register(opTab.isNotTrueOperator, new IsNotBoolInstrImplementor(true));
        //~ IS FALSE`---------------
        register(opTab.isFalseOperator, new IsBoolInstrImplementor(false));
        //~ IS NOT FALSE ---------------
        register(opTab.isNotFalseOperator, new IsNotBoolInstrImplementor(false));
        //~ LESS THAN ---------------
        registerInstr(opTab.lessThanOperator, CalcProgramBuilder.boolNativeLessThan);
        //~ LESS THAN OR EQUAL ---------------
        registerInstr(opTab.lessThanOrEqualOperator,
                CalcProgramBuilder.boolNativeLessOrEqualThan);
        //~ LIKE ---------------
        register(opTab.likeOperator,
                new InstrImplementor(ExtInstructionDefTable.like) {
                    protected ArrayList makeRegList(RexToCalcTranslator translator,
                            RexCall call) {
                        ArrayList regList = super.makeRegList(translator, call);
                        if (2 == call.operands.length) {
                            CalcProgramBuilder.Register emptyStringReg =
                                    translator._builder.newVarcharLiteral("");
                            regList.add(emptyStringReg);
                        }
                        return regList;
                    }
                });
        //~ LOG ---------------
        registerInstr(opTab.logFunc, ExtInstructionDefTable.log10);
        //~ UPPER ---------
        // TODO: need to know charset aswell. When ready,
        // use same construct as with CHAR_LENGTH above
        registerInstr(opTab.lowerFunc, ExtInstructionDefTable.lower);
        //~ MINUS ---------------
        registerInstr(opTab.minusOperator, CalcProgramBuilder.nativeMinus);
        //~ MOD maps directly to a built in instruction---------------
        registerInstr(opTab.modFunc, CalcProgramBuilder.integralNativeMod);
        //~ MUL ---------------
        registerInstr(opTab.multiplyOperator, CalcProgramBuilder.integralNativeMul);
        //~ NOT EQUAL ---------------
        registerInstr(opTab.notEqualsOperator, CalcProgramBuilder.boolNativeNotEqual);
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
        //~ PREFIX MINUS ---------------
        registerInstr(opTab.prefixMinusOperator, CalcProgramBuilder.nativeNeg);
        //~ PREFIX PLUS---------------
        register(opTab.prefixPlusOperator,
                new InstrImplementor(new CalcProgramBuilder.InstructionDef(null, -1) {
                    void add(CalcProgramBuilder builder,
                            CalcProgramBuilder.Register[] regs) {
                        /* empty implementation */
                    }
                }) {
                    protected ArrayList makeRegList(RexToCalcTranslator translator,
                            RexCall call) {
                        translator.implementNode(call.operands[0]);
                        ArrayList regList = new ArrayList(1);
                        regList.add(translator.getResult(call.operands[0]));
                        return regList;
                    }
                });
        //~ SIMILAR ---------------
        register(opTab.similarOperator,
                new InstrImplementor(ExtInstructionDefTable.similar) {
                    protected ArrayList makeRegList(RexToCalcTranslator translator,
                            RexCall call) {
                        ArrayList regList = super.makeRegList(translator, call);
                        if (2 == call.operands.length) {
                            CalcProgramBuilder.Register emptyStringReg =
                                    translator._builder.newVarcharLiteral("");
                            regList.add(emptyStringReg);
                        }
                        return regList;
                    }
                });
        //~ SUBSTRING ---------------
        registerInstr(opTab.substringFunc,ExtInstructionDefTable.substring);
        //~ TRIM ---------------
        register(opTab.trimFunc,
                new InstrImplementor(ExtInstructionDefTable.trim) {
                    protected ArrayList makeRegList(RexToCalcTranslator translator,
                            RexCall call) {
                        ArrayList regList =
                                implementOperands(call,1,
                                        call.operands.length,translator);
                        CalcProgramBuilder.Register resultOfCall =
                                createResultRegister(translator, call);
                        assert call.operands[0] instanceof RexLiteral;
                        final RexLiteral literal = (RexLiteral)call.operands[0];
                        SqlTrimFunction.Flag flag = (SqlTrimFunction.Flag)
                                literal.getValue();
                        regList.add(translator._builder.newInt4Literal(flag._left));
                        regList.add(translator._builder.newInt4Literal(flag._right));
                        regList.add(0, resultOfCall);
                        return regList;
                    }
                });
        //~ UPPER ---------
        // TODO: need to know charset aswell. When ready,
        // use same construct as with CHAR_LENGTH above
        registerInstr(opTab.upperFunc, ExtInstructionDefTable.upper);

        registerInstr(opTab.localTimeFunc,ExtInstructionDefTable.localTime);
        registerInstr(opTab.localTimestampFunc,
                    ExtInstructionDefTable.localTimestamp);


    }
}

// End CalcRexImplementorTableImpl.java
