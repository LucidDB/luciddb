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
import net.sf.saffron.sql.SqlOperator;
import net.sf.saffron.sql.SqlCollation;
import net.sf.saffron.sql.SqlKind;
import net.sf.saffron.sql.SqlFunction;
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
    private final HashMap operatorImplemenationMap = new HashMap();

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
     */
    void register(SqlOperator op, CalcRexImplementor impl) {

        Util.pre(op != null, "op != null");
        Util.pre(impl != null, "impl != null");
        operatorImplemenationMap.put(op, impl);
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

    /** implement interface CalcRexImplementorTable */
    public CalcRexImplementor get(SqlOperator op) {
        return (CalcRexImplementor) operatorImplemenationMap.get(op);
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
            CalcProgramBuilder.Register[] regs =
                    (CalcProgramBuilder.Register[]) regList.toArray(
                            new CalcProgramBuilder.Register[
                                    regList.size()]);

            instr.add(translator._builder, regs);
            final CalcProgramBuilder.Register resultReg = regs[0];
            translator.setResult(call, resultReg);
        }

        /**
         * Creates the list of registers which will be arguments to the
         * instruction call.
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

    private static class IsBoolInstrImplementor implements CalcRexImplementor
    {
        private boolean boolType;
        protected SqlStdOperatorTable opTab;
        protected RexNode res;

        IsBoolInstrImplementor(SqlStdOperatorTable opTab, boolean boolType) {
            this.opTab = opTab;
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

    private static class IsNotBoolInstrImplementor extends IsBoolInstrImplementor
    {
        IsNotBoolInstrImplementor(SqlStdOperatorTable opTab, boolean boolType) {
            super(opTab, boolType);
        }

        public void implement(RexCall call, RexToCalcTranslator translator) {
            super.implement(call,translator);
            res = translator._rexBuilder.makeCall(opTab.notOperator,res);
            translator.implementNode(res);
            translator.setResult(call, translator.getResult(res));
        }
    }

    private static class AddCharSetNameInstrImplementor extends InstrImplementor
    {
        AddCharSetNameInstrImplementor(String extCall, int regCount) {
            super(new CalcProgramBuilder.ExtInstrDef(extCall, regCount));
        }

        protected ArrayList makeRegList(RexToCalcTranslator translator,
                RexCall call) {

            ArrayList regList = super.makeRegList(translator, call);
            CalcProgramBuilder.Register charSetName =
                    translator._builder.newVarcharLiteral(
                            call.operands[0].getType().getCharset().name());
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
                new InstrImplementor(new CalcProgramBuilder.InstructionDef(null, -1) {
                    void add(CalcProgramBuilder builder, CalcProgramBuilder.Register[] regs) {
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
                            translator._builder.addLabelJumpFalse(next, translator.getResult(call.operands[i]));
                            translator.implementNode(call.operands[i+1]);
                            translator._builder.addMove(resultOfCall, translator.getResult(call.operands[i+1]));
                            translator._builder.addLabelJump(endOfCase);
                            translator._builder.addLabel(next);
                        }
                        int elseIndex = call.operands.length-1;
                        translator.implementNode(call.operands[elseIndex]);
                        translator._builder.addMove(resultOfCall, translator.getResult(call.operands[elseIndex]));
                        translator._builder.addLabel(endOfCase); //this assumes that more instructions will follow
                        translator.setResult(call,resultOfCall);
                    }
                });
        //~ CHARACTER_LENGTH ---------
        // needs an extra argument.
        register(opTab.characterLengthFunc,
                new AddCharSetNameInstrImplementor("CHAR_LENGTH", 3));
        // CHAR_LENGTH shares CHARACTER_LENGTH's implementation.
        // TODO: Combine the CHAR_LENGTH and CHARACTER_LENGTH at the
        //   RexNode level (they should remain separate functions at the
        //   SqlNode level).
        register(opTab.charLengthFunc, get(opTab.characterLengthFunc));
        //~ CONCAT ---------------
        register(opTab.concatOperator,
                new InstrImplementor(new CalcProgramBuilder.ExtInstrDef("CONCAT", 3)));
        //~ EQUAL ---------------
        registerInstr(opTab.equalsOperator, CalcProgramBuilder.boolNativeEqual);
        //~ GREATER THAN ---------------
        registerInstr(opTab.greaterThanOperator, CalcProgramBuilder.boolNativeGreaterThan);
        //~ GREATER THAN OR EQUAL ---------------
        registerInstr(opTab.greaterThanOrEqualOperator, CalcProgramBuilder.boolNativeGreaterOrEqualThan);
        //~ IS NULL---------------
        registerInstr(opTab.isNullOperator, CalcProgramBuilder.boolNativeIsNull);
        //~ IS NOT NULL---------------
        registerInstr(opTab.isNotNullOperator, CalcProgramBuilder.boolNativeIsNotNull);
        //~ IS TRUE ---------------
        register(opTab.isTrueOperator, new IsBoolInstrImplementor(opTab, true));
        //~ IS NOT TRUE ---------------
        register(opTab.isNotTrueOperator, new IsNotBoolInstrImplementor(opTab, true));
        //~ IS FALSE`---------------
        register(opTab.isFalseOperator, new IsBoolInstrImplementor(opTab, false));
        //~ IS NOT FALSE ---------------
        register(opTab.isNotFalseOperator, new IsNotBoolInstrImplementor(opTab, false));
        //~ LESS THAN ---------------
        registerInstr(opTab.lessThanOperator, CalcProgramBuilder.boolNativeLessThan);
        //~ LESS THAN OR EQUAL ---------------
        registerInstr(opTab.lessThanOrEqualOperator, CalcProgramBuilder.boolNativeLessOrEqualThan);
        //~ UPPER ---------
        // TODO: need to know charset aswell. When ready,
        // use same construct as with CHAR_LENGTH above
        registerInstr(opTab.lowerFunc,
                new CalcProgramBuilder.ExtInstrDef("strToLowerA", 2));
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
        // todo: optimization. If using not in front of IS NULL, create a call to the calc instruction ISNOTNULL
        //~ OR ---------------
        registerInstr(opTab.orOperator, CalcProgramBuilder.boolOr);
        //~ PREFIX MINUS ---------------
        registerInstr(opTab.prefixMinusOperator, CalcProgramBuilder.nativeNeg);
        //~ PREFIX PLUS---------------
        register(opTab.prefixPlusOperator,
                new InstrImplementor(new CalcProgramBuilder.InstructionDef(null, -1) {
                    void add(CalcProgramBuilder builder, CalcProgramBuilder.Register[] regs) {
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
        //~ TRIM ---------------
        register(opTab.trimFunc,
                new InstrImplementor(new CalcProgramBuilder.ExtInstrDef("TRIM", 5)) {
                    protected ArrayList makeRegList(RexToCalcTranslator translator,
                            RexCall call) {
                        ArrayList regList =
                                implementOperands(call,1,call.operands.length,translator);
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
        registerInstr(opTab.upperFunc,
                new CalcProgramBuilder.ExtInstrDef("strToUpperA", 2));


    }
}

// End CalcRexImplementorTableImpl.java