/*
// $Id$
// Aspen dataflow server
// (C) Copyright 2003-2003 Disruptive Technologies, Inc.
*/
package net.sf.saffron.opt;

import net.sf.saffron.rex.*;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.calc.CalcProgramBuilder;
import net.sf.saffron.core.SaffronTypeFactory;
import net.sf.saffron.core.SaffronTypeFactoryImpl;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.resource.SaffronResource;
import net.sf.saffron.sql.*;
import net.sf.saffron.sql.type.SqlTypeName;
import net.sf.saffron.util.Util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.lang.reflect.Method;

/**
 * ...
 *
 * @author wael
 * @since Feb 5, 2004
 * @version $Id$
 **/
public class CalcRelImplementor extends RelImplementor {

    public CalcRelImplementor (RexBuilder rexBuilder)
    {
        super(rexBuilder);
    }

    public Rex2CalcTranslator newTranslator(RexNode[] projectExps, RexNode conditionExp)
    {
        return new Rex2CalcTranslator(this.rexBuilder, projectExps, conditionExp);
    }

    /**
     *  Takes a rextree and transforms it into another in one sense equivalent rextree.
     *  Nodes in tree will be modified and hence tree will not remain unchanged
     */
//    public class RexToRexTransformer
//    {
//        private RexNode m_root;
//        public RexToRexTransformer(RexNode root)
//        {
//            m_root = root;
//        }
//
//        public boolean isBoolean(RexNode node)
//        {
//            if (node instanceof RexCall) {
//                SqlBinaryOperator op;
//                op.
//            }
//        }
//
//        public RexNode tranform()
//        {
//            RexCall c;
//            c.
//        }
//    }

    /** Inner class Translator */
    public class Rex2CalcTranslator
    {
        protected RexNode[] m_projectExps;
        protected RexNode m_conditionExp;
        final CalcProgramBuilder m_builder = new CalcProgramBuilder();
        final CalcProgramBuilder.Register m_trueReg = m_builder.newBoolLiteral(true);
        final CalcProgramBuilder.Register m_falseReg  = m_builder.newBoolLiteral(false);
        final SqlOperatorTable m_opTab = SqlOperatorTable.instance();
        final SqlFunctionTable m_funTab = SqlFunctionTable.instance();

        /** Maps all results from every result returnable RexNode to a register.<br>
         * Key: RexNode<BR>  Value: a register<br>
         * See {@link #getKey} for computing the key
         **/
        HashMap m_results = new HashMap();
        protected RexBuilder m_rexBuilder;
        protected boolean m_generateShortCircuit = false;
        protected int m_labelNbr = 0;
        protected final HashMap m_knownTypes = new HashMap();

        private String newLabel() {
            return "label$"+(m_labelNbr++);
        }


        public Rex2CalcTranslator(RexBuilder rexBuilder, RexNode[] projectExps, RexNode conditionExp)
        {
            m_projectExps = projectExps;
            m_conditionExp = conditionExp;
            m_rexBuilder = rexBuilder;

            SaffronTypeFactory fac = m_rexBuilder.getTypeFactory();
            m_knownTypes.put(fac.createSqlType(SqlTypeName.Integer), CalcProgramBuilder.OpType.Int);
            m_knownTypes.put(fac.createSqlType(SqlTypeName.Bigint), CalcProgramBuilder.OpType.LongLong);
            m_knownTypes.put(fac.createSqlType(SqlTypeName.Double), CalcProgramBuilder.OpType.Double);
            m_knownTypes.put(fac.createSqlType(SqlTypeName.Varbinary,0), CalcProgramBuilder.OpType.VoidPointer);
            m_knownTypes.put(fac.createSqlType(SqlTypeName.Bit,0), CalcProgramBuilder.OpType.VoidPointer);
            m_knownTypes.put(fac.createSqlType(SqlTypeName.Varchar,0), CalcProgramBuilder.OpType.VarCharPointer);
            m_knownTypes.put(fac.createSqlType(SqlTypeName.Boolean), CalcProgramBuilder.OpType.Boolean);


        }

        private CalcProgramBuilder.OpType getCalcType(RexNode node)
        {
            SaffronType unknownType = node.getType();
            Iterator it = m_knownTypes.keySet().iterator();
            while (it.hasNext()) {
                SaffronType knownType = (SaffronType) it.next();
                if (unknownType.isSameTypeFamily(knownType)) {
                    return (CalcProgramBuilder.OpType) m_knownTypes.get(knownType);
                }

                if (SaffronTypeFactoryImpl.isJavaType(unknownType)){
                    SqlTypeName typeName =
                            SaffronTypeFactoryImpl.JavaToSqlTypeConversionRules.instance().lookup(unknownType);
                    SaffronType lookupThis =
                            SaffronTypeFactoryImpl.createSqlTypeIgnorePrecOrScale(rexBuilder.getTypeFactory(),typeName);
                    if (lookupThis.isSameTypeFamily(knownType)) {
                        return (CalcProgramBuilder.OpType) m_knownTypes.get(knownType);
                    }
                }
            }

            throw Util.newInternal("unknown type");
        }

        private Object getKey(RexNode node)
        {
            if (node instanceof RexInputRef) {
                return ((RexInputRef) node).getName();
            }
            return node.toString()+node.getType().toString();
        }

        private void setResult(RexNode node, CalcProgramBuilder.Register register)
        {
            m_results.put(getKey(node), register);
        }



        /** Gets the result in form of a reference to register for the given rex node. If
         * node hasn't been implmented no result will be found and this function asserts.
         * Therefore don't use this function to check if result exists, see {@link #containsResult}
         * for that.
         * @param node
         * @return
         */
        private CalcProgramBuilder.Register getResult(RexNode node) {
            CalcProgramBuilder.Register found = (CalcProgramBuilder.Register) m_results.get(getKey(node));
            assert(null!=found);
            return found;
        }

        private boolean containsResult(RexNode node) {
            return m_results.get(getKey(node))!=null;
        }

        /**
         * Translate a the RexNode contined in a FilterRel into a {@link CalcProgramBuilder} calculator program
         * using a dept-fist recursive algorithm.
         * @return
         */
        public String getProgram()
        {

            //step 1: implement all the filtering logic
            implementNode(m_conditionExp);
            CalcProgramBuilder.Register filterResult = getResult(m_conditionExp);
            assert (CalcProgramBuilder.OpType.Boolean.getOrdinal() == filterResult.getOpType().getOrdinal());
            //step 2: report the status of the filtering
            CalcProgramBuilder.Register statusReg = m_builder.newStatus(CalcProgramBuilder.OpType.Boolean);

            //step 2-1: figure out the status
            m_builder.addJumpTrue(m_builder.getCurrentLineNumber()+4,filterResult); //todo line number incorrect
            // row didnt match
            m_builder.addMove(statusReg, m_trueReg);
            m_builder.addReturn();
            // row matched. Now calculate all the outputs
            for (int i = 0; i < m_projectExps.length; i++)
            {
                RexNode node = m_projectExps[i];
                //This could probably be optimized by writing to the outputs directly instead of temp registers
                //but harder to (java) implement
                implementNode(node);
            }
            // outputs calculated, now assign results to outputs.
            for (int i = 0; i < m_projectExps.length; i++) {
                RexNode node = m_projectExps[i];
                CalcProgramBuilder.OpType type = getCalcType(node);
                CalcProgramBuilder.Register outReg = m_builder.newOutput(type);
                m_builder.addMove(outReg, getResult(node));
            }

            m_builder.addMove(statusReg, m_falseReg);
            m_builder.addReturn();

            return m_builder.getProgram();
        }

        private void implementNode(RexNode node)
        {
            if (containsResult(node)) {
                return; //avoid reimplmenting node
            }

            //Try to have the most frequently implemented types first
            if (node instanceof RexInputRef) {
                implementNode((RexInputRef) node);
            }else if (node instanceof RexLiteral) {
                implementNode((RexLiteral) node);
            }else if (node instanceof RexCall) {
                implementNode((RexCall) node);
            }
            else {
                //nodes of type RexCorrelVariable, RexDynamicVariable, RexRangeRef should never feed to us
                throw SaffronResource.instance().newProgramImplementationError(
                        "Don't know how to implment rex node="+node);
            }

        }

        private void implementShortCircuit(RexCall call)
        {
            assert(call.operands.length == 2) : "not a binary operator";
            if (containsResult(call)) {
                assert(false) : "Shouldn't call this function directly, use implmentNode(RexNode) instead";
                return;//avoid reimplmenting node
            }
            SqlOperator op = call.op;

            if (op.kind.isA(SqlKind.And) || (op.kind.isA(SqlKind.Or)))
            {
                //first operand of AND/OR
                implementNode(call.operands[0]);
                String shortCut = newLabel();

                //Check if we can make a short cut
                if (op.kind.isA(SqlKind.And)) {
                    m_builder.addLabelJumpFalse(shortCut, getResult(call.operands[0]));
                }else{
                    m_builder.addLabelJumpTrue(shortCut, getResult(call.operands[0]));
                }

                //second operand
                implementNode(call.operands[1]);
                CalcProgramBuilder.Register result = m_builder.newLocal(CalcProgramBuilder.OpType.Boolean);
                assert(result.getOpType().getOrdinal()==getCalcType(call).getOrdinal());
                m_builder.addMove(result, getResult(call.operands[1]));

                String restOfInstructions = newLabel();
                m_builder.addLabelJump(restOfInstructions);
                m_builder.addLabel(shortCut);

                if (op.kind.isA(SqlKind.And)) {
                    m_builder.addMove(result, m_falseReg);
                }else{
                    m_builder.addMove(result, m_trueReg);
                }

                setResult(call, result);

                m_builder.addLabel(restOfInstructions); //TODO this assumes that more instructions will follow.
            }
            else{
                throw SaffronResource.instance().newProgramImplementationError(op.toString());
            }
        }


        private void implementNode(RexCall call)
        {
            if (containsResult(call)) {
                assert(false) : "Shouldn't call this function directly, use implmentNode(RexNode) instead";
                return;//avoid reimplmenting node
            }

            SqlOperator op = call.op;
            //check if and/or/xor should short circuit
            if (m_generateShortCircuit &&
                (op.kind.isA(SqlKind.And) || op.kind.isA(SqlKind.Or) /* || op.kind.isA(SqlKind.Xor) */ ))
            {
                implementShortCircuit(call);
                return;
            }

            CalcProgramBuilder.Register resultOfCall=null;
            CalcProgramBuilder.OpType resultType = getCalcType(call);

            //big fat if statement goes here
            //-- Binary Operators------------------------------------------
            if (op instanceof SqlBinaryOperator) {
                assert(call.operands.length == 2) : "not a binary operator";

                implementNode(call.operands[0]);
                implementNode(call.operands[1]);
                implmentConversionIfNeeded(op, call.operands[0], call.operands[1]);
                resultOfCall = m_builder.newLocal(resultType);
                CalcProgramBuilder.Register reg1=getResult(call.operands[0]);
                CalcProgramBuilder.Register reg2=getResult(call.operands[1]);


                if (op.kind.isA(SqlKind.Plus)) {
                    m_builder.addNativeAdd(resultOfCall, reg1, reg2);
                }else if (op.kind.isA(SqlKind.Minus)) {
                    m_builder.addNativeSub(resultOfCall, reg1, reg2);
                }else if (op.kind.isA(SqlKind.Times)) {
                    m_builder.addNativeMul(resultOfCall, reg1, reg2);
                }else if (op.kind.isA(SqlKind.Divide)) {
                    m_builder.addNativeDiv(resultOfCall, reg1, reg2);
                }else if (op.kind.isA(SqlKind.Equals)) {
                    m_builder.addBoolNativeEqual(resultOfCall, reg1, reg2);
                }else if (op.kind.isA(SqlKind.NotEquals)) {
                    m_builder.addBoolNativeNotEqual(resultOfCall, reg1, reg2);
                }else if (op.kind.isA(SqlKind.GreaterThan)) {
                    m_builder.addBoolNativeGreaterThan(resultOfCall, reg1, reg2);
                }else if (op.kind.isA(SqlKind.GreaterThanOrEqual)) {
                    m_builder.addBoolNativeGreaterOrEqual(resultOfCall, reg1, reg2);
                }else if (op.kind.isA(SqlKind.LessThan)) {
                    m_builder.addBoolNativeLessThan(resultOfCall, reg1, reg2);
                }else if (op.kind.isA(SqlKind.LessThanOrEqual)) {
                    m_builder.addBoolNativeLessOrEqual(resultOfCall, reg1, reg2);
                }else if (op.kind.isA(SqlKind.And)) {
                        m_builder.addBoolAnd(resultOfCall, reg1, reg2);
                }else if (op.kind.isA(SqlKind.Or)) {
                    m_builder.addBoolOr(resultOfCall, reg1, reg2);
                }

                else {
                    throw SaffronResource.instance().newProgramImplementationError("Binary operator"+op+" unknown");
                }
            }
            //-- Postfix Operators------------------------------------------
            else if (op instanceof SqlPostfixOperator)
            {
                assert(call.operands.length == 1) : "not a postfix operator";
                implementNode(call.operands[0]);

                //~  IS TRUE or IS FALSE    --------------------------
                if (op.kind.isA(SqlKind.IsTrue) ||
                    op.kind.isA(SqlKind.IsFalse))
                {
                    boolean boolValue = op.kind.isA(SqlKind.IsTrue);
                    RexNode operand =call.operands[0];
                    RexNode notNullCall = rexBuilder.makeCall(m_opTab.isNotNullOperator,operand);
                    RexNode eqCall = rexBuilder.makeCall(m_opTab.equalsOperator,operand, rexBuilder.makeLiteral(boolValue));
                    RexNode andCall = rexBuilder.makeCall(m_opTab.andOperator, notNullCall, eqCall);
                    implementNode(andCall);
                    resultOfCall = getResult(andCall);
                }
                //~  IS NOT NULL            --------------------------
                else if (op.equals(m_opTab.isNotNullOperator)){ //kind.isA(SqlKind.IsNotNull)) {
                    resultOfCall = m_builder.newLocal(resultType);
                    CalcProgramBuilder.Register reg=getResult(call.operands[0]);
                    m_builder.addBoolNativeIsNotNull(resultOfCall, reg);
                }
                //~  IS NULL                --------------------------
                else if (op.kind.isA(SqlKind.IsNull)) {
                    resultOfCall = m_builder.newLocal(resultType);
                    CalcProgramBuilder.Register reg=getResult(call.operands[0]);
                    m_builder.addBoolIsNull(resultOfCall, reg);
                }
                else {
                    throw SaffronResource.instance().newProgramImplementationError("Postfix operator "+op+" unknown");
                }
            }
            //-- Prefix Operators------------------------------------------
            else if (op instanceof SqlPrefixOperator)
            {
                assert(call.operands.length == 1) : "not a prefix operator";

                if (op.kind.isA(SqlKind.Not)) {
                    //optimization. If using not in front of IS NULL, create a call to the calc instruction ISNOTNULL
                    if (call.operands[0].isA(RexKind.IsNull)){
                        RexCall isNullCall = (RexCall) call.operands[0];
                        RexNode notNullCall = rexBuilder.makeCall(m_opTab.isNotNullOperator,isNullCall.operands[0]);
                        implementNode(notNullCall);
                        resultOfCall = getResult(notNullCall);
                    }
                    else
                    {
                        implementNode(call.operands[0]);
                        resultOfCall = m_builder.newLocal(resultType);
                        CalcProgramBuilder.Register reg=getResult(call.operands[0]);
                        m_builder.addBoolNot(resultOfCall, reg);
                    }
                } else if (op.kind.isA(SqlKind.PlusPrefix)) {
                    implementNode(call.operands[0]);
                    //dont do anything special, the operand is the result already
                    resultOfCall = getResult(call.operands[0]);
                } else if (op.kind.isA(SqlKind.MinusPrefix)) {
                    //todo if operand is literal make a new inverted literal
                    //if (call.operands[0] instanceof RexLiteral){
                    //}else
                    {
                        implementNode(call.operands[0]);
                        resultOfCall = m_builder.newLocal(resultType);
                        m_builder.addNativeNeg(resultOfCall, getResult(call.operands[0]));
                    }
                }

                else {
                    throw SaffronResource.instance().newProgramImplementationError("Prefix operator "+op+" unknown");
                }
            }
            //-- Function Calls ------------------------------------------
            //Special case when we cast null to some value
            else if (op.equals(m_funTab.cast) && RexLiteral.isNullLiteral(call.operands[0])){
                resultOfCall = m_builder.newLiteral(resultType, null);
            }
            else if (op instanceof SqlFunction)
            {
                //sanity checking that function exists, probably already done in the validator
                SqlFunction fun = (SqlFunction) op;
                Util.pre(fun.getNumOfOperands() == call.operands.length,"nbr of operands mismatch");
                if (null==m_funTab.lookup(fun.name)) {
                    throw SaffronResource.instance().newProgramImplementationError("Function "+fun+" unknown");
                }



                //implement the operands of the function
                for (int i = 0; i < call.operands.length; i++) {
                    RexNode operand = call.operands[i];
                    implementNode(operand);
                }

                //create the register where the result of the function will be placed
                resultOfCall = m_builder.newLocal(resultType);

                //add the call to the extended instruction
                //gather the result registers of the operands
                CalcProgramBuilder.Register[] resultOfOperands = new CalcProgramBuilder.Register[fun.getNumOfOperands()];
                for (int i = 0; i < call.operands.length; i++) {
                    resultOfOperands[i] = getResult(call.operands[i]);
                }

                m_builder.addExtendedInstructionCall(resultOfCall, fun.name, resultOfOperands);



            }
            //-- Special Operator Calls ------------------------------------------
            else if (op instanceof SqlCaseOperator)
            {
                Util.pre(call.operands.length>1,"call.operands.length>1");
                Util.pre((call.operands.length&1)==1,"(call.operands.length&1)==1");
                resultOfCall=m_builder.newLocal(resultType);
                String endOfCase = newLabel();
                String next;
                for(int i=0;i<call.operands.length-1;i+=2) {
                    next = newLabel();
                    implementNode(call.operands[i]);
                    m_builder.addLabelJumpFalse(next, getResult(call.operands[i]));
                    implementNode(call.operands[i+1]);
                    m_builder.addMove(resultOfCall, getResult(call.operands[i+1]));
                    m_builder.addLabelJump(endOfCase);
                    m_builder.addLabel(next);
                }
                int elseIndex = call.operands.length-1;
                implementNode(call.operands[elseIndex]);
                m_builder.addMove(resultOfCall, getResult(call.operands[elseIndex]));
                m_builder.addLabel(endOfCase); //this assumes that more instructions will follow

            }
            else {
                throw SaffronResource.instance().newProgramImplementationError("Unknown operator "+op);
            }

            assert null!=resultOfCall;
            setResult(call, resultOfCall);
        }

        /** If conversion is needed between the two operands this function will insert a call
         * to the calculator convert function and silently update the result register of the operands as needed
         * @param op
         * @param op1
         * @param op2
         */
        private void implmentConversionIfNeeded(SqlOperator op, RexNode op1, RexNode op2)
        {
            //TODO
        }

        private void implementNode(RexLiteral node)
        {
           if (containsResult(node)) {
               assert(false) : "Shouldn't call this function directly, use implmentNode(RexNode) instead";
               return;//avoid reimplmenting node
           }

           Object value = node.getValue();
           if (value instanceof SqlLiteral.BitString) {
               value = ((SqlLiteral.BitString) value).getAsByteArray();
           } else if (value instanceof SqlLiteral.StringLiteral) {
               value = ((SqlLiteral.StringLiteral) value).getValue();
           }

           setResult(node, m_builder.newLiteral(getCalcType(node), value));
        }

        private void implementNode(RexInputRef node)
        {
           if (containsResult(node)) {
               assert(false) : "Shouldn't call this function directly, use implmentNode(RexNode) instead";
               return;//avoid reimplmenting node
           }
           setResult(node, m_builder.newInput(getCalcType(node)));
        }

        public void setGenerateShortCircuit(boolean generateShortCircuit) {
            this.m_generateShortCircuit = generateShortCircuit;
        }
    }

}

// End CalcRelImplementor.java