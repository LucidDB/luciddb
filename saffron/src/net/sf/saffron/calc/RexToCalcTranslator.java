/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2004-2004 Disruptive Tech
// (C) Copyright 2003-2004 John V. Sichi
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

import net.sf.saffron.core.SaffronField;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.core.SaffronTypeFactory;
import net.sf.saffron.core.SaffronTypeFactoryImpl;
import net.sf.saffron.resource.SaffronResource;
import net.sf.saffron.rex.*;
import net.sf.saffron.sql.*;
import net.sf.saffron.sql.fun.SqlStdOperatorTable;
import net.sf.saffron.sql.type.SqlTypeName;
import net.sf.saffron.util.Util;
import net.sf.saffron.util.SaffronProperties;

import java.util.HashMap;

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
    protected final RexNode[] _projectExps;
    protected final RexNode _conditionExp;
    final CalcProgramBuilder _builder = new CalcProgramBuilder();
    final CalcProgramBuilder.Register _trueReg = _builder.newBoolLiteral(true);
    final CalcProgramBuilder.Register _falseReg = _builder.newBoolLiteral(false);
    final SqlStdOperatorTable _opTab = SqlOperatorTable.std();
    final CalcRexImplementorTable implementorTable =
            CalcRexImplementorTableImpl.threadInstance();

    /**
     * Maps all results from every result returnable RexNode to a register.
     *
     * <p>Key: {@link net.sf.saffron.rex.RexNode};
     * value: {@link net.sf.saffron.calc.CalcProgramBuilder.Register}.
     * See {@link #getKey} for computing the key
     **/
    private HashMap _results = new HashMap();
    protected final RexBuilder _rexBuilder;
    protected boolean _generateShortCircuit = false;
    protected int _labelNbr = 0;
    /**
     * Ordered mapping from Saffron types (representing a family of types)
     * to the corresponding calculator type. The ordering ensures
     * determinacy.
     */
    private final TypePair[] _knownTypes;

    public String newLabel() {
        return "label$"+(_labelNbr++);
    }

    private static class TypePair {
        private final SaffronType _saffronType;
        private final CalcProgramBuilder.OpType _opType;

        TypePair(SaffronType saffron, CalcProgramBuilder.OpType op) {
            _saffronType = saffron;
            _opType = op;
        }
    }

    public RexToCalcTranslator(RexBuilder rexBuilder, RexNode[] projectExps,
            RexNode conditionExp)
    {
        _projectExps = projectExps;
        _conditionExp = conditionExp;
        _rexBuilder = rexBuilder;

        setGenerateComments(SaffronProperties.instance().
                generateCalcProgramComments.get());
        SaffronTypeFactory fac = _rexBuilder.getTypeFactory();
        _knownTypes = new TypePair[] {
            new TypePair(fac.createSqlType(SqlTypeName.Tinyint),
                    CalcProgramBuilder.OpType.Int1),
            new TypePair(fac.createSqlType(SqlTypeName.Smallint),
                    CalcProgramBuilder.OpType.Int2),
            new TypePair(fac.createSqlType(SqlTypeName.Integer),
                    CalcProgramBuilder.OpType.Int4),
            new TypePair(fac.createSqlType(SqlTypeName.Bigint),
                    CalcProgramBuilder.OpType.Int8),
            new TypePair(fac.createSqlType(SqlTypeName.Double),
                    CalcProgramBuilder.OpType.Double),
            new TypePair(fac.createSqlType(SqlTypeName.Real),
                    CalcProgramBuilder.OpType.Real),
            new TypePair(fac.createSqlType(SqlTypeName.Varbinary,0),
                    CalcProgramBuilder.OpType.Varbinary),
            new TypePair(fac.createSqlType(SqlTypeName.Bit,0),
                    CalcProgramBuilder.OpType.Varbinary),
            new TypePair(fac.createSqlType(SqlTypeName.Varchar,0),
                    CalcProgramBuilder.OpType.Varchar),
            new TypePair(fac.createSqlType(SqlTypeName.Boolean),
                    CalcProgramBuilder.OpType.Bool),

            // FIXME: not right for T/w TZ.
            new TypePair(fac.createSqlType(SqlTypeName.Date),
                    CalcProgramBuilder.OpType.Int8),
            new TypePair(fac.createSqlType(SqlTypeName.Time),
                    CalcProgramBuilder.OpType.Int8),
            new TypePair(fac.createSqlType(SqlTypeName.Timestamp),
                    CalcProgramBuilder.OpType.Int8),

            new TypePair(fac.createJavaType(Byte.class),
                    CalcProgramBuilder.OpType.Int1),
            new TypePair(fac.createJavaType(byte.class),
                    CalcProgramBuilder.OpType.Int1),
            new TypePair(fac.createJavaType(Short.class),
                    CalcProgramBuilder.OpType.Int2),
            new TypePair(fac.createJavaType(short.class),
                    CalcProgramBuilder.OpType.Int2),
            new TypePair(fac.createJavaType(Integer.class),
                    CalcProgramBuilder.OpType.Int4),
            new TypePair(fac.createJavaType(int.class),
                    CalcProgramBuilder.OpType.Int4),
            new TypePair(fac.createJavaType(Long.class),
                    CalcProgramBuilder.OpType.Int8),
            new TypePair(fac.createJavaType(long.class),
                    CalcProgramBuilder.OpType.Int8),
            new TypePair(fac.createJavaType(Double.class),
                    CalcProgramBuilder.OpType.Double),
            new TypePair(fac.createJavaType(double.class),
                    CalcProgramBuilder.OpType.Double),
            new TypePair(fac.createJavaType(Float.class),
                    CalcProgramBuilder.OpType.Real),
            new TypePair(fac.createJavaType(float.class),
                    CalcProgramBuilder.OpType.Real),
            new TypePair(fac.createJavaType(String.class),
                    CalcProgramBuilder.OpType.Varchar),
            new TypePair(fac.createJavaType(Boolean.class),
                    CalcProgramBuilder.OpType.Bool),
            new TypePair(fac.createJavaType(boolean.class),
                    CalcProgramBuilder.OpType.Bool),
        };
    }

    /**
     * Returns whether an expression can be translated.
     *
     * @param node Expression
     * @param deep Whether to check child expressions
     */
    public boolean canTranslate(RexNode node, boolean deep) {
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

    CalcProgramBuilder.RegisterDescriptor
            getCalcRegisterDescriptor(RexNode node) {

        SaffronType rexNodeSaffronType = node.getType();
        String typeDigest = rexNodeSaffronType.toString();
        // Special case for Char and Binary, because have the same
        // respective rexNodeSaffronType families as Varchar and Varbinary, but the
        // calc needs to treat them differently.
        CalcProgramBuilder.OpType calcType = null;
        if (typeDigest.startsWith("CHAR")) {
            calcType = CalcProgramBuilder.OpType.Char;
        }
        if (typeDigest.startsWith("BINARY")) {
            calcType = CalcProgramBuilder.OpType.Binary;
        }
        SaffronType lookupThis = getSimilarSqlType(rexNodeSaffronType);
        for (int i = 0; i < _knownTypes.length; i++) {
            TypePair knownType = _knownTypes[i];
            if (rexNodeSaffronType.isSameType(knownType._saffronType)) {
                calcType = knownType._opType;
            }

            if (lookupThis != null &&
                    lookupThis.isSameType(knownType._saffronType)) {
                calcType = knownType._opType;
            }
        }

        if (null == calcType) {
            throw Util.newInternal("unknown type " + rexNodeSaffronType);
        }

        int bytes = rexNodeSaffronType.getMaxBytesStorage();
        if (bytes < 0) {
            // Adjust for types which map to char or binary,  but which
            // don't know their maximum length. The java string rexNodeSaffronType is an
            // example of this.
            switch (calcType.getOrdinal()) {
            case CalcProgramBuilder.OpType.Binary_ordinal:
            case CalcProgramBuilder.OpType.Char_ordinal:
            case CalcProgramBuilder.OpType.Varbinary_ordinal:
            case CalcProgramBuilder.OpType.Varchar_ordinal:
                bytes = 0;
                break;
            }
        }

        return new CalcProgramBuilder.RegisterDescriptor(calcType, bytes);
    }

    /**
     * If type is a SQL type, returns a broader SQL type, otherwise returns
     * null.
     */
    private SaffronType getSimilarSqlType(SaffronType type) {
        if (!SaffronTypeFactoryImpl.isJavaType(type)) {
            return null;
        }
        SqlTypeName typeName = SaffronTypeFactoryImpl
                .JavaToSqlTypeConversionRules.instance().lookup(type);
        return SaffronTypeFactoryImpl.createSqlTypeIgnorePrecOrScale(
                        _rexBuilder.getTypeFactory(), typeName);
    }

    private Object getKey(RexNode node)
    {
        if (node instanceof RexInputRef) {
            return ((RexInputRef) node).getName();
        }
        return node.toString()+node.getType().toString();
    }

    void setResult(RexNode node, CalcProgramBuilder.Register register)
    {
        _results.put(getKey(node), register);
    }



    /** Gets the result in form of a reference to register for the given rex node. If
     * node hasn't been implmented no result will be found and this function asserts.
     * Therefore don't use this function to check if result exists, see {@link #containsResult}
     * for that.
     * @param node
     * @return
     */
    CalcProgramBuilder.Register getResult(RexNode node) {
        CalcProgramBuilder.Register found = (CalcProgramBuilder.Register) _results.get(getKey(node));
        assert(null!=found);
        return found;
    }

    boolean containsResult(RexNode node) {
        return _results.get(getKey(node))!=null;
    }

    /**
     * Translates a RexNode contained in a FilterRel into a
     * {@link net.sf.saffron.calc.CalcProgramBuilder} calculator program
     * using a depth-first recursive algorithm.
     *
     * @param inputRowType The type of the input row to the calculator.
     *   If <code>inputRowType</code> is not null, the program contains
     *   an input register for every field in the input row type; otherwise
     *   it contains inputs for only those fields used.
     */
    public String getProgram(SaffronType inputRowType)
    {
        // Step 0. Create input fields.
        // Create an calculator input for each field in the input relation,
        // regardless of whether the calcualtor program uses them.
        if (inputRowType != null) {
            final SaffronField[] fields = inputRowType.getFields();
            for (int i = 0; i < fields.length; i++) {
                RexInputRef rexInputRef =
                        new RexInputRef(i, fields[i].getType());
                implementNode(rexInputRef);
            }
        }

        //step 1: implement all the filtering logic
        if (_conditionExp != null) {
            CalcProgramBuilder.Register filterResult =
                    implementNode(_conditionExp);
            assert CalcProgramBuilder.OpType.Bool ==
                    filterResult.getOpType() :
                    "Condition must be boolean: " + _conditionExp;
            //step 2: report the status of the filtering
            CalcProgramBuilder.Register statusReg =
                    _builder.newStatus(CalcProgramBuilder.OpType.Bool,
                                        -1);

            //step 2-1: figure out the status
            String prepareOutput = newLabel();
            _builder.addLabelJumpTrue(prepareOutput,filterResult);
            // row didnt match
            CalcProgramBuilder.move.add(_builder,statusReg, _trueReg);
            _builder.addReturn();
            _builder.addLabel(prepareOutput);
            CalcProgramBuilder.move.add(_builder,statusReg, _falseReg);
        }

        // row matched. Now calculate all the outputs
        for (int i = 0; i < _projectExps.length; i++)
        {
            RexNode node = _projectExps[i];
            // This could probably be optimized by writing to the outputs
            // directly instead of temp registers but harder to (java)
            // implement.
            implementNode(node);
        }
        // outputs calculated, now assign results to outputs by reference
        for (int i = 0; i < _projectExps.length; i++) {
            RexNode node = _projectExps[i];
            CalcProgramBuilder.RegisterDescriptor desc =
                    getCalcRegisterDescriptor(node);
            CalcProgramBuilder.Register outReg = _builder.newOutput(desc);
            _builder.addRef(outReg, getResult(node));
        }

        _builder.addReturn();
        return _builder.getProgram();
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

    public void visitInputRef(RexInputRef inputRef) {
        implementNode(inputRef);
    }

    public void visitLiteral(RexLiteral literal) {
        implementNode(literal);
    }

    public void visitCall(RexCall call) {
        int before = _builder.getCurrentLineNumber();
        implementNode(call);
        int after = _builder.getCurrentLineNumber();
        assert(after>=before);
        if (0!=(after-before)) {
            _builder.addComment(call.toString());
        }
    }

    public void visitCorrelVariable(RexCorrelVariable correlVariable) {
        throw SaffronResource.instance().newProgramImplementationError(
                "Don't know how to implement rex node=" + correlVariable);
    }

    public void visitDynamicParam(RexDynamicParam dynamicParam) {
        throw SaffronResource.instance().newProgramImplementationError(
                "Don't know how to implement rex node=" + dynamicParam);
    }

    public void visitRangeRef(RexRangeRef rangeRef) {
        throw SaffronResource.instance().newProgramImplementationError(
                "Don't know how to implement rex node=" + rangeRef);
    }

    public void visitContextVariable(RexContextVariable variable) {
        implementNode(variable);
    }

    public void visitFieldAccess(RexFieldAccess fieldAccess) {
        throw SaffronResource.instance().newProgramImplementationError(
                "Don't know how to implement rex node=" + fieldAccess);
    }

    private void implementShortCircuit(RexCall call)
    {
        assert(call.operands.length == 2) : "not a binary operator";
        if (containsResult(call)) {
            throw new AssertionError(
                "Shouldn't call this function directly;"
                + " use implementNode(RexNode) instead");
        }
        SqlOperator op = call.op;

        if (op.kind.isA(SqlKind.And) || (op.kind.isA(SqlKind.Or)))
        {
            //first operand of AND/OR
            CalcProgramBuilder.Register reg0 = implementNode(call.operands[0]);
            String shortCut = newLabel();

            //Check if we can make a short cut
            if (op.kind.isA(SqlKind.And)) {
                _builder.addLabelJumpFalse(shortCut, reg0);
            }else{
                _builder.addLabelJumpTrue(shortCut, reg0);
            }

            //second operand
            CalcProgramBuilder.Register reg1 = implementNode(call.operands[1]);
            CalcProgramBuilder.Register result =
                    _builder.newLocal(CalcProgramBuilder.OpType.Bool, -1);
            assert result.getOpType().getOrdinal() ==
                   getCalcRegisterDescriptor(call).getType().getOrdinal();
            CalcProgramBuilder.move.add(_builder,result, reg1);

            String restOfInstructions = newLabel();
            _builder.addLabelJump(restOfInstructions);
            _builder.addLabel(shortCut);

            if (op.kind.isA(SqlKind.And)) {
                CalcProgramBuilder.move.add(_builder,result, _falseReg);
            } else {
                CalcProgramBuilder.move.add(_builder,result, _trueReg);
            }

            setResult(call, result);

            //WARNING this assumes that more instructions will follow.
            //Return is currently always at the end.
            _builder.addLabel(restOfInstructions);
        }
        else{
            throw SaffronResource.instance().newProgramImplementationError(op.toString());
        }
    }

    private void implementNode(RexCall call)
    {
        if (containsResult(call)) {
            throw new AssertionError(
                "Shouldn't call this function directly;"
                + " use implementNode(RexNode) instead");
        }

        SqlOperator op = call.op;
        //check if and/or/xor should short circuit
        if (_generateShortCircuit &&
                (op.kind.isA(SqlKind.And) ||
                op.kind.isA(SqlKind.Or) /* ||
                op.kind.isA(SqlKind.Xor) */ )) {
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
            implementConversionIfNeeded(op, call.operands[0], call.operands[1]);
            resultOfCall = _builder.newLocal(resultDesc);

            assert resultDesc.getType() == CalcProgramBuilder.OpType.Bool;
            CalcProgramBuilder.Register strCmpResult =
                    _builder.newLocal(CalcProgramBuilder.OpType.Int4, -1);
            if (call.operands[0].getType().isCharType()) {
                String collationToUse =
                        SqlCollation.getCoercibilityDyadicComparison(
                                call.operands[0].getType().getCollation(),
                                call.operands[1].getType().getCollation());
                CalcProgramBuilder.Register colReg = _builder.newLiteral(
                        CalcProgramBuilder.OpType.Varchar,
                        collationToUse,
                        CalcProgramBuilder.stringByteCount(collationToUse));
                //TODO this is only for ascci cmp. Need to pump in colReg when a
                //fennel function that can take it is born
                ExtInstructionDefTable.strCmpA.add(_builder,
                    new CalcProgramBuilder.Register[]{strCmpResult,reg1,reg2});
            } else {
                ExtInstructionDefTable.strCmpOct.add(_builder,
                    new CalcProgramBuilder.Register[]{strCmpResult,reg1,reg2});
            }

            CalcProgramBuilder.Register zero = _builder.newInt4Literal(0);
            CalcProgramBuilder.Register[] regs = {
                resultOfCall, strCmpResult, zero};
            if (op.kind.isA(SqlKind.Equals)) {
                CalcProgramBuilder.boolNativeEqual.add(_builder, regs);
            } else if (op.kind.isA(SqlKind.NotEquals)) {
                CalcProgramBuilder.boolNativeNotEqual.add(_builder, regs);
            } else if (op.kind.isA(SqlKind.GreaterThan)) {
                CalcProgramBuilder.boolNativeGreaterThan.add(_builder, regs);
            } else if (op.kind.isA(SqlKind.LessThan)) {
                CalcProgramBuilder.boolNativeLessThan.add(_builder, regs);
            } else if (op.kind.isA(SqlKind.GreaterThanOrEqual)) {
                CalcProgramBuilder.boolNativeGreaterOrEqualThan.add(_builder, regs);
            } else if (op.kind.isA(SqlKind.LessThanOrEqual)) {
                CalcProgramBuilder.boolNativeLessOrEqualThan.add(_builder, regs);
            } else {
                throw Util.newInternal("Unknown op " + op);
            }
            setResult(call, resultOfCall);
            return;
        }

        // Ask implementor table for if op exists.
        // There are some special cases: see above and below.
        CalcRexImplementor implementor = implementorTable.get(op);
        if (implementor != null && implementor.canImplement(call)) {
            CalcProgramBuilder.Register reg =
                    implementor.implement(call, this);
            setResult(call, reg);
            return;
        }


        throw SaffronResource.instance().newProgramImplementationError(
                "Unknown operator "+op);
    }

    protected boolean lhsTypeIsNullableRHSType(SaffronType returnType, SaffronType argType) {
        return returnType.isSameType(argType) ||
               (returnType.isNullable() && !argType.isNullable() &&
                returnType.getSqlTypeName().equals(argType.getSqlTypeName()));
    }

    private boolean isStrCmp(RexCall call) {
        SqlOperator op = call.op;
        if (op.kind.isA(SqlKind.Equals) ||
            op.kind.isA(SqlKind.NotEquals) ||
            op.kind.isA(SqlKind.GreaterThan) ||
            op.kind.isA(SqlKind.LessThan) ||
            op.kind.isA(SqlKind.GreaterThanOrEqual) ||
            op.kind.isA(SqlKind.LessThanOrEqual))
        {
            SaffronType t0 = call.operands[0].getType();
            SaffronType t1 = call.operands[1].getType();

            return ( t0.isCharType() && t1.isCharType() ) ||
                   ( isOctetString(t0) && isOctetString(t1) );
        }
        return false;
    }

    private static boolean isOctetString(SaffronType t) {
        switch (t.getSqlTypeName().ordinal_) {
        case SqlTypeName.Bit_ordinal:
        case SqlTypeName.Varbinary_ordinal:
        case SqlTypeName.Binary_ordinal:
            return true;
        }
        return false;
    }


    /**
     * If conversion is needed between the two operands this function
     * inserts a call to the calculator convert function and silently
     * updates the result register of the operands as needed
     */
    private void implementConversionIfNeeded(SqlOperator op, RexNode op1,
            RexNode op2)
    {
        //TODO
    }

    private void implementNode(RexLiteral node)
    {
       if (containsResult(node)) {
           throw new AssertionError(
               "Shouldn't call this function directly;"
               + " use implementNode(RexNode) instead");
       }

       Object value = node.getValue2();
       CalcProgramBuilder.RegisterDescriptor desc =
               getCalcRegisterDescriptor(node);
       setResult(node, _builder.newLiteral(desc, value));
    }

    private void implementNode(RexInputRef node)
    {
       if (containsResult(node)) {
           throw new AssertionError(
               "Shouldn't call this function directly;"
               + " use implementNode(RexNode) instead");
       }
        setResult(node, _builder.newInput(getCalcRegisterDescriptor(node)));
    }

    private void implementNode(RexContextVariable node)
    {
        if (containsResult(node)) {
            throw new AssertionError(
                    "Shouldn't call this function directly;"
                    + " use implementNode(RexNode) instead");
        }

        throw SaffronResource.instance().newProgramImplementationError(
                    "Don't know how to implement rex node=" + node);
    }

    public void setGenerateShortCircuit(boolean generateShortCircuit) {
        this._generateShortCircuit = generateShortCircuit;
    }

    public void setGenerateComments(boolean outputComments) {
        _builder.setOutputComments(outputComments);
    }

    /**
     * Trivial exception thrown when {@link TranslationTester} finds a node
     * it cannot translate.
     */
    private static class TranslationException extends RuntimeException {
    };

    /**
     * Visitor which walks over a {@link RexNode row expression} and throws
     * {@link TranslationException} if it finds a node which cannot be
     * implemented.
     */
    private class TranslationTester implements RexVisitor {
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
        TranslationTester(RexToCalcTranslator translator, boolean deep) {
            this.translator = translator;
            this.deep = deep;
        }
        public void visitInputRef(RexInputRef inputRef) {
        }

        public void visitLiteral(RexLiteral literal) {
        }

        public void visitCall(RexCall call) {
            final SqlOperator op = call.op;
            CalcRexImplementor implementor =
                    translator.implementorTable.get(op);
            if (implementor == null ||
                    !implementor.canImplement(call)) {
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

        public void visitCorrelVariable(RexCorrelVariable correlVariable) {
            // Matches RexToCalcTranslator.visitCorrelVariable()
            throw new TranslationException();
        }

        public void visitDynamicParam(RexDynamicParam dynamicParam) {
            // Matches RexToCalcTranslator.visitDynamicParam()
            throw new TranslationException();
        }

        public void visitRangeRef(RexRangeRef rangeRef) {
            // Matches RexToCalcTranslator.visitRangeRef()
            throw new TranslationException();
        }

        public void visitContextVariable(RexContextVariable variable) {
        }

        public void visitFieldAccess(RexFieldAccess fieldAccess) {
            if (!deep) {
                return;
            }
            final RexNode expr = fieldAccess.getReferenceExpr();
            expr.accept(this);
        }
    }
}

// End RexToCalcTranslator.java
