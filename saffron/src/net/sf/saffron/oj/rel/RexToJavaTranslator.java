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
package net.sf.saffron.oj.rel;

import net.sf.saffron.core.SaffronField;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.oj.OJTypeFactory;
import net.sf.saffron.oj.util.JavaRowExpression;
import net.sf.saffron.oj.util.OJUtil;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.rex.*;
import net.sf.saffron.sql.*;
import net.sf.saffron.sql.type.SqlTypeName;
import net.sf.saffron.util.NlsString;
import net.sf.saffron.util.Util;
import net.sf.saffron.util.BitString;
import openjava.mop.OJClass;
import openjava.mop.OJSystem;
import openjava.ptree.*;

import java.math.BigDecimal;
import java.util.Calendar;
import java.sql.Timestamp;
import java.sql.Date;
import java.sql.Time;

/**
 * Translates {@link RexNode row-expressions} into
 * {@link Expression Java expressions}.
 *
 * A translator is created by the {@link JavaRelImplementor#newTranslator}
 * factory method, and used by methods such as
 * {@link JavaRelImplementor#translate}.
 */
public class RexToJavaTranslator
{
    protected JavaRelImplementor _implementor;
    protected SaffronRel _rel;

    /**
     * Creates a translator.
     *
     * @param implementor Implementation context
     * @param rel Relational expression which is the context for the
     *   row-expressions which are to be translated
     */
    protected RexToJavaTranslator(JavaRelImplementor implementor,
            SaffronRel rel)
    {
        this._implementor = implementor;
        this._rel = rel;
    }

    /**
     * Translates an row-expression into a Java expression.
     */
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
            WhichInputResult inputAndCol = whichInput(inputColRef.index, _rel);
            if (inputAndCol == null) {
                throw Util.newInternal("input not found");
            }
            final Variable v = _implementor.findInputVariable(inputAndCol._input);
            SaffronType rowType = inputAndCol._input.getRowType();
            final SaffronField field = rowType.getFields()[inputAndCol._fieldIndex];
            return new FieldAccess(v, field.getName());
        }
        if (rex instanceof RexFieldAccess) {
            RexFieldAccess fieldAccess = (RexFieldAccess) rex;
            // TODO jvs 27-May-2004:  rex-to-Java field name translation
            return new FieldAccess(
                    go(fieldAccess.getReferenceExpr()),
                    fieldAccess.getName());
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
            final WhichInputResult inputAndCol = whichInput(range.offset, _rel);
            if (inputAndCol == null) {
                throw Util.newInternal("input not found");
            }
            final SaffronType inputRowType = inputAndCol._input.getRowType();
            // Simple case is if the range refers to every field of the
            // input. Return the whole input.
            final Variable inputExpr =
                    _implementor.findInputVariable(inputAndCol._input);
            final SaffronType rangeType = range.getType();
            if (inputAndCol._fieldIndex == 0 &&
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
                        inputRowFields[inputAndCol._fieldIndex + i].getName()));
            }
            return new AllocationExpression(toOJClass(rangeType), args);
        }
        throw Util.needToImplement("Translate row-expression of kind " +
                rex.getKind() + "(" + rex + ")");
    }

    /**
     * Converts a {@link RexLiteral} to a Java expression.
     */
    protected Expression convertLiteral(RexLiteral literal)
    {
        // Refer to RexLiteral.valueMatchesType for the type/value combinations
        // we need to handle here.
        final Object value = literal.getValue();
        Calendar calendar;
        long timeInMillis;
        switch (literal._typeName.ordinal_) {
        case SqlTypeName.Null_ordinal:
            return Literal.constantNull();
        case SqlTypeName.Char_ordinal:
            return Literal.makeLiteral(((NlsString) value).getValue());
        case SqlTypeName.Boolean_ordinal:
            return Literal.makeLiteral((Boolean) value);
        case SqlTypeName.Decimal_ordinal:
            BigDecimal bd = (BigDecimal) value;
            if (bd.scale() > 0) {
                // If the value is a fraction, translate to a double.
                return Literal.makeLiteral(bd.doubleValue());
            } else {
                // Fit the value into an int if possible, otherwise long.
                long longValue = ((BigDecimal) value).longValue();
                int intValue = (int) longValue;
                if (longValue == intValue) {
                    return Literal.makeLiteral(intValue);
                } else {
                    return Literal.makeLiteral(longValue);
                }
            }
        case SqlTypeName.Double_ordinal:
            return Literal.makeLiteral(((BigDecimal) value).doubleValue());
        case SqlTypeName.Binary_ordinal:
            return convertByteArrayLiteral((byte []) value);
        case SqlTypeName.Bit_ordinal:
            byte[] bytes = ((BitString) value).getAsByteArray();
            return convertByteArrayLiteral(bytes);
        case SqlTypeName.Timestamp_ordinal:
            calendar = (Calendar) value;
            timeInMillis = calendar.getTimeInMillis();
            return Literal.makeLiteral(new Timestamp(timeInMillis));
        case SqlTypeName.Time_ordinal:
            calendar = (Calendar) value;
            timeInMillis = calendar.getTimeInMillis();
            return Literal.makeLiteral(new Time(timeInMillis));
        case SqlTypeName.Date_ordinal:
            calendar = (Calendar) value;
            timeInMillis = calendar.getTimeInMillis();
            return Literal.makeLiteral(new Date(timeInMillis));
        default:
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
     * @return  a {@link WhichInputResult}
     *     if found, otherwise null.
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
            this._input = input;
            this._inputIndex = inputIndex;
            this._fieldIndex = fieldIndex;
        }
        final SaffronRel _input;
        final int _inputIndex;
        final int _fieldIndex;
    }

    /**
     * Converts a rex expression to a Java expression.
     */
    protected Expression convertCall(RexCall call, Expression[] operands) {
        SqlOperator op = call.op;
        // todo: Store the implementor in a mapping class (maybe this one!)
        //   rather than asking the operator for it.
        final SqlOperator.JavaRexImplementor javaImplementor =
                op.getJavaImplementor();
        if (javaImplementor != null) {
            return javaImplementor.translateToJava(call, operands, this);
        }
        if (op instanceof SqlBinaryOperator) {
            Integer binaryExpNum =
                    _implementor.getBinaryExpressionOrdinal(call.op);
            if (binaryExpNum != null) {
                return new BinaryExpression(
                        operands[0],
                        binaryExpNum.intValue(),
                        operands[1]);
            }
        } else if (op instanceof SqlFunction) {
            if (call.op.equals(_implementor._rexBuilder._opTab.castFunc)) {
                OJClass type = ((OJTypeFactory)
                        _implementor._rexBuilder.getTypeFactory())
                        .toOJClass(null, call.getType());
                return new CastExpression(type, operands[0]);
            }
        }
        switch (op.kind.getOrdinal()) {
        case SqlKind.IsTrueORDINAL:
            return operands[0];
        default:
        }
        // TODO: Get rid of this if-else-if... logic: use a table to do
        // the mapping of rex calls to oj expressions. canConvertCall
        // should use the same table.
        throw Util.needToImplement("Row-expression " + call);
    }

    /**
     * Similar to {@link #convertCall}, but doesn't convert, just returns
     * whether the call can be converted to Java.
     */
    protected boolean canConvertCall(RexCall call) {
        SqlOperator op = call.op;
        if (op instanceof SqlBinaryOperator) {
            Integer binaryExpNum =
                    _implementor.getBinaryExpressionOrdinal(call.op);
            if (binaryExpNum != null) {
                return true;
            }
        } else if (op instanceof SqlFunction) {
            if (call.op.equals(_implementor._rexBuilder._opTab.castFunc)) {
                return true;
            }
        }
        switch (op.kind.getOrdinal()) {
        case SqlKind.IsTrueORDINAL:
            return true;
        default:
        }
        return false;
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

    /**
     * Converts an expression to the equivalent expression in a primitive
     * Java class.
     *
     * @param expr Expression to convert.
     * @param clazz Target class, must be a primitive Java class.
     * @return Converted expression.
     */
    public Expression convertToJava(Expression expr, Class clazz) {
        return expr;
    }

    protected JavaRelImplementor getImplementor()
    {
        return _implementor;
    }

    public void addMember(MemberDeclaration member) {
        throw Util.needToImplement("addMember");
    }

    public void addStatement(Statement stmt) {
        throw Util.needToImplement("addStatement");
    }

    public Variable createScratchVariable(OJClass ojClass, ExpressionList exprs,
                                          MemberDeclarationList mdlst) {
        throw Util.needToImplement("createScratchVariable");
    }
    
    public Variable createScratchVariable(OJClass ojClass) {
        return createScratchVariable(ojClass, null, null);
    }
   
}

// End RexToJavaTranslator.java
