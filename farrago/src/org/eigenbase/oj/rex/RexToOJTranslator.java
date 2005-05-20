/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

package org.eigenbase.oj.rex;

import java.math.*;
import java.util.*;

import openjava.mop.*;
import openjava.ptree.*;

import org.eigenbase.oj.*;
import org.eigenbase.oj.rel.*;
import org.eigenbase.oj.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * Converts expressions in logical format ({@link RexNode}) into
 * OpenJava code.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class RexToOJTranslator implements RexVisitor
{
    //~ Instance fields -------------------------------------------------------

    private final JavaRelImplementor implementor;
    private final RelNode contextRel;
    private final OJRexImplementorTable implementorTable;
    private Expression translatedExpr;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a translator.
     *
     * @param implementor implementation context
     *
     * @param contextRel relational expression which is the context for the
     *   row-expressions which are to be translated
     *
     * @param implementorTable table of implementation functors for Rex
     * operators; if null, {@link OJRexImplementorTableImpl#instance} is used
     */
    public RexToOJTranslator(
        JavaRelImplementor implementor,
        RelNode contextRel,
        OJRexImplementorTable implementorTable)
    {
        if (implementorTable == null) {
            implementorTable = OJRexImplementorTableImpl.instance();
        }

        this.implementor = implementor;
        this.contextRel = contextRel;
        this.implementorTable = implementorTable;
    }

    //~ Methods ---------------------------------------------------------------

    protected void setTranslation(Expression expr)
    {
        translatedExpr = expr;
    }

    protected Expression getTranslation()
    {
        return translatedExpr;
    }

    protected OJRexImplementorTable getImplementorTable()
    {
        return implementorTable;
    }

    public JavaRelImplementor getRelImplementor()
    {
        return implementor;
    }

    public RelDataTypeFactory getTypeFactory()
    {
        return contextRel.getCluster().typeFactory;
    }

    // implement RexVisitor
    public void visitInputRef(RexInputRef inputRef)
    {
        WhichInputResult inputAndCol = whichInput(inputRef.index, contextRel);
        if (inputAndCol == null) {
            throw Util.newInternal("input not found");
        }
        final Variable v = implementor.findInputVariable(inputAndCol.input);
        RelDataType rowType = inputAndCol.input.getRowType();
        final RelDataTypeField field =
            rowType.getFields()[inputAndCol.fieldIndex];
        final String javaFieldName =
            Util.toJavaId(
                field.getName(),
                inputAndCol.fieldIndex);
        setTranslation(new FieldAccess(v, javaFieldName));
    }

    // implement RexVisitor
    public void visitLiteral(RexLiteral literal)
    {
        // Refer to RexLiteral.valueMatchesType for the type/value combinations
        // we need to handle here.
        final Object value = literal.getValue();
        Calendar calendar;
        long timeInMillis;
        switch (literal.typeName.ordinal) {
        case SqlTypeName.Null_ordinal:
            setTranslation(Literal.constantNull());
            break;
        case SqlTypeName.Char_ordinal:
            setTranslation(
                Literal.makeLiteral(((NlsString) value).getValue()));
            break;
        case SqlTypeName.Boolean_ordinal:
            setTranslation(Literal.makeLiteral((Boolean) value));
            break;
        case SqlTypeName.Decimal_ordinal:
            BigDecimal bd = (BigDecimal) value;
            if (bd.scale() > 0) {
                // If the value is a fraction, translate to a double.
                setTranslation(Literal.makeLiteral(bd.doubleValue()));
                break;
            } else {
                // Fit the value into an int if possible, otherwise long.
                long longValue = ((BigDecimal) value).longValue();
                int intValue = (int) longValue;
                if (longValue == intValue) {
                    setTranslation(Literal.makeLiteral(intValue));
                } else {
                    setTranslation(Literal.makeLiteral(longValue));
                }
                break;
            }
        case SqlTypeName.Double_ordinal:
            setTranslation(
                Literal.makeLiteral(((BigDecimal) value).doubleValue()));
            break;
        case SqlTypeName.Binary_ordinal:
            setTranslation(convertByteArrayLiteral((byte []) value));
            break;
        case SqlTypeName.Date_ordinal:
        case SqlTypeName.Time_ordinal:
        case SqlTypeName.Timestamp_ordinal:
            calendar = (Calendar) value;
            timeInMillis = calendar.getTimeInMillis();
            setTranslation(Literal.makeLiteral(timeInMillis));
            break;
        default:
            throw Util.newInternal("Bad literal value " + value + " ("
                + value.getClass() + "); breaches "
                + "post-condition on RexLiteral.getValue()");
        }
    }

    // implement RexVisitor
    public void visitCall(RexCall call)
    {
        RexNode [] operands = call.getOperands();
        Expression [] exprs = new Expression[operands.length];
        for (int i = 0; i < operands.length; i++) {
            RexNode operand = operands[i];
            exprs[i] = translateRexNode(operand);
        }
        Expression callExpr = convertCall(call, exprs);
        setTranslation(callExpr);
    }

    /**
     * Converts a call after its operands have already been translated.
     *
     * @param call call to be translated
     *
     * @param operandExprs translated operands
     *
     * @return converted call
     */
    protected Expression convertCall(
        RexCall call, Expression [] operandExprs)
    {
        OJRexImplementor implementor =
            implementorTable.get(call.getOperator());
        if (implementor == null) {
            throw Util.needToImplement(call);
        }
        return implementor.implement(this, call, operandExprs);
    }

    // implement RexVisitor
    public void visitOver(RexOver over)
    {
        throw Util.needToImplement("Row-expression RexOver");
    }

    // implement RexVisitor
    public void visitCorrelVariable(RexCorrelVariable correlVariable)
    {
        throw Util.needToImplement("Row-expression RexCorrelVariable");
    }

    // implement RexVisitor
    public void visitDynamicParam(RexDynamicParam dynamicParam)
    {
        throw Util.needToImplement("Row-expression RexDynamicParam");
    }

    // implement RexVisitor
    public void visitRangeRef(RexRangeRef rangeRef)
    {
        final WhichInputResult inputAndCol =
            whichInput(rangeRef.offset, contextRel);
        if (inputAndCol == null) {
            throw Util.newInternal("input not found");
        }
        final RelDataType inputRowType = inputAndCol.input.getRowType();

        // Simple case is if the range refers to every field of the
        // input. Return the whole input.
        final Variable inputExpr =
            implementor.findInputVariable(inputAndCol.input);
        final RelDataType rangeType = rangeRef.getType();
        if ((inputAndCol.fieldIndex == 0) && (rangeType == inputRowType)) {
            setTranslation(inputExpr);
            return;
        }

        // More complex case is if the range refers to a subset of
        // the input's fields. Generate "new Type(argN,...,argM)".
        final RelDataTypeField [] rangeFields = rangeType.getFields();
        final RelDataTypeField [] inputRowFields = inputRowType.getFields();
        final ExpressionList args = new ExpressionList();
        for (int i = 0; i < rangeFields.length; i++) {
            String fieldName =
                inputRowFields[inputAndCol.fieldIndex + i].getName();
            final String javaFieldName = Util.toJavaId(fieldName, i);
            args.add(new FieldAccess(inputExpr, javaFieldName));
        }
        setTranslation(
            new AllocationExpression(
                OJUtil.typeToOJClass(rangeType, getTypeFactory()),
                args));
    }

    // implement RexVisitor
    public void visitFieldAccess(RexFieldAccess fieldAccess)
    {
        final String javaFieldName =
            Util.toJavaId(
                fieldAccess.getName(),
                fieldAccess.getField().getIndex());
        setTranslation(
            new FieldAccess(
                translateRexNode(fieldAccess.getReferenceExpr()),
                javaFieldName));
    }

    public Expression translateRexNode(RexNode node)
    {
        if (node instanceof JavaRowExpression) {
            return ((JavaRowExpression) node).expression;
        } else {
            node.accept(this);
            Expression expr = translatedExpr;
            translatedExpr = null;
            return expr;
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

    public boolean canConvertCall(RexCall call)
    {
        OJRexImplementor implementor =
            implementorTable.get(call.getOperator());
        if (implementor == null) {
            return false;
        }
        return implementor.canImplement(call);
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
    private static WhichInputResult whichInput(
        int fieldIndex,
        RelNode rel)
    {
        assert fieldIndex >= 0;
        final RelNode [] inputs = rel.getInputs();
        for (int inputIndex = 0, firstFieldIndex = 0;
                inputIndex < inputs.length; inputIndex++) {
            RelNode input = inputs[inputIndex];

            // Index of first field in next input. Special case if this
            // input has no fields: it's ambiguous (we could be looking
            // at the first field of the next input) but we allow it.
            final int fieldCount = input.getRowType().getFieldList().size();
            final int lastFieldIndex = firstFieldIndex + fieldCount;
            if ((lastFieldIndex > fieldIndex)
                    || ((fieldCount == 0) && (lastFieldIndex == fieldIndex))) {
                final int fieldIndex2 = fieldIndex - firstFieldIndex;
                return new WhichInputResult(input, inputIndex, fieldIndex2);
            }
            firstFieldIndex = lastFieldIndex;
        }
        return null;
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Result of call to {@link RexToOJTranslator#whichInput}, contains the
     * input relational expression, its index, and the index of the field
     * within that relational expression.
     */
    private static class WhichInputResult
    {
        final RelNode input;
        final int inputIndex;
        final int fieldIndex;

        WhichInputResult(
            RelNode input,
            int inputIndex,
            int fieldIndex)
        {
            this.input = input;
            this.inputIndex = inputIndex;
            this.fieldIndex = fieldIndex;
        }
    }
}


// End RexToOJTranslator.java
