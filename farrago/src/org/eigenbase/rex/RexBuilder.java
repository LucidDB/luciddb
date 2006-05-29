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

package org.eigenbase.rex;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.EnumeratedValues;
import org.eigenbase.util.NlsString;
import org.eigenbase.util.Util;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Calendar;


/**
 * Factory for row expressions.
 *
 * <p>Some common literal values (NULL, TRUE, FALSE, 0, 1, '') are cached.</p>
 *
 * @author jhyde
 * @since Nov 23, 2003
 * @version $Id$
 **/
public class RexBuilder
{
    //~ Instance fields -------------------------------------------------------

    protected final RelDataTypeFactory typeFactory;
    private final RexLiteral booleanTrue;
    private final RexLiteral booleanFalse;
    private final RexLiteral charEmpty;
    private final RexLiteral constantNull;
    private final SqlStdOperatorTable opTab = SqlStdOperatorTable.instance();

    //~ Constructors ----------------------------------------------------------

    // REVIEW jvs 22-Jan-2005: I changed this constructor from protected to
    // public so that unit tests needn't depend on oj.  If RexBuilder
    // isn't supposed to be instantiated, then it should be declared abstrct.
    public RexBuilder(RelDataTypeFactory typeFactory)
    {
        this.typeFactory = typeFactory;
        this.booleanTrue =
            makeLiteral(
                Boolean.TRUE,
                typeFactory.createSqlType(SqlTypeName.Boolean),
                SqlTypeName.Boolean);
        this.booleanFalse =
            makeLiteral(
                Boolean.FALSE,
                typeFactory.createSqlType(SqlTypeName.Boolean),
                SqlTypeName.Boolean);
        this.charEmpty =
            makeLiteral(
                new NlsString("", null, null),
                typeFactory.createSqlType(SqlTypeName.Char, 0),
                SqlTypeName.Char);
        this.constantNull =
            makeLiteral(
                null,
                typeFactory.createSqlType(SqlTypeName.Null),
                SqlTypeName.Null);
    }

    //~ Methods ---------------------------------------------------------------

    public RelDataTypeFactory getTypeFactory()
    {
        return typeFactory;
    }

    public SqlStdOperatorTable getOpTab()
    {
        return opTab;
    }

    public RexNode makeFieldAccess(
        RexNode expr,
        String fieldName)
    {
        final RelDataType type = expr.getType();
        final RelDataTypeField field = type.getField(fieldName);
        if (field == null) {
            throw Util.newInternal("Type '" + type + "' has no field '"
                + fieldName + "'");
        }
        return makeFieldAccessInternal(expr, field);
    }

    public RexNode makeFieldAccess(
        RexNode expr,
        int i)
    {
        final RelDataType type = expr.getType();
        final RelDataTypeField [] fields = type.getFields();
        if ((i < 0) || (i >= fields.length)) {
            throw Util.newInternal("Field ordinal " + i + " is invalid for "
                + " type '" + type + "'");
        }
        return makeFieldAccessInternal(expr, fields[i]);
    }

    private RexNode makeFieldAccessInternal(
        RexNode expr,
        final RelDataTypeField field)
    {
        if (expr instanceof RexRangeRef) {
            RexRangeRef range = (RexRangeRef) expr;
            return new RexInputRef(
                range.getOffset() + field.getIndex(),
                field.getType());
        }
        return new RexFieldAccess(expr, field);
    }

    /**
     * Creates a call with 1 argument.
     */
    public RexNode makeCall(
        SqlOperator op,
        RexNode expr0)
    {
        return makeCall(
            op,
            new RexNode [] { expr0 });
    }

    /**
     * Creates a call with 2 arguments.
     */
    public RexNode makeCall(
        SqlOperator op,
        RexNode expr0,
        RexNode expr1)
    {
        return makeCall(
            op,
            new RexNode [] { expr0, expr1 });
    }

    /**
     * Creates a call with an array of arguments.
     *
     * <p>This is the fundamental method called by all of the other
     * <code>makeCall</code> methods. If you derive a class from
     * {@link RexBuilder}, this is the only method you need to override.</p>
     */
    public RexNode makeCall(
        SqlOperator op,
        RexNode [] exprs)
    {
        final RelDataType type = deriveReturnType(op, typeFactory, exprs);
        RexNode [] fixExprs = exprs;
        if (type instanceof IntervalSqlType) {
            //if (op instanceof SqlDatetimeSubtractionOperator) {
            //    op = SqlStdOperatorTable.minusOperator;
            //}
            int count = 0;
            for (int i = 0; i < exprs.length; i++) {
                if (exprs[i] instanceof RexLiteral &&
                    exprs[i].getType() instanceof IntervalSqlType &&
                    exprs[i].getType().getIntervalQualifier() != null) {
                    exprs[i] = null;
                    continue;
                }
                count++;
            }
            fixExprs = new RexNode[count];
            for (int i = 0; i < exprs.length; i++) {
                if (exprs[i] == null) {
                    continue;
                }
                fixExprs[i] = exprs[i];
            }
        }
        return new RexCall(type, op, fixExprs);
    }

    /**
     * Derives the return type of a call to an operator.
     *
     * @param op the operator being called
     *
     * @param typeFactory factory for return type
     *
     * @param exprs actual operands
     *
     * @return derived type
     */
    public RelDataType deriveReturnType(
        SqlOperator op,
        RelDataTypeFactory typeFactory,
        RexNode [] exprs)
    {
        return op.inferReturnType(
            new RexCallBinding(typeFactory, op, exprs));
    }

    /**
     * Creates a call to a windowed agg.
     */
    public RexNode makeOver(
        RelDataType type,
        SqlAggFunction operator,
        RexNode[] exprs,
        RexNode[] partitionKeys,
        RexNode[] orderKeys,
        SqlNode lowerBound,
        SqlNode upperBound,
        boolean physical)
    {
        assert operator != null;
        assert exprs != null;
        assert partitionKeys != null;
        assert orderKeys != null;
        final RexWindow window = new RexWindow(
            partitionKeys, orderKeys, lowerBound, upperBound, physical);
        return new RexOver(type, operator, exprs, window);
    }

    /**
     * Creates a constant for the SQL <code>NULL</code> value.
     */
    public RexLiteral constantNull()
    {
        return constantNull;
    }

    public RexNode makeCorrel(
        RelDataType type,
        String name)
    {
        return new RexCorrelVariable(name, type);
    }

    public RexNode makeNewInvocation(
        RelDataType type,
        RexNode [] exprs)
    {
        return new RexCall(
            type,
            SqlStdOperatorTable.newOperator,
            exprs);
    }

    public RexNode makeCast(
        RelDataType type,
        RexNode exp)
    {
        return makeAbstractCast(type, exp);
    }

    public RexNode makeAbstractCast(
        RelDataType type,
        RexNode exp)
    {
        return new RexCall(
            type,
            SqlStdOperatorTable.castFunc,
            new RexNode [] { exp });
    }

    /**
     * Makes a reinterpret cast
     * 
     * @param type type returned by the cast
     * @param exp expression to be casted
     * @param checkOverflow whether an overflow check is required
     *
     * @return a RexCall with two operands and a special return type
     */
    public RexNode makeReinterpretCast(
        RelDataType type,
        RexNode exp,
        RexNode checkOverflow)
    {
        RexNode[] args;
        if (checkOverflow != null && checkOverflow.isAlwaysTrue()) {
            args = new RexNode [] { exp, checkOverflow };
        } else {
            args = new RexNode [] { exp };
        }
        return new RexCall(
            type,
            SqlStdOperatorTable.reinterpretOperator,
            args);
    }
    
    /**
     * Creates a reference to all the fields in the row. That is, the whole
     * row as a single record object.
     *
     * @param rowType Type of the input row.
     */
    public RexNode makeRangeReference(RelDataType rowType)
    {
        return new RexRangeRef(rowType, 0);
    }

    /**
     * Creates a reference to all the fields in the row.
     *
     * <p>For example, if the input row has type <code>T{f0,f1,f2,f3,f4}</code>
     * then <code>makeRangeReference(T{f0,f1,f2,f3,f4}, S{f3,f4}, 3)</code>
     * is an expression which yields the last 2 fields.
     *
     * @param type     Type of the resulting range record.
     * @param offset   Index of first field.
     * @param nullable Whether the record is nullable.
     */
    public RexNode makeRangeReference(
        RelDataType type,
        int offset,
        boolean nullable)
    {
        if (nullable && !type.isNullable()) {
            type = typeFactory.createTypeWithNullability(
                type, nullable);
        }
        return new RexRangeRef(type, offset);
    }

    public RexNode makeInputRef(
        RelDataType type,
        int i)
    {
        type = SqlTypeUtil.addCharsetAndCollation(type, typeFactory);
        return new RexInputRef(i, type);
    }

    /**
     * Creates a literal representing a flag.
     */
    public RexLiteral makeFlag(
        EnumeratedValues.Value flag)
    {
        assert flag != null;
        return makeLiteral(
            flag,
            typeFactory.createSqlType(SqlTypeName.Symbol),
            SqlTypeName.Symbol);
    }

    protected RexLiteral makeLiteral(
        Comparable o,
        RelDataType type,
        SqlTypeName typeName)
    {
        // All literals except NULL have NOT NULL types.
        type = typeFactory.createTypeWithNullability(type, o == null);
        if (typeName == SqlTypeName.Char) {
            // Character literals must have a charset and collation. Populate
            // from the type if necessary.
            NlsString nlsString = (NlsString) o;
            if (nlsString.getCollation() == null ||
                nlsString.getCharset() == null) {
                assert type.getSqlTypeName() == SqlTypeName.Char;
                assert type.getCharset().name() != null;
                assert type.getCollation() != null;
                o = new NlsString(
                    nlsString.getValue(),
                    type.getCharset().name(),
                    type.getCollation());
            }
        }
        return new RexLiteral(o, type, typeName);
    }

    /**
     * Creates a boolean literal.
     */
    public RexLiteral makeLiteral(boolean b)
    {
        return b ? booleanTrue : booleanFalse;
    }

    /**
     * Creates a numeric literal.
     */
    public RexLiteral makeExactLiteral(BigDecimal bd)
    {
        RelDataType relType;
        int scale = bd.scale();
        long l = bd.unscaledValue().longValue();
        assert(scale >= 0 && scale <= SqlTypeName.MAX_NUMERIC_SCALE);
        assert(BigDecimal.valueOf(l, scale).equals(bd));
        if (scale == 0) {
            if ((l >= Integer.MIN_VALUE) && (l <= Integer.MAX_VALUE)) {
                relType = typeFactory.createSqlType(SqlTypeName.Integer);
            } else {
                relType = typeFactory.createSqlType(SqlTypeName.Bigint);
            }
        } else {
            int precision = bd.unscaledValue().toString().length();
            relType =
                typeFactory.createSqlType(
                    SqlTypeName.Decimal,
                    scale,
                    precision);
        }
        return makeExactLiteral(bd, relType);
    }

    /**
     * Creates a numeric literal.
     */
    public RexLiteral makeExactLiteral(BigDecimal bd, RelDataType type)
    {
        return makeLiteral(
            bd,
            type,
            SqlTypeName.Decimal);
    }

    /**
     * Creates a byte array literal.
     */
    public RexLiteral makeBinaryLiteral(byte [] byteArray)
    {
        return makeLiteral(
            ByteBuffer.wrap(byteArray),
            typeFactory.createSqlType(SqlTypeName.Varbinary, byteArray.length),
            SqlTypeName.Binary);
    }

    /**
     * Creates a double-precision literal.
     */
    public RexLiteral makeApproxLiteral(BigDecimal bd)
    {
        // Validator should catch if underflow is allowed
        // If underflow is allowed, let underflow become zero
        if (bd.doubleValue() == 0) {
            bd = BigDecimal.ZERO;
        }
        return makeApproxLiteral(
            bd,
            typeFactory.createSqlType(SqlTypeName.Double));
    }

    /**
     * Creates an approximate numeric literal (double or float).
     *
     * @param bd literal value
     *
     * @param type approximate numeric type
     *
     * @return new literal
     */
    public RexLiteral makeApproxLiteral(BigDecimal bd, RelDataType type)
    {
        assert(SqlTypeFamily.ApproximateNumeric.getTypeNames().contains(
            type.getSqlTypeName()));
        return makeLiteral(
            bd,
            type,
            SqlTypeName.Double);
    }

    /**
     * Creates a character string literal.
     * @pre s != null
     */
    public RexLiteral makeLiteral(String s)
    {
        return makePreciseStringLiteral(s);
    }

    protected RexLiteral makePreciseStringLiteral(String s)
    {
        Util.pre(s != null, "s != null");
        if (s.equals("")) {
            return charEmpty;
        } else {
            return makeLiteral(
                new NlsString(s, null, null),
                typeFactory.createSqlType(
                    SqlTypeName.Char,
                    s.length()),
                SqlTypeName.Char);
        }
    }

    /**
     * Creates a character string literal from an {@link NlsString}.
     *
     * <p>If the string's charset and collation are not set, uses the system
     * defaults.
     *
     * @pre str != null
     */
    public RexLiteral makeCharLiteral(NlsString str)
    {
        Util.pre(str != null, "str != null");
        RelDataType type = SqlUtil.createNlsStringType(typeFactory, str);
        return makeLiteral(str, type, SqlTypeName.Char);
    }

    /**
     * Creates a Date literal.
     * @pre date != null
     */
    public RexLiteral makeDateLiteral(Calendar date)
    {
        Util.pre(date != null, "date != null");
        return makeLiteral(
            date,
            typeFactory.createSqlType(SqlTypeName.Date),
            SqlTypeName.Date);
    }

    /**
     * Creates a Time literal.
     * @pre time != null
     */
    public RexLiteral makeTimeLiteral(
        Calendar time,
        int precision)
    {
        Util.pre(time != null, "time != null");
        return makeLiteral(
            time,
            typeFactory.createSqlType(SqlTypeName.Time, precision),
            SqlTypeName.Time);
    }

    /**
     * Creates a Timestamp literal.
     * @pre timestamp != null
     */
    public RexLiteral makeTimestampLiteral(
        Calendar timestamp,
        int precision)
    {
        Util.pre(timestamp != null, "timestamp != null");
        return makeLiteral(
            timestamp,
            typeFactory.createSqlType(SqlTypeName.Timestamp, precision),
            SqlTypeName.Timestamp);
    }

    /**
     * Creates an interval literal.
     */
    public RexLiteral makeIntervalLiteral(
        SqlIntervalQualifier intervalQualifier)
    {
        Util.pre(intervalQualifier != null, "intervalQualifier != null");
        return makeLiteral(
            null,
            new IntervalSqlType(intervalQualifier, false),
            intervalQualifier.isYearMonth() ? SqlTypeName.IntervalYearMonth :
                SqlTypeName.IntervalDayTime);
    }

    /**
     * Creates an interval literal.
     */
    public RexLiteral makeIntervalLiteral(long l)
    {
        return makeLiteral(
            new BigDecimal(l), typeFactory.createSqlType(SqlTypeName.Bigint),
            SqlTypeName.Decimal);
    }

    public RexDynamicParam makeDynamicParam(
        RelDataType type,
        int index)
    {
        return new RexDynamicParam(type, index);
    }
}


// End RexBuilder.java
