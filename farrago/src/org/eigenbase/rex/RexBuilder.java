/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.Calendar;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.NlsString;
import org.eigenbase.util.Util;


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
    private final RexLiteral varcharEmpty;
    private final RexLiteral constantNull;
    public final SqlStdOperatorTable opTab = SqlStdOperatorTable.instance();

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
        this.varcharEmpty =
            makeLiteral(
                new NlsString("", null, null),
                typeFactory.createSqlType(SqlTypeName.Varchar, 0),
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
                range.offset + field.getIndex(),
                field.getType());
        }
        return new RexFieldAccess(expr, field);
    }

    /**
     * Creates a call with 1 argument, converting a {@link RexKind}
     * to an appropriate {@link SqlOperator}.
     */
    public RexNode makeCall(
        RexKind kind,
        RexNode arg0)
    {
        final RexNode [] args = new RexNode [] { arg0 };
        return makeCall(kind, args);
    }

    /**
     * Creates a call with an array of arguments, converting a {@link RexKind}
     * to an appropriate {@link SqlOperator}.
     */
    public RexNode makeCall(
        RexKind kind,
        final RexNode [] args)
    {
        SqlOperator op = getFunctionOp(kind, args);
        if (op == null) {
            throw Util.newInternal("No operator for " + kind);
        }
        return makeCall(op, args);
    }

    /**
     * Creates a call with 2 arguments, converting a {@link RexKind}
     * to an appropriate {@link SqlOperator}.
     */
    public RexNode makeCall(
        RexKind kind,
        RexNode arg0,
        RexNode arg1)
    {
        final RexNode [] args = new RexNode [] { arg0, arg1 };
        SqlOperator op = getFunctionOp(kind, args);
        if (op == null) {
            throw Util.newInternal("No operator for " + kind);
        }
        return makeCall(op, args);
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
        final RelDataType type = op.getType(typeFactory, exprs);
        return new RexCall(type, op, exprs);
    }

    private RelDataType [] getTypes(RexNode [] exprs)
    {
        RelDataType [] types = new RelDataType[exprs.length];
        for (int i = 0; i < types.length; i++) {
            types[i] = exprs[i].getType();
        }
        return types;
    }

    /**
     * Chooses an appropriate operator to implement a {@link RexKind}, given
     * a specific number and types of arguments.
     */
    public SqlOperator getFunctionOp(
        RexKind kind,
        RexNode [] args)
    {
        SqlFunction function =
            SqlUtil.lookupRoutine(
                opTab,
                new SqlIdentifier(kind.getName(), null),
                getTypes(args),
                false);
        if (function == null) {
            throw Util.newInternal("No operator for " + kind);
        }
        return function;
    }

    /**
     * Creates a call to a windowed agg.
     */
    public RexNode makeOver(RelDataType type,
        SqlOperator operator,
        RexNode[] exprs,
        SqlWindow window,
        SqlNode lowerBound,
        SqlNode upperBound,
        boolean physical)
    {
        return new RexOver(type, operator, exprs, window, lowerBound,
            upperBound, physical);
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
            opTab.newOperator,
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
            opTab.castFunc,
            new RexNode [] { exp });
    }

    /**
     * Creates a reference to all the fields in the row. That is, the whole
     * row as a single record object.
     * @param rowType Type of the input row
     */
    public RexNode makeRangeReference(RelDataType rowType)
    {
        return new RexRangeRef(rowType, 0);
    }

    /**
     * Creates a reference to all the fields in the row.
     *
     * For example, if the input row has type <code>T{f0,f1,f2,f3,f4}</code>
     * then <code>makeRangeReference(T{f0,f1,f2,f3,f4}, S{f3,f4}, 3)</code>
     * is an expression which yields the last 2 fields.
     *
     * @param type    Type of the resulting range record.
     * @param i       Index of first field
     */
    public RexNode makeRangeReference(
        RelDataType type,
        int i)
    {
        return new RexRangeRef(type, i);
    }

    public RexNode makeInputRef(
        RelDataType type,
        int i)
    {
        if (SqlTypeUtil.inCharFamily(type)) {
            Charset charset =
                (type.getCharset() == null) ? Util.getDefaultCharset()
                : type.getCharset();
            SqlCollation collation =
                (type.getCollation() == null)
                ? new SqlCollation(SqlCollation.Coercibility.Implicit)
                : type.getCollation();

            //todo: should get the implicit collation from repository instead of null
            type =
                typeFactory.createTypeWithCharsetAndCollation(type, charset,
                    collation);
        }

        return new RexInputRef(i, type);
    }

    protected RexLiteral makeLiteral(
        Object o,
        RelDataType type,
        SqlTypeName typeName)
    {
        // All literals except NULL have NOT NULL types.
        type = typeFactory.createTypeWithNullability(type, o == null);
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
     * Creates an integer literal.
     */
    public RexLiteral makeExactLiteral(BigDecimal bd)
    {
        SqlTypeName result;
        if (bd.scale() > 0) {
            result = SqlTypeName.Double;
        } else {
            long l = bd.longValue();
            if ((l >= Integer.MIN_VALUE) && (l <= Integer.MAX_VALUE)) {
                result = SqlTypeName.Integer;
            } else {
                result = SqlTypeName.Bigint;
            }
        }

        return makeLiteral(
            bd,
            typeFactory.createSqlType(result),
            SqlTypeName.Decimal);
    }

    /**
     * Creates a byte array literal.
     */
    public RexLiteral makeBinaryLiteral(byte [] byteArray)
    {
        return makeLiteral(
            byteArray,
            typeFactory.createSqlType(SqlTypeName.Varbinary, byteArray.length),
            SqlTypeName.Binary);
    }

    /**
     * Creates a double-precision literal.
     */
    public RexLiteral makeApproxLiteral(BigDecimal bd)
    {
        return makeLiteral(
            bd,
            typeFactory.createSqlType(SqlTypeName.Double),
            SqlTypeName.Double);
    }

    /**
     * Creates a varchar literal.
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
            return varcharEmpty;
        } else {
            return makeLiteral(
                new NlsString(s, null, null),
                typeFactory.createSqlType(
                    SqlTypeName.Varchar,
                    s.length()),
                SqlTypeName.Char);
        }
    }

    /**
     * Creates a String literal
     * @pre str != null
     */
    public RexLiteral makeCharLiteral(NlsString str)
    {
        Util.pre(str != null, "str != null");
        if (null == str.getCharset()) {
            str.setCharset(Util.getDefaultCharset());
        }
        if (null == str.getCollation()) {
            str.setCollation(
                new SqlCollation(SqlCollation.Coercibility.Coercible));
        }
        RelDataType type =
            typeFactory.createSqlType(
                SqlTypeName.Varchar,
                str.getValue().length());
        type =
            typeFactory.createTypeWithCharsetAndCollation(
                type,
                str.getCharset(),
                str.getCollation());
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

    public RexDynamicParam makeDynamicParam(
        RelDataType type,
        int index)
    {
        return new RexDynamicParam(type, index);
    }

    public RexLiteral makeSymbolLiteral(SqlSymbol flag)
    {
        Util.pre(flag != null, "flag != null");
        return makeLiteral(
            flag,
            typeFactory.createSqlType(SqlTypeName.Symbol),
            SqlTypeName.Symbol);
    }
}


// End RexBuilder.java
