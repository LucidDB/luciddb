/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2003-2003 Disruptive Technologies, Inc.
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
package net.sf.saffron.rex;


import net.sf.saffron.core.SaffronField;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.core.SaffronTypeFactory;
import net.sf.saffron.sql.*;
import net.sf.saffron.sql.fun.SqlStdOperatorTable;
import net.sf.saffron.sql.type.SqlTypeName;
import net.sf.saffron.util.BitString;
import net.sf.saffron.util.NlsString;
import net.sf.saffron.util.Util;

import java.math.BigDecimal;
import java.util.Calendar;
import java.nio.charset.Charset;

/**
 * Factory for row expressions.
 *
 * <p>Some common literal values (NULL, TRUE, FALSE, 0, 1, '') are cached.</p>
 *
 * @author jhyde
 * @since Nov 23, 2003
 * @version $Id$
 **/
public class RexBuilder {
    protected final SaffronTypeFactory _typeFactory;
    private final RexLiteral _booleanTrue;
    private final RexLiteral _booleanFalse;
    private final RexLiteral _varcharEmpty;
    private final RexLiteral _constantNull;
    public final SqlStdOperatorTable _opTab = SqlOperatorTable.std();

    // REVIEW jvs 22-Jan-2004: I changed this constructor from protected to
    // public so that unit tests needn't depend on oj.  If RexBuilder
    // isn't supposed to be instantiated, then it should be declared abstrct.
    public RexBuilder(SaffronTypeFactory typeFactory) {
        this._typeFactory = typeFactory;
        this._booleanTrue = makeLiteral(Boolean.TRUE,
                typeFactory.createSqlType(SqlTypeName.Boolean),
                SqlTypeName.Boolean);
        this._booleanFalse = makeLiteral(Boolean.FALSE,
                typeFactory.createSqlType(SqlTypeName.Boolean),
                SqlTypeName.Boolean);
        this._varcharEmpty = makeLiteral(new NlsString("",null,null),
                typeFactory.createSqlType(SqlTypeName.Varchar, 0),
                SqlTypeName.Char);
        this._constantNull = makeLiteral(null,
                typeFactory.createSqlType(SqlTypeName.Null),
                SqlTypeName.Null);
    }

    public SaffronTypeFactory getTypeFactory() {
        return _typeFactory;
    }

    public RexNode makeFieldAccess(RexNode expr, String fieldName) {
        final SaffronType type = expr.getType();
        final SaffronField field = type.getField(fieldName);
        if (field == null) {
            throw Util.newInternal("Type '" + type + "' has no field '" +
                    fieldName + "'");
        }
        return makeFieldAccessInternal(expr, field);
    }

    public RexNode makeFieldAccess(RexNode expr, int i) {
        final SaffronType type = expr.getType();
        final SaffronField [] fields = type.getFields();
        if (i < 0 || i >= fields.length) {
            throw Util.newInternal("Field ordinal " + i + " is invalid for " +
                    " type '" + type + "'");
        }
        return makeFieldAccessInternal(expr, fields[i]);
    }

    private RexNode makeFieldAccessInternal(RexNode expr, final SaffronField field) {
        if (expr instanceof RexRangeRef) {
            RexRangeRef range = (RexRangeRef) expr;
            return new RexInputRef(range.offset + field.getIndex(),
                    field.getType());
        }
        return new RexFieldAccess(expr,field);
    }

    /**
     * Creates a call with 1 argument, converting a {@link RexKind}
     * to an appropriate {@link SqlOperator}.
     */
    public RexNode makeCall(RexKind kind, RexNode arg0) {
        final RexNode[] args = new RexNode[] {arg0};
        return makeCall(kind, args);
    }

    /**
     * Creates a call with an array of arguments, converting a {@link RexKind}
     * to an appropriate {@link SqlOperator}.
     */
    public RexNode makeCall(RexKind kind, final RexNode[] args) {
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
    public RexNode makeCall(RexKind kind, RexNode arg0, RexNode arg1) {
        final RexNode[] args = new RexNode[] {arg0, arg1};
        SqlOperator op = getFunctionOp(kind, args);
        if (op == null) {
            throw Util.newInternal("No operator for " + kind);
        }
        return makeCall(op, args);
    }

    /**
     * Creates a call with 1 argument.
     */
    public RexNode makeCall(SqlOperator op, RexNode expr0) {
        return makeCall(op, new RexNode[] {expr0});
    }

    /**
     * Creates a call with 2 arguments.
     */
    public RexNode makeCall(SqlOperator op, RexNode expr0, RexNode expr1) {
        return makeCall(op, new RexNode[] {expr0, expr1});
    }

    /**
     * Creates a call with an array of arguments.
     *
     * <p>This is the fundamental method called by all of the other
     * <code>makeCall</code> methods. If you derive a class from
     * {@link RexBuilder}, this is the only method you need to override.</p>
     */
    public RexNode makeCall(SqlOperator op, RexNode[] exprs) {
        final SaffronType type = op.getType(_typeFactory, exprs);
        return new RexCall(type, op, exprs);
    }

    private SaffronType[] getTypes(RexNode[] exprs) {
        SaffronType[] types = new SaffronType[exprs.length];
        for (int i = 0; i < types.length; i++) {
            types[i] = exprs[i].getType();
        }
        return types;
    }

    /**
     * Chooses an appropriate operator to implement a {@link RexKind}, given
     * a specific number and types of arguments.
     */
    public SqlOperator getFunctionOp(RexKind kind, RexNode[] args) {
        SqlFunction function = _opTab.lookupFunction(kind.getName(),
                getTypes(args));
        if(function == null) {
            throw Util.newInternal("No operator for " + kind);
        }
        return function;
    }

    /**
     * Creates a constant for the SQL <code>NULL</code> value.
     */
    public RexLiteral constantNull() {
        return _constantNull;
    }

    public RexNode makeCorrel(SaffronType type, String name) {
        return new RexCorrelVariable(name, type);
    }

    public RexNode makeCast(SaffronType type, RexNode exp) {
        return makeAbstractCast(type,exp);
    }

    public RexNode makeAbstractCast(SaffronType type, RexNode exp) {
        return new RexCall(type, _opTab.castFunc, new RexNode[] {exp});
    }

    /**
     * Creates a reference to all the fields in the row. That is, the whole
     * row as a single record object.
     * @param rowType Type of the input row
     */
    public RexNode makeRangeReference(SaffronType rowType) {
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
    public RexNode makeRangeReference(SaffronType type, int i) {
        return new RexRangeRef(type, i);
    }

    public RexNode makeInputRef(SaffronType type,int i) {
        if (type.isCharType()) {
            Charset charset = type.getCharset()==null?
                    Util.getDefaultCharset() :
                    type.getCharset();
            SqlCollation collation = type.getCollation()==null?
                    new SqlCollation(SqlCollation.Coercibility.Implicit) :
                    type.getCollation();
            //todo: should get the implicit collation from repository instead of null
            type = _typeFactory.createTypeWithCharsetAndCollation(type,
                    charset, collation);
        }

        return new RexInputRef(i,type);
    }

    protected RexLiteral makeLiteral(Object o, SaffronType type,
            SqlTypeName typeName) {
        // All literals except NULL have NOT NULL types.
        type = _typeFactory.createTypeWithNullability(type, o == null);
        return new RexLiteral(o, type, typeName);
    }

    /**
     * Creates a boolean literal.
     */
    public RexLiteral makeLiteral(boolean b)
    {
        return b ? _booleanTrue : _booleanFalse;
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
            if (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) {
                result = SqlTypeName.Integer;
            } else {
                result = SqlTypeName.Bigint;
            }
        }

        return makeLiteral(bd,
                _typeFactory.createSqlType(result),
                SqlTypeName.Decimal);
    }

    /**
     * Creates a byte array literal.
     */
    public RexLiteral makeBinaryLiteral(byte[] byteArray)
    {
        return makeLiteral(byteArray,
                _typeFactory.createSqlType(SqlTypeName.Varbinary,
                        byteArray.length),
                SqlTypeName.Binary);
    }

    /**
     * Creates a double-precision literal.
     */
    public RexLiteral makeApproxLiteral(BigDecimal bd) {
        return makeLiteral(bd,
                _typeFactory.createSqlType(SqlTypeName.Double),
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
            return _varcharEmpty;
        } else {
            return makeLiteral(new NlsString(s, null, null),
                    _typeFactory.createSqlType(SqlTypeName.Varchar,
                            s.length()),
                    SqlTypeName.Char);
        }
    }

    /**
     * Creates a Bit String literal
     * @pre bitString != null
     */
    public RexLiteral makeBitLiteral(BitString bitString) {
        Util.pre(bitString != null, "bitString != null");
        return makeLiteral(bitString,
                _typeFactory.createSqlType(SqlTypeName.Bit,
                        bitString.getBitCount()),
                SqlTypeName.Bit);
    }

    /**
     * Creates a String literal
     * @pre str != null
     */
    public RexLiteral makeCharLiteral(NlsString str) {
        Util.pre(str != null, "str != null");
        if (null==str.getCharset()) {
            str.setCharset(Util.getDefaultCharset());
        }
        if (null==str.getCollation()) {
            str.setCollation(new
                    SqlCollation(SqlCollation.Coercibility.Coercible));
        }
        SaffronType type = _typeFactory.createSqlType(SqlTypeName.Varchar,
                str.getValue().length());
        type=_typeFactory.createTypeWithCharsetAndCollation(type,str.getCharset(),
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
        return makeLiteral(date,
                _typeFactory.createSqlType(SqlTypeName.Date),
                SqlTypeName.Date);
    }

    /**
     * Creates a Time literal.
     * @pre time != null
     */
    public RexLiteral makeTimeLiteral(Calendar time, int precision)
    {
        Util.pre(time != null, "time != null");
        return makeLiteral(time,
                _typeFactory.createSqlType(SqlTypeName.Time, precision),
                SqlTypeName.Time);
    }

    /**
     * Creates a Timestamp literal.
     * @pre timestamp != null
     */
    public RexLiteral makeTimestampLiteral(Calendar timestamp, int precision)
    {
        Util.pre(timestamp != null, "timestamp != null");
        return makeLiteral(timestamp,
                _typeFactory.createSqlType(SqlTypeName.Timestamp, precision),
                SqlTypeName.Timestamp);
    }

    public RexDynamicParam makeDynamicParam(SaffronType type,int index)
    {
        return new RexDynamicParam(type,index);
    }

    public RexContextVariable makeContextVariable(String name,SaffronType type)
    {
        return new RexContextVariable(name,type);
    }

    public RexLiteral makeSymbolLiteral(SqlSymbol flag) {
        Util.pre(flag != null, "flag != null");
        return makeLiteral(flag,
                _typeFactory.createSqlType(SqlTypeName.Symbol),
                SqlTypeName.Symbol);
    }
}

// End RexBuilder.java
