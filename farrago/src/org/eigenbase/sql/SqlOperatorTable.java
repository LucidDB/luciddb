/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

package org.eigenbase.sql;

import java.lang.reflect.Field;
import java.util.*;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.ParserPosition;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.validation.ValidationUtil;
import org.eigenbase.util.MultiMap;
import org.eigenbase.util.Util;


/**
 * <code>SqlOperatorTable</code> is a singleton which contains an instance of
 * each operator.
 */
public class SqlOperatorTable
{
    //~ Static fields/initializers --------------------------------------------

    public static final SqlTypeName [] stringTypes =
    {
        SqlTypeName.Varchar, SqlTypeName.Bit, SqlTypeName.Binary,
        SqlTypeName.Varbinary
    };
    public static final SqlTypeName [] stringNullableTypes =
    {
        SqlTypeName.Null, SqlTypeName.Varchar, SqlTypeName.Bit,
        SqlTypeName.Binary, SqlTypeName.Varbinary
    };
    public static final SqlTypeName [] numericTypes =
    {
        SqlTypeName.Tinyint, SqlTypeName.Smallint, SqlTypeName.Integer,
        SqlTypeName.Bigint, SqlTypeName.Decimal, SqlTypeName.Float,
        SqlTypeName.Real, SqlTypeName.Double
    };
    public static final SqlTypeName [] numericNullableTypes =
    {
        SqlTypeName.Null, SqlTypeName.Tinyint, SqlTypeName.Smallint,
        SqlTypeName.Integer, SqlTypeName.Bigint, SqlTypeName.Decimal,
        SqlTypeName.Float, SqlTypeName.Real, SqlTypeName.Double
    };
    public static final SqlTypeName [] booleanTypes = { SqlTypeName.Boolean };
    public static final SqlTypeName [] booleanNullableTypes =
    { SqlTypeName.Null, SqlTypeName.Boolean };
    public static final SqlTypeName [] binaryNullableTypes =
    { SqlTypeName.Null, SqlTypeName.Bit, SqlTypeName.Varbinary };
    public static final SqlTypeName [] intTypes =
    {
        SqlTypeName.Tinyint, SqlTypeName.Smallint, SqlTypeName.Integer,
        SqlTypeName.Bigint
    };
    public static final SqlTypeName [] intNullableTypes =
    {
        SqlTypeName.Null, SqlTypeName.Tinyint, SqlTypeName.Smallint,
        SqlTypeName.Integer, SqlTypeName.Bigint
    };
    public static final SqlTypeName [] charTypes =
    { SqlTypeName.Char, SqlTypeName.Varchar };
    public static final SqlTypeName [] charNullableTypes =
    { SqlTypeName.Null, SqlTypeName.Char, SqlTypeName.Varchar };
    public static final SqlTypeName [] timeIntervalNullableTypes =
    {
        SqlTypeName.Null, SqlTypeName.IntervalDayTime,
        SqlTypeName.IntervalYearToMonth
    };

    /**
     * Parameter type-inference transform strategy where a derived type is
     * transformed into the same type but nullable if any of a calls operands
     * is nullable
     */
    public static final SqlOperator.TypeInferenceTransform transformNullable =
        new SqlOperator.TypeInferenceTransform() {
            public RelDataType getType(
                RelDataTypeFactory typeFactory,
                RelDataType [] argTypes,
                RelDataType typeToTransform)
            {
                return makeNullableIfOperandsAre(typeFactory, argTypes,
                    typeToTransform);
            }
        };

    /**
     * Type-inference strategy whereby the result type of a call is VARYING
     * the type given.
     * The precision returned is the same as precision of the
     * first argument.
     * Return type will have same nullablilty as input type nullablility.
     * First Arg must be of string type.
     */
    public static final SqlOperator.TypeInferenceTransform transformVarying =
        new SqlOperator.TypeInferenceTransform() {
            public RelDataType getType(
                RelDataTypeFactory fac,
                RelDataType [] argTypes,
                RelDataType type)
            {
                switch (type.getSqlTypeName().ordinal) {
                case SqlTypeName.Varchar_ordinal:
                case SqlTypeName.Varbinary_ordinal:
                case SqlTypeName.Varbit_ordinal:
                    return type;
                }

                SqlTypeName retTypeName = toVar(type);

                RelDataType ret =
                    fac.createSqlType(
                        retTypeName,
                        type.getPrecision());
                if (type.isCharType()) {
                    ret = fac.createTypeWithCharsetAndCollation(
                            ret,
                            type.getCharset(),
                            type.getCollation());
                }
                return fac.createTypeWithNullability(
                    ret,
                    type.isNullable());
            }

            private SqlTypeName toVar(RelDataType type)
            {
                final SqlTypeName sqlTypeName = type.getSqlTypeName();
                switch (sqlTypeName.ordinal) {
                case SqlTypeName.Char_ordinal:
                    return SqlTypeName.Varchar;
                case SqlTypeName.Binary_ordinal:
                    return SqlTypeName.Varbinary;
                case SqlTypeName.Bit_ordinal:
                    return SqlTypeName.Varbit;
                default:
                    throw sqlTypeName.unexpected();
                }
            }
        };

    /**
     * Type-inference strategy whereby the result type of a call is the type of
     * the first operand.
     */
    public static final SqlOperator.TypeInference useFirstArgType =
        new SqlOperator.TypeInference() {
            public RelDataType getType(
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlCall call)
            {
                return validator.deriveType(scope, call.operands[0]);
            }

            public RelDataType getType(
                RelDataTypeFactory typeFactory,
                RelDataType [] argTypes)
            {
                return argTypes[0];
            }
        };

    /**
     * Type-inference strategy whereby the result type of a call is the type of
     * the first operand. If any of the other operands are nullable the returned
     * type will also be nullable.
     */
    public static final SqlOperator.TypeInference useNullableFirstArgType =
        new SqlOperator.CascadeTypeInference(useFirstArgType, transformNullable);

    /**
     * Type-inference strategy whereby the result type of a call is VARYING
     * the type of the first argument.
     * The precision returned is the same as precision of the
     * first argument.
     * If any of the other operands are nullable the returned
     * type will also be nullable.
     * First Arg must be of string type.
     */
    public static final SqlOperator.TypeInference useNullableVaryingFirstArgType =
        new SqlOperator.CascadeTypeInference(useFirstArgType,
            transformNullable, transformVarying);

    /**
     * Type-inference strategy whereby the result type of a call is the type of
     * the second operand.
     */
    public static final SqlOperator.TypeInference useSecondArgType =
        new SqlOperator.TypeInference() {
            public RelDataType getType(
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlCall call)
            {
                return validator.deriveType(scope, call.operands[1]);
            }

            public RelDataType getType(
                RelDataTypeFactory typeFactory,
                RelDataType [] argTypes)
            {
                return argTypes[1];
            }
        };

    /**
     * Type-inference strategy whereby the result type of a call is Boolean.
     */
    public static final SqlOperator.TypeInference useBoolean =
        new SqlOperator.TypeInference() {
            public RelDataType getType(
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlCall call)
            {
                return validator.typeFactory.createSqlType(SqlTypeName.Boolean);
            }

            public RelDataType getType(
                RelDataTypeFactory typeFactory,
                RelDataType [] argTypes)
            {
                return typeFactory.createSqlType(SqlTypeName.Boolean);
            }
        };

    /**
     * Type-inference strategy whereby the result type of a call is Boolean,
     * with nulls allowed if any of the operands allow nulls.
     */
    public static final SqlOperator.TypeInference useNullableBoolean =
        new SqlOperator.CascadeTypeInference(useBoolean, transformNullable);

    /**
     * Type-inference strategy whereby the result type of a call is Time.
     */
    public static final SqlOperator.TypeInference useTime =
        new SqlOperator.TypeInference() {
            public RelDataType getType(
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlCall call)
            {
                return validator.typeFactory.createSqlType(SqlTypeName.Time, 0);
            }

            public RelDataType getType(
                RelDataTypeFactory typeFactory,
                RelDataType [] argTypes)
            {
                int precision = 0;
                return typeFactory.createSqlType(SqlTypeName.Time, precision);
            }
        };

    /**
     * Type-inference strategy whereby the result type of a call is Double.
     */
    public static final SqlOperator.TypeInference useDouble =
        new SqlOperator.TypeInference() {
            public RelDataType getType(
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlCall call)
            {
                return validator.typeFactory.createSqlType(SqlTypeName.Double);
            }

            public RelDataType getType(
                RelDataTypeFactory typeFactory,
                RelDataType [] argTypes)
            {
                return typeFactory.createSqlType(SqlTypeName.Double);
            }
        };

    /**
     * Type-inference strategy whereby the result type of a call is Double
     * with nulls allowed if any of the operands allow nulls.
     */
    public static final SqlOperator.TypeInference useNullableDouble =
        new SqlOperator.CascadeTypeInference(useDouble, transformNullable);

    /**
     * Type-inference strategy whereby the result type of a call is an Integer.
     */
    public static final SqlOperator.TypeInference useInteger =
        new SqlOperator.TypeInference() {
            public RelDataType getType(
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlCall call)
            {
                return validator.typeFactory.createSqlType(SqlTypeName.Integer);
            }

            public RelDataType getType(
                RelDataTypeFactory typeFactory,
                RelDataType [] argTypes)
            {
                return typeFactory.createSqlType(SqlTypeName.Integer);
            }
        };

    /**
     * Type-inference strategy whereby the result type of a call is an Integer
     * with nulls allowed if any of the operands allow nulls.
     */
    public static final SqlOperator.TypeInference useNullableInteger =
        new SqlOperator.CascadeTypeInference(useInteger, transformNullable);

    /**
     * Type-inference strategy whereby the result type of a call is using its
     * operands biggest type, using the rules described in ISO/IEC 9075-2:1999
     * section 9.3 "Data types of results of aggregations".
     * These rules are used in union, except, intercect, case and other places.
     *
     * <p>For example, the expression <code>(500000000000 + 3.0e-3)</code> has
     * the operands INTEGER and DOUBLE. Its biggest type is double.
     */
    public static final SqlOperator.TypeInference useBiggest =
        new SqlOperator.TypeInference() {
            public RelDataType getType(
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlCall call)
            {
                return collectTypesFromCall(validator, scope, call);
            }

            public RelDataType getType(
                RelDataTypeFactory typeFactory,
                RelDataType [] argTypes)
            {
                // REVIEW jvs 1-Mar-2004: I changed this to
                // leastRestrictive since that's its purpose (and at least
                // for Farrago, works better than the old getBiggest code).
                // But this type inference rule isn't general enough for
                // numeric types, which use different rules depending on
                // the operator (e.g. sum precision is based on the max of
                // the arg precisions, while product precision is based on
                // the sum of the arg precisions).
                return typeFactory.leastRestrictive(argTypes);
            }
        };

    /**
     * Type-inference strategy similar to {@link #useBiggest}, except that the
     * result is nullable if any of the arguments is nullable.
     */
    public static final SqlOperator.TypeInference useNullableBiggest =
        new SqlOperator.CascadeTypeInference(useBiggest, transformNullable);

    /**
     * Type-inference strategy where by the
     * Result type of a call is
     * <ul>
     * <li>the same type as the input types but with the
     * combined precision of the two first types</li>
     * <li>If types are of char type the type with the highest coercibility
     * will be used</li>
     *
     * @pre input types must be of the same string type
     * @pre types must be comparable without casting
     */
    public static final SqlOperator.TypeInference useDyadicStringSumPrecision =
        new SqlOperator.TypeInference() {
            public RelDataType getType(
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlCall call)
            {
                return collectTypesFromCall(validator, scope, call);
            }

            /**
             * @pre type0 <COMPARABLE> to type1 (without casting)
             */
            public RelDataType getType(
                RelDataTypeFactory typeFactory,
                RelDataType [] argTypes)
            {
                if (!(argTypes[0].isCharType() && argTypes[1].isCharType())) {
                    Util.pre(
                        argTypes[0].isSameType(argTypes[1]),
                        "argTypes[0].isSameType(argTypes[1])");
                }
                SqlCollation pickedCollation = null;
                if (argTypes[0].isCharType()) {
                    if (!ValidationUtil.isCharTypeComparable(argTypes, 0, 1)) {
                        throw EigenbaseResource.instance()
                            .newTypeNotComparable(
                                argTypes[0].toString(),
                                argTypes[1].toString());
                    }

                    pickedCollation =
                        SqlCollation.getCoercibilityDyadicOperator(
                            argTypes[0].getCollation(),
                            argTypes[1].getCollation());
                    assert (null != pickedCollation);
                }

                RelDataType ret;
                ret = typeFactory.createSqlType(
                        argTypes[0].getSqlTypeName(),
                        argTypes[0].getPrecision()
                        + argTypes[1].getPrecision());
                if (null != pickedCollation) {
                    RelDataType pickedType;
                    if (argTypes[0].getCollation().equals(pickedCollation)) {
                        pickedType = argTypes[0];
                    } else if (argTypes[1].getCollation().equals(pickedCollation)) {
                        pickedType = argTypes[1];
                    } else {
                        throw Util.newInternal("should never come here");
                    }
                    ret = typeFactory.createTypeWithCharsetAndCollation(
                            ret,
                            pickedType.getCharset(),
                            pickedType.getCollation());
                }
                return ret;
            }
        };

    /**
     * Same as {@link #useDyadicStringSumPrecision} and using
     * {@link #transformNullable}
     */
    public static final SqlOperator.TypeInference useNullableDyadicStringSumPrecision =
        new SqlOperator.CascadeTypeInference(useDyadicStringSumPrecision,
            new SqlOperator.TypeInferenceTransform [] { transformNullable });

    /**
     * Same as {@link #useDyadicStringSumPrecision} and using
     * {@link #transformNullable}, {@link #transformVarying}
     */
    public static final SqlOperator.TypeInference useNullableVaryingDyadicStringSumPrecision =
        new SqlOperator.CascadeTypeInference(useDyadicStringSumPrecision,
            new SqlOperator.TypeInferenceTransform [] {
                transformNullable, transformVarying
            });

    /**
     * Type-inference strategy where the expression is assumed to be registered
     * as a {@link SqlValidator.Scope}, and therefore the result type of the
     * call is the type of that scope.
     */
    public static final SqlOperator.TypeInference useScope =
        new SqlOperator.TypeInference() {
            public RelDataType getType(
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlCall call)
            {
                return validator.getScope(call).getRowType();
            }

            public RelDataType getType(
                RelDataTypeFactory typeFactory,
                RelDataType [] argTypes)
            {
                throw new UnsupportedOperationException();
            }
        };

    /**
     * Parameter type-inference strategy where an unknown operand
     * type is derived from the first operand with a known type.
     */
    public static final SqlOperator.ParamTypeInference useFirstKnownParam =
        new SqlOperator.ParamTypeInference() {
            public void inferOperandTypes(
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlCall call,
                RelDataType returnType,
                RelDataType [] operandTypes)
            {
                SqlNode [] operands = call.getOperands();
                RelDataType knownType = validator.unknownType;
                for (int i = 0; i < operands.length; ++i) {
                    knownType = validator.deriveType(scope, operands[i]);
                    if (!knownType.equals(validator.unknownType)) {
                        break;
                    }
                }
                assert (!knownType.equals(validator.unknownType));
                for (int i = 0; i < operandTypes.length; ++i) {
                    operandTypes[i] = knownType;
                }
            }
        };

    /**
     * Parameter type-inference strategy where an unknown operand
     * type is derived from the call's return type.  If the return type is
     * a record, it must have the same number of fields as the number
     * of operands.
     */
    public static final SqlOperator.ParamTypeInference useReturnForParam =
        new SqlOperator.ParamTypeInference() {
            public void inferOperandTypes(
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlCall call,
                RelDataType returnType,
                RelDataType [] operandTypes)
            {
                for (int i = 0; i < operandTypes.length; ++i) {
                    if (returnType.isProject()) {
                        operandTypes[i] = returnType.getFields()[i].getType();
                    } else {
                        operandTypes[i] = returnType;
                    }
                }
            }
        };

    /**
     * Parameter type-inference strategy where an unknown operand
     * type is assumed to be boolean.
     */
    public static final SqlOperator.ParamTypeInference useBooleanParam =
        new SqlOperator.ParamTypeInference() {
            public void inferOperandTypes(
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlCall call,
                RelDataType returnType,
                RelDataType [] operandTypes)
            {
                for (int i = 0; i < operandTypes.length; ++i) {
                    operandTypes[i] =
                        validator.typeFactory.createSqlType(SqlTypeName.Boolean);
                }
            }
        };

    /**
     * Parameter type-checking strategy
     * type must be nullable boolean, nullable boolean.
     */
    public static final SqlOperator.AllowedArgInference typeNullableBoolBool =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                booleanNullableTypes, booleanNullableTypes
            });

    /**
     * Parameter type-checking strategy
     * type must be numeric.
     */
    public static final SqlOperator.AllowedArgInference typeNumeric =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] { numericTypes });

    /**
     * Parameter type-checking strategy
     * type must be a numeric literal.
     */
    public static final SqlOperator.AllowedArgInference typeNumericLiteral =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] { numericTypes }) {
            public boolean check(
                SqlCall call,
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlNode node,
                int operandOrdinal)
            {
                boolean res =
                    super.check(call, validator, scope, node, operandOrdinal);
                if (!res) {
                    return res;
                }
                if (operandOrdinal == 0) {
                    if (!SqlUtil.isLiteral(node)) {
                        throw EigenbaseResource.instance()
                            .newArgumentMustBeLiteral(call.operator.name);
                    }
                }
                return res;
            }
        };

    /**
     * Parameter type-checking strategy
     * type must be a positive integer literal.
     */
    public static final SqlOperator.AllowedArgInference typePositiveIntegerLiteral =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                { SqlTypeName.Integer }
            }) {
            public boolean check(
                SqlCall call,
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlNode node,
                int operandOrdinal)
            {
                boolean res =
                    super.check(call, validator, scope, node, operandOrdinal);
                if (!res) {
                    return res;
                }

                // REVIEW (jhyde, 2004/7/23) why is the isNullLiteral check
                //   outside the 'if (operandOrdinal == 0)' check, and the
                //   isLiteral check inside? In fact, why the 'operandOrdinal == 0'
                //   check at all?
                if (SqlUtil.isNullLiteral(node, true)) {
                    throw EigenbaseResource.instance()
                        .newArgumentMustNotBeNull(call.operator.name);
                }
                if (operandOrdinal == 0) {
                    if (!SqlUtil.isLiteral(node)) {
                        throw EigenbaseResource.instance()
                            .newArgumentMustBeLiteral(call.operator.name);
                    }
                    final SqlLiteral arg = ((SqlLiteral) node);
                    final int value = arg.intValue();
                    if (value < 0) {
                        throw EigenbaseResource.instance()
                            .newArgumentMustBePositiveInteger(call.operator.name);
                    }
                }
                return res;
            }
        };

    /**
     * Parameter type-checking strategy
     * type must be numeric, numeric.
     */
    public static final SqlOperator.AllowedArgInference typeNumericNumeric =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                numericTypes, numericTypes
            });

    /**
     * Parameter type-checking strategy
     * type must be integer, integer. (exact types)
     */
    public static final SqlOperator.AllowedArgInference typeIntegerInteger =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                intTypes, intTypes
            });

    /**
     * Parameter type-checking strategy
     * type must be nullable integer, nullable integer. (exact types)
     */
    public static final SqlOperator.AllowedArgInference typeNullableIntegerInteger =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                intNullableTypes, intNullableTypes
            });

    /**
     * Parameter type-checking strategy
     * type must be nullable aType, nullable sameType
     */
    public static final SqlOperator.AllowedArgInference typeNullableSameSame =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                { SqlTypeName.Any },
                { SqlTypeName.Any }
            }) {
            public void check(
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlCall call)
            {
                if (!checkNoThrowing(call, validator, scope)) {
                    throw EigenbaseResource.instance()
                        .newNeedSameTypeParameter(
                            call.getParserPosition().toString());
                }
            }

            public boolean checkNoThrowing(
                SqlCall call,
                SqlValidator validator,
                SqlValidator.Scope scope)
            {
                assert (2 == call.operands.length);
                RelDataType type1 =
                    validator.deriveType(scope, call.operands[0]);
                RelDataType type2 =
                    validator.deriveType(scope, call.operands[1]);
                RelDataType nullType =
                    validator.typeFactory.createSqlType(SqlTypeName.Null);
                if (type1.equals(nullType) || type2.equals(nullType)) {
                    return true; //null is ok;
                }
                return type1.isSameType(type2) || type2.isSameType(type1);
            }
        };

    /**
     * Parameter type-checking strategy
     * type must be nullable aType, nullable sameType, nullable sameType
     */
    public static final SqlOperator.AllowedArgInference typeNullableSameSameSame =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                { SqlTypeName.Any },
                { SqlTypeName.Any },
                { SqlTypeName.Any }
            }) {
            public void check(
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlCall call)
            {
                if (!checkNoThrowing(call, validator, scope)) {
                    throw EigenbaseResource.instance()
                        .newNeedSameTypeParameter(
                            call.getParserPosition().toString());
                }
            }

            public boolean checkNoThrowing(
                SqlCall call,
                SqlValidator validator,
                SqlValidator.Scope scope)
            {
                assert (3 == call.operands.length);
                RelDataType type1 =
                    validator.deriveType(scope, call.operands[0]);
                RelDataType type2 =
                    validator.deriveType(scope, call.operands[1]);
                RelDataType type3 =
                    validator.deriveType(scope, call.operands[2]);
                RelDataType nullType =
                    validator.typeFactory.createSqlType(SqlTypeName.Null);

                //null is ok;
                return
                    (type1.equals(nullType) || type2.equals(nullType) ||
                        type1.isSameType(type2) || type2.isSameType(type1)) &&
                    (type1.equals(nullType) || type3.equals(nullType) ||
                        type1.isSameType(type3) || type3.isSameType(type1)) &&
                    (type2.equals(nullType) || type3.equals(nullType) ||
                        type2.isSameType(type3) || type3.isSameType(type2));
            }
        };

    /**
     * Parameter type-checking strategy
     * type must be nullable numeric, nullable numeric.
     */
    public static final SqlOperator.AllowedArgInference typeNullableNumericNumeric =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                numericNullableTypes, numericNullableTypes
            });

    /**
     * Parameter type-checking strategy
     * type must be nullable numeric, nullable numeric, nullabl numeric.
     */
    public static final SqlOperator.AllowedArgInference typeNullableNumericNumericNumeric =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                numericNullableTypes, numericNullableTypes,
                numericNullableTypes
            });

    /**
     * Parameter type-checking strategy
     * type must be nullable binary, nullable binary.
     */
    public static final SqlOperator.AllowedArgInference typeNullableBinariesBinaries =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                binaryNullableTypes, binaryNullableTypes
            });

    /**
     * Parameter type-checking strategy
     * type must be nullable binary, nullable binary, nullable binary.
     */
    public static final SqlOperator.AllowedArgInference typeNullableBinariesBinariesBinaries =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                binaryNullableTypes, binaryNullableTypes, binaryNullableTypes
            });

    /**
     * Parameter type-checking strategy
     * type must be null | charstring | bitstring | hexstring
     */
    public static final SqlOperator.AllowedArgInference typeNullableString =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                stringNullableTypes
            });

    /**
     * Parameter type-checking strategy
     * types must be null | charstring | bitstring | hexstring
     */
    public static final SqlOperator.AllowedArgInference typeNullableStringString =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                stringNullableTypes, stringNullableTypes
            });

    /**
      * Parameter type-checking strategy
      * type must be a varchar literal.
      */
    public static final SqlOperator.AllowedArgInference typeVarcharLiteral =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {charTypes}) {
            public boolean check(
                SqlCall call,
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlNode node,
                int operandOrdinal)
            {
                boolean res =
                    super.check(call, validator, scope, node, operandOrdinal);
                if (!res) {
                    return res;
                }
                if (SqlUtil.isNullLiteral(node, true)) {
                    throw EigenbaseResource.instance()
                        .newArgumentMustNotBeNull(call.operator.name);
                }
                if (operandOrdinal == 0) {
                    if (node instanceof SqlLiteral) {
                    } else {
                        throw EigenbaseResource.instance()
                            .newArgumentMustBeLiteral(call.operator.name);
                    }
                }
                return res;
            }
        };

    /**
      * Parameter type-checking strategy
      * type must be a nullable varchar literal.
      */
    public static final SqlOperator.AllowedArgInference typeNullableVarcharLiteral =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                charNullableTypes
            }) {
            public boolean check(
                SqlCall call,
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlNode node,
                int operandOrdinal)
            {
                boolean res =
                    super.check(call, validator, scope, node, operandOrdinal);
                if (!res) {
                    return res;
                }
                if (operandOrdinal == 0) {
                    if (SqlUtil.isNullLiteral(node, true)) {
                        return res;
                    } else if (SqlUtil.isLiteral(node)) {
                        return res;
                    } else {
                        throw EigenbaseResource.instance()
                            .newArgumentMustBeLiteral(call.operator.name);
                    }
                }
                return res;
            }
        };

    /**
      * Parameter type-checking strategy
      * type must be nullable varchar, NOT nullable varchar literal.
      * the expression <code>CAST(NULL AS TYPE)</code> is considered a NULL literal.
      */
    public static final SqlOperator.AllowedArgInference typeNullableVarcharNotNullableVarcharLiteral =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                charNullableTypes, charTypes
            }) {
            public void check(
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlCall call)
            {
                // check that the 2nd argument is a varchar literal
                super.check(validator, scope, call);
                if (SqlUtil.isNullLiteral(call.getOperands()[1], true)) {
                    throw EigenbaseResource.instance()
                        .newArgumentMustNotBeNull(call.operator.name);
                }
                if (!(call.getOperands()[1] instanceof SqlLiteral)) {
                    throw EigenbaseResource.instance()
                        .newArgumentMustBeLiteral(call.operator.name);
                }
            }
        };

    /**
    * Parameter type-checking strategy
    * types must be null | charstring | bitstring | hexstring
    * AND types must be identical to eachother
    */
    public static final SqlOperator.AllowedArgInference typeNullableStringStringOfSameType =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                stringNullableTypes, stringNullableTypes
            }) {
            public String getAllowedSignatures(SqlOperator op)
            {
                StringBuffer ret = new StringBuffer();
                for (int i = 0; i < types[0].length; i++) {
                    if (types[0][i].getOrdinal() == SqlTypeName.Null_ordinal) {
                        continue;
                    }

                    ArrayList list = new ArrayList(2);
                    list.add(types[0][i]); //adding same twice
                    list.add(types[0][i]); //adding same twice
                    ret.append(op.getSignature(list));

                    if ((i + 1) < types[0].length) {
                        ret.append(op.NL);
                    }
                }
                return ret.toString();
            }

            protected void getAllowedSignatures(
                int depth,
                ArrayList list,
                StringBuffer buf,
                SqlOperator op)
            {
                throw Util.needToImplement(
                    "should not be called unless implemented");
            }

            public void check(
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlCall call)
            {
                RelDataType t0 = validator.deriveType(scope, call.operands[0]);
                RelDataType t1 = validator.deriveType(scope, call.operands[1]);
                assert (null != t0) : "should not be null";
                assert (null != t1) : "should not be null";
                RelDataType nullType =
                    validator.typeFactory.createSqlType(SqlTypeName.Null);
                if (!nullType.isAssignableFrom(t0, false)
                        && !nullType.isAssignableFrom(t1, false)) {
                    if (!t0.isSameTypeFamily(t1)) {
                        //parser postition retrieved in
                        //newValidationSignatureError()
                        throw call.newValidationSignatureError(validator, scope);
                    }
                }
                super.check(validator, scope, call);
            }
        };

    /**
     * Parameter type-checking strategy
     * types must be null | charstring | bitstring | hexstring
     * AND types must be identical to eachother
     */
    public static final SqlOperator.AllowedArgInference typeNullableStringStringStringOfSameType =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                stringNullableTypes, stringNullableTypes, stringNullableTypes
            }) {
            public String getAllowedSignatures(SqlOperator op)
            {
                StringBuffer ret = new StringBuffer();
                for (int i = 0; i < types[0].length; i++) {
                    if (types[0][i].getOrdinal() == SqlTypeName.Null_ordinal) {
                        continue;
                    }

                    ArrayList list = new ArrayList(3);
                    list.add(types[0][i]); //adding same trice
                    list.add(types[0][i]); //adding same trice
                    list.add(types[0][i]); //adding same trice
                    ret.append(op.getSignature(list));

                    if ((i + 1) < types[0].length) {
                        ret.append(op.NL);
                    }
                }
                return ret.toString();
            }

            protected void getAllowedSignatures(
                int depth,
                ArrayList list,
                StringBuffer buf,
                SqlOperator op)
            {
                throw Util.needToImplement(
                    "should not be called unless implemented");
            }

            public void check(
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlCall call)
            {
                RelDataType t0 = validator.deriveType(scope, call.operands[0]);
                RelDataType t1 = validator.deriveType(scope, call.operands[1]);
                RelDataType t2 = validator.deriveType(scope, call.operands[2]);
                assert (null != t0) : "should not be null";
                assert (null != t1) : "should not be null";
                assert (null != t2) : "should not be null";
                RelDataType nullType =
                    validator.typeFactory.createSqlType(SqlTypeName.Null);
                if (!nullType.isAssignableFrom(t0, false)
                        && !nullType.isAssignableFrom(t1, false)
                        && !nullType.isAssignableFrom(t2, false)) {
                    if (!t0.isSameTypeFamily(t1) || !t1.isSameTypeFamily(t2)) {
                        throw call.newValidationSignatureError(validator, scope);
                    }
                }
                super.check(validator, scope, call);
            }
        };

    /**
     * Parameter type-checking strategy
     * types must be null | charstring | bitstring | hexstring
     */
    public static final SqlOperator.AllowedArgInference typeNullableStringStringString =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                stringNullableTypes, stringNullableTypes, stringNullableTypes
            });

    /**
     * Parameter type-checking strategy
     * types must be
     * null | charstring | bitstring | hexstring
     * null | int
     */
    public static final SqlOperator.AllowedArgInference typeNullableStringInt =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                stringNullableTypes, intNullableTypes
            });

    /**
     * Parameter type-checking strategy
     * types must be
     * null | charstring | bitstring | hexstring
     * null | int
     * null | int
     */
    public static final SqlOperator.AllowedArgInference typeNullableStringIntInt =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                stringTypes, intNullableTypes, intNullableTypes
            });

    /**
     * Parameter type-checking strategy
     * 2 first types must be null | charstring | bitstring | hexstring
     * 3 type must be integer
     */
    public static final SqlOperator.AllowedArgInference typeNullableStringStringNotNullableInt =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                stringTypes, stringTypes, intTypes
            });

    /**
     * Parameter type-checking strategy
     * 2 first types must be null | charstring | bitstring | hexstring
     * 3&4 type must be integer
     */
    public static final SqlOperator.AllowedArgInference typeNullableStringStringNotNullableIntInt =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                stringTypes, stringTypes, intTypes, intTypes
            });

    /**
         * Parameter type-checking strategy
         * type must be nullable numeric
         */
    public static final SqlOperator.AllowedArgInference typeNullableNumeric =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                numericNullableTypes
            });

    /**
     * Parameter type-checking strategy
     * types must be varchar, int, int
     */
    public static final SqlOperator.AllowedArgInference typeVarcharIntInt =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                charTypes, intTypes, intTypes
            });

    /**
     * Parameter type-checking strategy
     * types must be [nullable] varchar, [nullable] int, [nullable] int
     */
    public static final SqlOperator.AllowedArgInference typeNullableVarcharIntInt =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                charNullableTypes, intNullableTypes, intNullableTypes
            });

    /**
         * Parameter type-checking strategy
         * types must be not nullable varchar, not nullable int
         */
    public static final SqlOperator.AllowedArgInference typeVarcharInt =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                charTypes, intTypes
            });

    /**
     * Parameter type-checking strategy
     * types must be [nullable] varchar, [nullable] varchar,
     */
    public static final SqlOperator.AllowedArgInference typeNullableVarcharVarchar =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                charNullableTypes, charNullableTypes
            });

    /**
     * Parameter type-checking strategy
     * types must be [nullable] varchar, [nullable] varchar, [nullable] varchar
     */
    public static final SqlOperator.AllowedArgInference typeNullableVarcharVarcharVarchar =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                charNullableTypes, charNullableTypes, charNullableTypes
            });

    /**
     * Parameter type-checking strategy
     * types must be nullable boolean
     */
    public static final SqlOperator.AllowedArgInference typeNullableBool =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                booleanNullableTypes
            });

    /**
     * Parameter type-checking strategy
     * types can be any,any
     */
    public static final SqlOperator.AllowedArgInference typeAnyAny =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                { SqlTypeName.Any },
                { SqlTypeName.Any }
            });

    /**
     * Parameter type-checking strategy
     * types can be any
     */
    public static final SqlOperator.AllowedArgInference typeAny =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                { SqlTypeName.Any }
            });

    /**
     * Parameter type-checking strategy
     * types must be varchar,varchar
     */
    public static final SqlOperator.AllowedArgInference typeVarcharVarchar =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                charTypes, charTypes
            });

    /**
     * Parameter type-checking strategy
     * types must be nullable varchar
     */
    public static final SqlOperator.AllowedArgInference typeNullableVarchar =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                charNullableTypes
            });

    /**
     * Parameter type-checking strategy
     * type must be
     * positive integer literal
     * OR varchar literal
     */
    public static final SqlOperator.AllowedArgInference typePositiveIntegerLiteral_or_VarcharLiteral =
        new SqlOperator.CompositeAllowedArgInference(new SqlOperator.AllowedArgInference [] {
                typePositiveIntegerLiteral, typeVarcharLiteral
            });

    /**
     * Parameter type-checking strategy
     * type must be
     * nullable aType, nullable aType
     * and must be comparable to eachother
     */
    public static final SqlOperator.AllowedArgInference typeNullableComparable =
        new SqlOperator.CompositeAllowedArgInference(new SqlOperator.AllowedArgInference [] {
                typeNullableSameSame, typeNullableNumericNumeric,
                typeNullableBinariesBinaries, typeNullableVarcharVarchar
            });

    /**
     * Parameter type-checking strategy
     * type must be
     * nullable string, nullable string, nulalble string
     * OR nullable string, nullable numeric, nullable numeric.
     */
    public static final SqlOperator.AllowedArgInference typeNullabeStringStringString_or_NullableStringIntInt =
        new SqlOperator.CompositeAllowedArgInference(new SqlOperator.AllowedArgInference [] {
                typeNullableStringStringString, typeNullableStringIntInt
            });

    /**
     * Parameter type-checking strategy
     * type must be
     * nullable String
     * OR nullable numeric
     */
    public static final SqlOperator.AllowedArgInference typeNullableString_or_NullableNumeric =
        new SqlOperator.CompositeAllowedArgInference(new SqlOperator.AllowedArgInference [] {
                typeNullableString, typeNullableNumeric
            });

    /**
    * Parameter type-checking strategy
    * type must a nullable time interval
    */
    public static final SqlOperator.AllowedArgInference typeNullableInterval =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                timeIntervalNullableTypes
            });

    /**
     * Parameter type-checking strategy
     * type must a nullable time interval, nullable time interval
     */
    public static final SqlOperator.AllowedArgInference typeNullableIntervalInterval =
        new SqlOperator.AllowedArgInference(new SqlTypeName [][] {
                timeIntervalNullableTypes, timeIntervalNullableTypes
            });

    /**
     * The standard operator table.
     */

    // Must be declared last, because many of the strategy objects above are
    // required by objects in the table.
    private static final SqlStdOperatorTable instance = createStd();

    //~ Instance fields -------------------------------------------------------

    private final MultiMap operators = new MultiMap();
    private final HashMap mapNameToOp = new HashMap();
    public final Set stringFuncNames = new LinkedHashSet();
    public final Set numericFuncNames = new LinkedHashSet();
    public final Set timeDateFuncNames = new LinkedHashSet();
    public final Set systemFuncNames = new LinkedHashSet();

    /**
     * Multi-map from function name to a list of functions with that name.
     */
    private final MultiMap mapNameToFunc = new MultiMap();

    //~ Constructors ----------------------------------------------------------

    protected SqlOperatorTable()
    {
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Creates and initializes the standard operator table.
     * Uses two-phase construction, because we can't intialize the table until
     * the constructor of the sub-class has completed.
     */
    private static SqlStdOperatorTable createStd()
    {
        SqlStdOperatorTable table = new SqlStdOperatorTable();
        table.init();
        return table;
    }

    /**
     * Call this method after constructing an operator table. It can't be
     * part of the constructor, because the sub-class' constructor needs to
     * complete first.
     */
    public final void init()
    {
        // Use reflection to register the expressions stored in public fields.
        Field [] fields = getClass().getFields();
        for (int i = 0; i < fields.length; i++) {
            try {
                Field field = fields[i];
                if (SqlFunction.class.isAssignableFrom(field.getType())) {
                    SqlFunction op = (SqlFunction) field.get(this);
                    if (op != null) {
                        register(op);
                    }
                } else if (SqlOperator.class.isAssignableFrom(field.getType())) {
                    SqlOperator op = (SqlOperator) field.get(this);
                    register(op);

                }
            } catch (IllegalArgumentException e) {
                throw Util.newInternal(e,
                    "Error while initializing operator table");
            } catch (IllegalAccessException e) {
                throw Util.newInternal(e,
                    "Error while initializing operator table");
            }
        }
    }

    /**
     * Returns the {@link org.eigenbase.util.Glossary#SingletonPattern
     * singleton} instance, creating it if necessary.
     */
    public static SqlOperatorTable instance()
    {
        return instance;
    }

    /**
     * Returns the {@link org.eigenbase.util.Glossary#SingletonPattern
     * singleton} instance of the table of standard operators.
     */
    public static SqlStdOperatorTable std()
    {
        return (SqlStdOperatorTable) instance();
    }

    /**
     * Retrieves an operator by its name and syntactic type.
     */
    public SqlOperator lookup(
        String opName,
        SqlSyntax syntax)
    {
        final List list = operators.getMulti(opName.toUpperCase());
        for (int i = 0, n = list.size(); i < n; i++) {
            SqlOperator op = (SqlOperator) list.get(i);
            if (op.getSyntax() == syntax) {
                return op;
            }
        }
        switch (syntax.ordinal) {
        case SqlSyntax.Binary_ordinal:
            return (SqlBinaryOperator) mapNameToOp.get(opName + ":BINARY");
        case SqlSyntax.Prefix_ordinal:
            return (SqlPrefixOperator) mapNameToOp.get(opName + ":PREFIX");
        case SqlSyntax.Postfix_ordinal:
            return (SqlPostfixOperator) mapNameToOp.get(opName + ":POSTFIX");
        case SqlSyntax.Function_ordinal:
            throw Util.newInternal("Use lookupFunction to lookup function");
        default:
            throw syntax.unexpected();
        }
    }

    public void register(SqlOperator op)
    {
        operators.putMulti(op.name, op);
        if (op instanceof SqlBinaryOperator) {
            mapNameToOp.put(op.name + ":BINARY", op);
        } else if (op instanceof SqlPrefixOperator) {
            mapNameToOp.put(op.name + ":PREFIX", op);
        } else if (op instanceof SqlPostfixOperator) {
            mapNameToOp.put(op.name + ":POSTFIX", op);
        }
    }

    public SqlCall createCall(
        String funName,
        SqlNode [] operands,
        ParserPosition pos)
    {
        List funs = lookupFunctionsByName(funName);
        final SqlFunction fun;
        if (funs.isEmpty()) {
            //REVIEW/TODO wael: why is this call neccessary? I tried removing it and
            //tests failed.
            fun = new SqlFunction(funName, null, null, null) {
                        public void test(SqlTester tester)
                        {
                            /* empty implementation */
                        }
                    };
        } else {
            fun = (SqlFunction) funs.get(0);
        }
        return fun.createCall(operands, pos);
    }

    /**
     * Retrieves a list of overloading function by a given name.
     * @return If no function exists, null is returned,
     *         else retrieves a list of overloading function by a given name.
     */
    public List lookupFunctionsByName(String funcName)
    {
        return mapNameToFunc.getMulti(funcName);
    }

    /**
     * Register function to the table.
     * @param function
     */
    public void register(SqlFunction function)
    {
        mapNameToFunc.putMulti(function.name, function);
        SqlFunction.SqlFuncTypeName funcType = function.getFunctionType();
        assert (funcType != null) : "Function type for " + function.name
        + " not set";
        switch (funcType.getOrdinal()) {
        case SqlFunction.SqlFuncTypeName.String_ordinal:
            stringFuncNames.add(function.name);
            break;
        case SqlFunction.SqlFuncTypeName.Numeric_ordinal:
            numericFuncNames.add(function.name);
            break;
        case SqlFunction.SqlFuncTypeName.TimeDate_ordinal:
            timeDateFuncNames.add(function.name);
            break;
        case SqlFunction.SqlFuncTypeName.System_ordinal:
            systemFuncNames.add(function.name);
            break;
        default: Util.needToImplement(funcType);
        }
    }

    private SqlFunction [] lookupFunctionsByNameAndArgCount(
        String name,
        int numberOfParams)
    {
        List funcList = mapNameToFunc.getMulti(name);
        if (funcList.isEmpty()) {
            return null;
        }

        List candidateList = new LinkedList();
        for (int i = 0; i < funcList.size(); i++) {
            SqlFunction function = (SqlFunction) funcList.get(i);
            SqlOperator.OperandsCountDescriptor od =
                function.getOperandsCountDescriptor();
            if (od.isVariadic()
                    || od.getPossibleNumOfOperands().contains(
                        new Integer(numberOfParams))) {
                candidateList.add(function);
            }
        }
        return (SqlFunction []) candidateList.toArray(
            new SqlFunction[candidateList.size()]);
    }

    /**
     * Chose the best fit function
     * @param funcName
     * @param argTypes
     * @return
     */
    public SqlFunction lookupFunction(
        String funcName,
        RelDataType [] argTypes)
    {
        // The number of defined parameters need to match the invocation
        SqlFunction [] functions =
            lookupFunctionsByNameAndArgCount(funcName, argTypes.length);
        if ((null == functions) || (0 == functions.length)) {
            return null;
        } else if (functions.length == 1) {
            return functions[0];
        }

        ArrayList candidates = new ArrayList();
        for (int i = 0; i < functions.length; i++) {
            SqlFunction function = functions[i];
            if (function.isMatchParamType(argTypes)) {
                candidates.add(function);
            }
        }

        if (candidates.size() == 0) {
            return null;
        } else if (candidates.size() == 1) {
            return (SqlFunction) candidates.get(1);
        }

        // Next, consider each argument of the function invocation, from left to right. For each argument,
        // eliminate all functions that are not the best match for that argument. The best match for a given
        // argument is the first data type appearing in the precedence list corresponding to the argument data
        // type in Table 3 for which there exists a function with a parameter of that data type. Lengths,
        // precisions, scales and the "FOR BIT DATA" attribute are not considered in this comparison.
        // For example, a DECIMAL(9,1) argument is considered an exact match for a DECIMAL(6,5) parameter,
        // and a VARCHAR(19) argument is an exact match for a VARCHAR(6) parameter.
        // Reference: http://www.pdc.kth.se/doc/SP/manuals/db2-5.0/html/db2s0/db2s067.htm#HDRUDFSEL
        //
        for (int i = 0; i < argTypes.length; i++) {
            throw Util.needToImplement("Function resolution with different "
                + "types is not implemented yet.");
        }
        return null;
    }

    /**
     * Returns a list of all functions and operators in this table.
     * Used for automated testing.
     */
    public ArrayList getOperatorList()
    {
        ArrayList list = new ArrayList();

        Iterator it = operators.entryIterMulti();
        while (it.hasNext()) {
            Map.Entry mapEntry = (Map.Entry) it.next();
            SqlOperator operator = (SqlOperator) mapEntry.getValue();
            list.add(operator);
        }

        it = mapNameToFunc.entryIterMulti();
        while (it.hasNext()) {
            Map.Entry mapEntry = (Map.Entry) it.next();
            SqlFunction function = (SqlFunction) mapEntry.getValue();
            list.add(function);
        }

        return list;
    }

    /**
     * Helper function that recreates a given RelDataType with nullablility
     * iff any of a calls operand types are nullable.
     */
    public final static RelDataType makeNullableIfOperandsAre(
        final SqlValidator validator,
        final SqlValidator.Scope scope,
        final SqlCall call,
        RelDataType type)
    {
        for (int i = 0; i < call.operands.length; ++i) {
            if (call.operands[i] instanceof SqlSymbol) {
                continue;
            }

            RelDataType operandType =
                validator.deriveType(scope, call.operands[i]);

            if (operandType.isNullable()) {
                type =
                    validator.typeFactory.createTypeWithNullability(type, true);
                break;
            }
        }
        return type;
    }

    /**
     * Helper function that recreates a given RelDataType with nullablility
     * iff any of a {@param argTypes} are nullable.
     */
    public final static RelDataType makeNullableIfOperandsAre(
        final RelDataTypeFactory typeFactory,
        final RelDataType [] argTypes,
        RelDataType type)
    {
        for (int i = 0; i < argTypes.length; ++i) {
            if (argTypes[i].isNullable()) {
                type = typeFactory.createTypeWithNullability(type, true);
                break;
            }
        }
        return type;
    }
}


// End SqlOperatorTable.java
