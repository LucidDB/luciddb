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
package org.eigenbase.sql.type;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeFactoryImpl;
import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.SqlCall;
import org.eigenbase.sql.SqlCollation;
import org.eigenbase.sql.SqlValidator;
import org.eigenbase.util.Util;

/**
 * Strategy to infer the type of an operator call from the type of the
 * operands.
 *
 * <p>This class is an example of the
 * {@link org.eigenbase.util.Glossary#StrategyPattern strategy pattern}.
 * This makes sense because many operators have similar, straightforward
 * strategies, such as to take the type of the first operand.</p>
 *
 * @author Wael Chatila
 * @since Sept 8, 2004
 * @version $Id$
 */
public abstract class ReturnTypeInference
{
    // REVIEW jvs 26-May-2004:  I think we should try to eliminate one
    // of these methods; they are redundant.
    public abstract RelDataType getType(
        SqlValidator validator,
        SqlValidator.Scope scope,
        SqlCall call);

    public abstract RelDataType getType(
        RelDataTypeFactory typeFactory,
        RelDataType [] argTypes);

    /**
     * Iterates over all of the call's operands and derive their types
     * before calling and returning the result from
     * {@link #getType(org.eigenbase.reltype.RelDataTypeFactory, org.eigenbase.reltype.RelDataType[])}
     */
    protected final RelDataType collectTypesFromCall(
        SqlValidator validator,
        SqlValidator.Scope scope,
        SqlCall call)
    {
        return getType(
            validator.typeFactory,
            TypeUtil.collectTypes(validator, scope, call.operands));
    }

    //~ INNER CLASSES --------------------------------------------
    /**
     * Strategy to transform one type to another. The transformation is dependent on
     * the implemented strategy object and in the general case is a function of
     * the type and the other operands
     *
     * Can not be used by itself. Must be used in an object of type
     * {@link TransformCascade}

     * <p>This class is an example of the
     * {@link org.eigenbase.util.Glossary#StrategyPattern strategy pattern}.
     * This makes sense because many operators have similar, straightforward
     * strategies, such as to take the type of the first operand.</p>
     */
    public interface Transform
    {
        /**
         * @param typeToTransform The type subject of transformation. The return
         * type is (in the general case) a function of
         * <ul><li>The typeToTransform</li><li>The other operand types</li></ul>
         * {@link ReturnTypeInference}  object.
         * @return A new type depending on
         * {@param typeToTransform} and {@param argTypes}
         */
        RelDataType getType(
            RelDataTypeFactory typeFactory,
            RelDataType [] argTypes,
            RelDataType typeToTransform);
    }

    /**
     * Strategy to infer the type of an operator call from the type of the
     * operands by using one {@link ReturnTypeInference} rule and a combination of
     * {@link Transform}s
     */
    public static class TransformCascade extends ReturnTypeInference
    {
        final ReturnTypeInference rule;
        final Transform [] transforms;

        /**
         * Creates a TransformCascade from a rule and an array of one or
         * more transforms.
         *
         * @pre null!=rule
         * @pre null!=transforms
         * @pre transforms.length > 0
         * @pre transforms[i] != null
         */
        public TransformCascade(
            ReturnTypeInference rule,
            Transform [] transforms)
        {
            Util.pre(null != rule, "null!=rule");
            Util.pre(null != transforms, "null!=transforms");
            Util.pre(transforms.length > 0, "transforms.length>0");
            for (int i = 0; i < transforms.length; i++) {
                Util.pre(transforms[i] != null, "transforms[i] != null");
            }
            this.rule = rule;
            this.transforms = transforms;
        }

        /**
         * Creates a TransformCascade from a rule and a single transform.
         *
         * @pre null!=rule
         * @pre null!=transform
         */
        public TransformCascade(
            ReturnTypeInference rule,
            Transform transform)
        {
            this(rule, new Transform [] { transform });
        }

        /**
         * Creates a TransformCascade from a rule and two transforms.
         *
         * @pre null!=rule
         * @pre null!=transform0
         * @pre null!=transform1
         */
        public TransformCascade(
            ReturnTypeInference rule,
            Transform transform0,
            Transform transform1)
        {
            this(rule, new Transform [] { transform0, transform1 });
        }

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
            RelDataType ret = rule.getType(typeFactory, argTypes);
            for (int i = 0; i < transforms.length; i++) {
                Transform transform = transforms[i];
                ret = transform.getType(typeFactory, argTypes, ret);
            }
            return ret;
        }
    }

    /**
     * A {@link ReturnTypeInference} which always returns the same SQL type.
     */
    private static class FixedReturnTypeInference extends ReturnTypeInference
    {
        private final int argCount;
        private final SqlTypeName typeName;
        private final int length;
        private final int scale;

        FixedReturnTypeInference(SqlTypeName typeName)
        {
            this.argCount = 1;
            this.typeName = typeName;
            this.length = -1;
            this.scale = -1;
        }

        FixedReturnTypeInference(SqlTypeName typeName, int length)
        {
            this.argCount = 2;
            this.typeName = typeName;
            this.length = length;
            this.scale = -1;
        }

        FixedReturnTypeInference(SqlTypeName typeName, int length, int scale)
        {
            this.argCount = 3;
            this.typeName = typeName;
            this.length = length;
            this.scale = scale;
        }

        public RelDataType getType(
            SqlValidator validator,
            SqlValidator.Scope scope,
            SqlCall call)
        {
            return createType(validator.typeFactory);
        }

        public RelDataType getType(
            RelDataTypeFactory typeFactory,
            RelDataType[] argTypes)
        {
            return createType(typeFactory);
        }

        private RelDataType createType(RelDataTypeFactory typeFactory) {
            switch (argCount) {
            case 1:
                return typeFactory.createSqlType(typeName);
            case 2:
                return typeFactory.createSqlType(typeName, length);
            case 3:
                return typeFactory.createSqlType(typeName, length, scale);
            default:
                throw Util.newInternal("unexpected argCount " + argCount);
            }
        }
    }

    //~  IMPLEMENTATIONS -----------------------------------------

    /**
     * Parameter type-inference transform strategy where a derived type is
     * transformed into the same type but nullable if any of a calls operands
     * is nullable
     */
    public static final Transform toNullable =
        new Transform() {
            public RelDataType getType(
                RelDataTypeFactory typeFactory,
                RelDataType [] argTypes,
                RelDataType typeToTransform)
            {
                return TypeUtil.makeNullableIfOperandsAre(typeFactory, argTypes,
                    typeToTransform);
            }
        };

    /**
     * Parameter type-inference transform strategy where a derived INTERVAL type
     * is transformed into the same type but possible with a different
     * {@link org.eigenbase.sql.SqlIntervalQualifier}.
     * If the type to transform is not of a INTERVAL type, this transformation
     * does nothing.
     * @see {@link RelDataTypeFactoryImpl.IntervalSqlType}
     */
    public static final Transform toLeastRestrictiveInterval =
        new Transform() {
            public RelDataType getType(
                RelDataTypeFactory typeFactory,
                RelDataType [] argTypes,
                RelDataType typeToTransform)
            {
                if (typeToTransform instanceof
                    RelDataTypeFactoryImpl.IntervalSqlType) {
                    RelDataTypeFactoryImpl.IntervalSqlType it =
                       (RelDataTypeFactoryImpl.IntervalSqlType) typeToTransform;
                    for (int i = 0; i < argTypes.length; i++) {
                        it = it.combine((RelDataTypeFactoryImpl.IntervalSqlType)
                            argTypes[i]);
                    }
                    return it;
                }
                return typeToTransform;
            }
        };
    /**
     * Type-inference strategy whereby the result type of a call is VARYING
     * the type given.
     * The length returned is the same as length of the
     * first argument.
     * Return type will have same nullablilty as input type nullablility.
     * First Arg must be of string type.
     */
    public static final Transform toVarying =
        new Transform() {
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
     * Parameter type-inference transform strategy where a derived type
     * must be a multiset type and the returned type is the multiset's
     * element type.
     * @see {@link RelDataTypeFactoryImpl.MultisetSqlType#getComponentType}
     */
    public static final Transform toMultisetElementType =
        new Transform() {
            public RelDataType getType(
                RelDataTypeFactory typeFactory,
                RelDataType [] argTypes,
                RelDataType typeToTransform)
            {
                return typeToTransform.getComponentType();
            }
        };

    /**
     * Type-inference strategy whereby the result type of a call is the type of
     * the first operand.
     */
    public static final ReturnTypeInference useFirstArgType =
        new ReturnTypeInference() {
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
    public static final ReturnTypeInference useNullableFirstArgType =
        new TransformCascade(useFirstArgType, toNullable);

    /**
     * Type-inference strategy whereby the result type of a call is VARYING
     * the type of the first argument.
     * The length returned is the same as length of the
     * first argument.
     * If any of the other operands are nullable the returned
     * type will also be nullable.
     * First Arg must be of string type.
     */
    public static final ReturnTypeInference useNullableVaryingFirstArgType =
        new TransformCascade(useFirstArgType,
            toNullable, toVarying);

    /**
     * Type-inference strategy whereby the result type of a call is the type of
     * the second operand.
     */
    public static final ReturnTypeInference useSecondArgType =
        new ReturnTypeInference() {
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
    public static final ReturnTypeInference useBoolean =
        new FixedReturnTypeInference(SqlTypeName.Boolean);

    /**
     * Type-inference strategy whereby the result type of a call is Boolean,
     * with nulls allowed if any of the operands allow nulls.
     */
    public static final ReturnTypeInference useNullableBoolean =
        new TransformCascade(useBoolean, toNullable);

    /**
     * Type-inference strategy whereby the result type of a call is Date.
     */
    public static final ReturnTypeInference useDate =
        new FixedReturnTypeInference(SqlTypeName.Date);

    /**
     * Type-inference strategy whereby the result type of a call is Time(0).
     */
    public static final ReturnTypeInference useTime =
        new FixedReturnTypeInference(SqlTypeName.Time, 0);

    /**
     * Type-inference strategy whereby the result type of a call is nullable Time.
     */
     public static final ReturnTypeInference useNullableTime =
        new TransformCascade(useTime, toNullable);

    /**
     * Type-inference strategy whereby the result type of a call is Double.
     */
    public static final ReturnTypeInference useDouble =
        new FixedReturnTypeInference(SqlTypeName.Double);

    /**
     * Type-inference strategy whereby the result type of a call is Double
     * with nulls allowed if any of the operands allow nulls.
     */
    public static final ReturnTypeInference useNullableDouble =
        new TransformCascade(useDouble, toNullable);

    /**
     * Type-inference strategy whereby the result type of a call is an Integer.
     */
    public static final ReturnTypeInference useInteger =
        new FixedReturnTypeInference(SqlTypeName.Integer);

    /**
     * Type-inference strategy whereby the result type of a call is an Integer
     * with nulls allowed if any of the operands allow nulls.
     */
    public static final ReturnTypeInference useNullableInteger =
        new TransformCascade(useInteger, toNullable);

    /**
     * Type-inference strategy which always returns "VARCHAR(30)".
     */
    public static final ReturnTypeInference useVarchar30 =
        new FixedReturnTypeInference(SqlTypeName.Varchar, 30, 0);

    /**
     * Type-inference strategy whereby the result type of a call is using its
     * operands biggest type, using the rules described in ISO/IEC 9075-2:1999
     * section 9.3 "Data types of results of aggregations".
     * These rules are used in union, except, intercect, case and other places.
     *
     * <p>For example, the expression <code>(500000000000 + 3.0e-3)</code> has
     * the operands INTEGER and DOUBLE. Its biggest type is double.
     */
    private static final ReturnTypeInference useLeastRestrictive =
        new ReturnTypeInference() {
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
                // the operator (e.g. sum length is based on the max of
                // the arg precisions, while product length is based on
                // the sum of the arg precisions).
                return typeFactory.leastRestrictive(argTypes);
            }
        };

    /**
     * Same as {@link #useLeastRestrictive} but with INTERVAL aswell.
     */
    public static final ReturnTypeInference useBiggest =
        new TransformCascade(useLeastRestrictive, toLeastRestrictiveInterval);

    /**
     * Type-inference strategy similar to {@link #useBiggest}, except that the
     * result is nullable if any of the arguments is nullable.
     */
    public static final ReturnTypeInference useNullableBiggest =
        new TransformCascade(useLeastRestrictive, toLeastRestrictiveInterval, toNullable);

    /**
     * Type-inference strategy where by the
     * Result type of a call is
     * <ul>
     * <li>the same type as the input types but with the
     * combined length of the two first types</li>
     * <li>If types are of char type the type with the highest coercibility
     * will be used</li>
     *
     * @pre input types must be of the same string type
     * @pre types must be comparable without casting
     */
    public static final ReturnTypeInference useDyadicStringSumPrecision =
        new ReturnTypeInference() {
            public RelDataType getType(
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlCall call)
            {
                return collectTypesFromCall(validator, scope, call);
            }

            /**
             * @pre argTypes[0].isSameType(argTypes[1])
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
                    if (!TypeUtil.isCharTypeComparable(argTypes, 0, 1)) {
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
     * {@link #toNullable}
     */
    public static final ReturnTypeInference useNullableDyadicStringSumPrecision =
        new TransformCascade(useDyadicStringSumPrecision,
            new Transform [] { toNullable });

    /**
     * Same as {@link #useDyadicStringSumPrecision} and using
     * {@link #toNullable}, {@link #toVarying}
     */
    public static final ReturnTypeInference useNullableVaryingDyadicStringSumPrecision =
        new TransformCascade(useDyadicStringSumPrecision,
            new Transform [] {
                toNullable, toVarying
            });

    /**
     * Type-inference strategy where the expression is assumed to be registered
     * as a {@link SqlValidator.Namespace}, and therefore the result type of
     * the call is the type of that namespace.
     */
    public static final ReturnTypeInference useScope =
        new ReturnTypeInference() {
            public RelDataType getType(
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlCall call)
            {
                return validator.getNamespace(call).getRowType();
            }

            public RelDataType getType(
                RelDataTypeFactory typeFactory,
                RelDataType [] argTypes)
            {
                throw new UnsupportedOperationException();
            }
        };

    /**
     * Returns the same type as the multiset carries. The multiset type returned
     * is the least restrictive of the call's multiset operands
     */
    public static final ReturnTypeInference useMultiset =
        new ReturnTypeInference() {
            public RelDataType getType(
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlCall call)
            {
                return getType(validator.typeFactory,
                    TypeUtil.collectTypes(validator, scope, call.operands));
            }

            public RelDataType getType(
                RelDataTypeFactory typeFactory,
                RelDataType [] argTypes)
            {
                RelDataType[] argElementTypes = new RelDataType[argTypes.length];
                for (int i = 0; i < argTypes.length; i++) {
                    argElementTypes[i] = argTypes[i].getComponentType();
                }

                RelDataType biggestElementType = ReturnTypeInference.useBiggest.
                    getType(typeFactory, argElementTypes);
                return  typeFactory.createMultisetType(biggestElementType);
            }
        };

    /**
     * Same as {@link #useMultiset} but returns with nullablity
     * if any of the operands is nullable
     */
    public static final ReturnTypeInference useNullableMultiset =
        new TransformCascade(useMultiset, toNullable);

    /**
     * Returns the element type of a multiset
     */
    public static final ReturnTypeInference useNullableMultisetElementType =
        new TransformCascade(useMultiset, toMultisetElementType);

}

// End ReturnTypeInference.java
