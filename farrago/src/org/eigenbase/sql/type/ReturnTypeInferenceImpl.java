package org.eigenbase.sql.type;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.SqlValidator;
import org.eigenbase.sql.SqlCollation;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.util.Util;
import org.eigenbase.resource.EigenbaseResource;

/**
 * Strategies implementations to infer the type of an operator call from the
 * type of the operands.
 *
 * <p>This interface is an example of the
 * {@link org.eigenbase.util.Glossary#StrategyPattern strategy pattern}.
 * This makes sense because many operators have similar, straightforward
 * strategies, such as to take the type of the first operand.</p>
 *
 * @author Wael Chatila
 * @since Dec 16, 2004
 * @version $Id$
 */
public class ReturnTypeInferenceImpl
{
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
         * @return A new type depending on the operands types
         */
        RelDataType getType(
            SqlValidator validator,
            SqlValidator.Scope scope,
            RelDataTypeFactory typeFactory,
            CallOperands callOperands,
            RelDataType typeToTransform);
    }

    /**
     * Strategy to infer the type of an operator call from the type of the
     * operands by using one {@link ReturnTypeInference} rule and a combination of
     * {@link Transform}s
     */
    public static class TransformCascade implements ReturnTypeInference
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
            RelDataTypeFactory typeFactory,
            CallOperands callOperands)
        {
            RelDataType ret =
                rule.getType(validator, scope, typeFactory, callOperands);
            for (int i = 0; i < transforms.length; i++) {
                Transform transform = transforms[i];
                ret = transform.getType(
                    validator, scope, typeFactory, callOperands, ret);
            }
            return ret;
        }
    }

    /**
     * Strategy to infer the type of an operator call from the type of the
     * operands by using a series of {@link ReturnTypeInference} rules in a
     * given order. If a rule fails to find a return type (by returning NULL),
     * next rule is tried until there are no more rules in which case NULL will
     * be returned.
     */
    public static class FallbackCascade implements ReturnTypeInference
    {
        final ReturnTypeInference[] rules;

        /**
         * Creates a FallbackCascade from an array of rules
         *
         * @pre null!=rules
         * @pre null!=rules[i]
         * @pre rules.length > 0
         */
        public FallbackCascade(
            ReturnTypeInference[] rules)
        {
            Util.pre(null != rules, "null!=rules");
            Util.pre(rules.length > 0, "rules.length>0");
            for (int i = 0; i < rules.length; i++) {
                Util.pre(rules[i] != null, "transforms[i] != null");
            }
            this.rules = rules;
        }

        /**
         * Creates a FallbackCascade from two rules
         */
        public FallbackCascade(
            ReturnTypeInference rule1,
            ReturnTypeInference rule2)
        {
            this(new ReturnTypeInference[] { rule1, rule2 });
        }

        public RelDataType getType(
            SqlValidator validator,
            SqlValidator.Scope scope,
            RelDataTypeFactory typeFactory,
            CallOperands callOperands)
        {
            RelDataType ret = null;
            for (int i = 0; i < rules.length; i++) {
                ReturnTypeInference rule = rules[i];
                ret = rule.getType(validator, scope, typeFactory, callOperands);
                if (null!=ret) {
                    break;
                }
            }
            return ret;
        }
    }

    /**
     * Returns the first type that matches a set of given {@link SqlTypeName}s.
     * If not match could be found, null is returned
     */
    private static class TypeMatchReturnTypeInference implements ReturnTypeInference {
        private final int start;
        private final SqlTypeName[] typeNames;

        /**
         * Returns the type at element start (zero based)
         * @see {@link TypeMatchReturnTypeInference(int, SqlTypeName[])}
         */
        public TypeMatchReturnTypeInference(int start) {
            this(start, SqlTypeName.Any);
        }

        /**
         * Returns the first type of typeName at or after position start
         * (zero based)
         * @see {@link TypeMatchReturnTypeInference(int, SqlTypeName[])}
         */
        public TypeMatchReturnTypeInference(int start, SqlTypeName typeName) {
            this(start, new SqlTypeName[]{typeName});
        }

        /**
         * Returns the first type matching any type in typeNames at or after
         * postition start (zero based)
         * @pre start>=0
         * @pre null!=typeNames
         * @pre typeNames.length>0
         */
        public TypeMatchReturnTypeInference(int start, SqlTypeName[] typeNames) {
            Util.pre(start>=0,"start>=0");
            Util.pre(null!=typeNames,"null!=typeNames");
            Util.pre(typeNames.length>0,"typeNames.length>0");
            this.start = start;
            this.typeNames = typeNames;
        }

        public RelDataType getType(
            SqlValidator validator,
            SqlValidator.Scope scope,
            RelDataTypeFactory typeFactory,
            CallOperands callOperands) {
            for (int i = start; i < callOperands.size(); i++) {
                RelDataType argType = callOperands.getType(i);
                if (SqlTypeUtil.isOfSameTypeName(typeNames, argType)) {
                    return argType;
                }
            }
            return null;
        }
    }

    /**
     * Returns the type of position ordinal (zero based)
     */
    public static class OrdinalReturnTypeInference implements ReturnTypeInference {
        int ordinal;

        public OrdinalReturnTypeInference(int ordinal) {
            this.ordinal = ordinal;
        }

        public RelDataType getType(
            SqlValidator validator,
            SqlValidator.Scope scope,
            RelDataTypeFactory typeFactory,
            CallOperands callOperands) {
            return callOperands.getType(ordinal);
        }
    }

    /**
     * A {@link ReturnTypeInference} which always returns the same SQL type.
     */
    public static class FixedReturnTypeInference implements ReturnTypeInference
    {
        private final int argCount;
        private final SqlTypeName typeName;
        private final int length;
        private final int scale;
        private RelDataType type;

        public FixedReturnTypeInference(RelDataType type)
        {
            this.type = type;
            this.typeName = null;
            this.length = -1;
            this.scale = -1;
            this.argCount = 0;
        }

        public FixedReturnTypeInference(SqlTypeName typeName)
        {
            this.argCount = 1;
            this.typeName = typeName;
            this.length = -1;
            this.scale = -1;
        }

        public FixedReturnTypeInference(SqlTypeName typeName, int length)
        {
            this.argCount = 2;
            this.typeName = typeName;
            this.length = length;
            this.scale = -1;
        }

        public FixedReturnTypeInference(
            SqlTypeName typeName, int length, int scale)
        {
            this.argCount = 3;
            this.typeName = typeName;
            this.length = length;
            this.scale = scale;
        }

        public RelDataType getType(
            SqlValidator validator,
            SqlValidator.Scope scope,
            RelDataTypeFactory typeFactory,
            CallOperands callOperands)
        {
            if (type == null) {
                type = createType(typeFactory);
            }
            return type;
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
                SqlValidator validator,
                SqlValidator.Scope scope,
                RelDataTypeFactory typeFactory,
                CallOperands callOperands,
                RelDataType typeToTransform)
            {
                return SqlTypeUtil.makeNullableIfOperandsAre(
                    typeFactory,
                    callOperands.collectTypes(),
                    typeToTransform);
            }
        };

    /**
     * Parameter type-inference transform strategy where a derived INTERVAL type
     * is transformed into the same type but possible with a different
     * {@link org.eigenbase.sql.SqlIntervalQualifier}.
     * If the type to transform is not of a INTERVAL type, this transformation
     * does nothing.
     * @see {@link IntervalSqlType}
     */
    public static final Transform toLeastRestrictiveInterval =
        new Transform() {
            public RelDataType getType(
                SqlValidator validator,
                SqlValidator.Scope scope,
                RelDataTypeFactory typeFactory,
                CallOperands callOperands,
                RelDataType typeToTransform)
            {
                if (typeToTransform instanceof IntervalSqlType) {
                    IntervalSqlType it =
                       (IntervalSqlType) typeToTransform;
                    for (int i = 0; i < callOperands.size(); i++) {
                        it = it.combine((IntervalSqlType)
                                            callOperands.getType(i));
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
                SqlValidator validator,
                SqlValidator.Scope scope,
                RelDataTypeFactory fac,
                CallOperands callOperands,
                RelDataType typeToTransform)
            {
                switch (typeToTransform.getSqlTypeName().ordinal) {
                case SqlTypeName.Varchar_ordinal:
                case SqlTypeName.Varbinary_ordinal:
                case SqlTypeName.Varbit_ordinal:
                    return typeToTransform;
                }

                SqlTypeName retTypeName = toVar(typeToTransform);

                RelDataType ret =
                    fac.createSqlType(
                        retTypeName,
                        typeToTransform.getPrecision());
                if (SqlTypeUtil.inCharFamily(typeToTransform)) {
                    ret = fac.createTypeWithCharsetAndCollation(
                            ret,
                            typeToTransform.getCharset(),
                            typeToTransform.getCollation());
                }
                return fac.createTypeWithNullability(
                    ret,
                    typeToTransform.isNullable());
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
     * @see {@link MultisetSqlType#getComponentType}
     */
    public static final Transform toMultisetElementType =
        new Transform() {
            public RelDataType getType(
                SqlValidator validator,
                SqlValidator.Scope scope,
                RelDataTypeFactory typeFactory,
                CallOperands callOperands,
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
        new OrdinalReturnTypeInference(0);

    /**
     * Type-inference strategy whereby the result type of a call is the type of
     * the first operand. If any of the other operands are nullable the returned
     * type will also be nullable.
     */
    public static final ReturnTypeInference useNullableFirstArgType =
        new TransformCascade(useFirstArgType, toNullable);

    public static final ReturnTypeInference useFirstInterval =
        new TypeMatchReturnTypeInference(0, SqlTypeName.timeIntervalTypes);

    public static final ReturnTypeInference useNullableFirstInterval =
        new TransformCascade(useFirstInterval, toNullable);

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
        new OrdinalReturnTypeInference(1);

    /**
     * Type-inference strategy whereby the result type of a call is the type of
     * the third operand.
     */
    public static final ReturnTypeInference useThirdArgType =
        new OrdinalReturnTypeInference(2);



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
     * Type-inference strategy whereby the result type of a call is nullable Time(0).
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
     * Type-inference strategy which always returns "VARCHAR(2000)".
     */
    public static final ReturnTypeInference useVarchar2000 =
        new FixedReturnTypeInference(SqlTypeName.Varchar, 2000);

    /**
     * Type-inference strategy whereby the result type of a call is using its
     * operands biggest type, using the SQL:1999 rules described in 
     * "Data types of results of aggregations".
     * These rules are used in union, except, intercept, case and other places.
     *
     * @sql.99 Part 2 Section 9.3
     *
     * <p>For example, the expression <code>(500000000000 + 3.0e-3)</code> has
     * the operands INTEGER and DOUBLE. Its biggest type is double.
     */
    public static final ReturnTypeInference useLeastRestrictive =
        new ReturnTypeInference() {
            public RelDataType getType(
                SqlValidator validator,
                SqlValidator.Scope scope,
                RelDataTypeFactory typeFactory,
                CallOperands callOperands)
            {
                // REVIEW jvs 1-Mar-2004: I changed this to
                // leastRestrictive since that's its purpose (and at least
                // for Farrago, works better than the old getBiggest code).
                // But this type inference rule isn't general enough for
                // numeric types, which use different rules depending on
                // the operator (e.g. sum length is based on the max of
                // the arg precisions, while product length is based on
                // the sum of the arg precisions).
                return typeFactory.leastRestrictive(
                            callOperands.collectTypes());
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

    public static final ReturnTypeInference useNullableMutliplyDivison =
        new FallbackCascade(useNullableFirstInterval, useNullableBiggest);

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
            /**
             * @pre SqlTypeUtil.sameNamedType(argTypes[0], (argTypes[1]))
             */
            public RelDataType getType(
                SqlValidator validator,
                SqlValidator.Scope scope,
                RelDataTypeFactory typeFactory,
                CallOperands callOperands)
            {
                if (!(SqlTypeUtil.inCharFamily(callOperands.getType(0))
                        && SqlTypeUtil.inCharFamily(callOperands.getType(1))))
                {
                    Util.pre(
                        SqlTypeUtil.sameNamedType(
                            callOperands.getType(0), callOperands.getType(1)),
                        "SqlTypeUtil.sameNamedType(argTypes[0], argTypes[1])");
                }
                SqlCollation pickedCollation = null;
                if (SqlTypeUtil.inCharFamily(callOperands.getType(0))) {
                    if (!SqlTypeUtil.isCharTypeComparable(
                            callOperands.collectTypes(), 0, 1)) {
                        throw EigenbaseResource.instance()
                            .newTypeNotComparable(
                                callOperands.getType(0).toString(),
                                callOperands.getType(1).toString());
                    }

                    pickedCollation =
                        SqlCollation.getCoercibilityDyadicOperator(
                            callOperands.getType(0).getCollation(),
                            callOperands.getType(1).getCollation());
                    assert (null != pickedCollation);
                }

                RelDataType ret;
                ret = typeFactory.createSqlType(
                        callOperands.getType(0).getSqlTypeName(),
                        callOperands.getType(0).getPrecision()
                        + callOperands.getType(1).getPrecision());
                if (null != pickedCollation) {
                    RelDataType pickedType;
                    if (callOperands.getType(0).getCollation().equals(pickedCollation)) {
                        pickedType = callOperands.getType(0);
                    } else if (callOperands.getType(1).getCollation().equals(pickedCollation)) {
                        pickedType = callOperands.getType(1);
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
     * as a {@link org.eigenbase.sql.SqlValidator.Namespace}, and therefore the result type of
     * the call is the type of that namespace.
     */
    public static final ReturnTypeInference useScope =
        new ReturnTypeInference() {
            public RelDataType getType(
                SqlValidator validator,
                SqlValidator.Scope scope,
                RelDataTypeFactory typeFactory,
                CallOperands callOperands)
            {
                return validator.getNamespace(
                    (SqlNode) callOperands.getUnderlyingObject()).getRowType();
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
                RelDataTypeFactory typeFactory,
                CallOperands callOperands)
            {
                RelDataType[] argElementTypes = new RelDataType[callOperands.size()];
                for (int i = 0; i < callOperands.size(); i++) {
                    argElementTypes[i] = callOperands.getType(i).getComponentType();
                }

                CallOperands.RelDataTypesCallOperands types = new
                    CallOperands.RelDataTypesCallOperands(argElementTypes);
                RelDataType biggestElementType = useBiggest.
                    getType(validator, scope,typeFactory, types);
                return  typeFactory.createMultisetType(biggestElementType, -1);
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

// End ReturnTypeInferenceImpl.java
