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

import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.*;
import org.eigenbase.util.Util;
import org.eigenbase.util.EnumeratedValues;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactoryImpl;
import org.eigenbase.resource.EigenbaseResource;

import java.util.ArrayList;

/**
 * Strategies to check for allowed operand types of an operator call.
 *
 * @author Wael Chatila
 * @since Sept 8, 2004
 * @version $Id$
 */
public abstract class OperandsTypeChecking
{
    /**
     * Checks if a node is of correct type
     * @param call
     * @param validator
     * @param scope
     * @param node
     * @param ruleOrdinal
     *
     * Note that <code>ruleOrdinal</code> is <i>not</i> an index in any
     * call.operands[] array. It's rather used to specify which
     * signature the node should correspond too.
     *
     * <p>For example, if we have typeStringInt, a check can be made to see
     * if a <code>node</code> is of type int by calling
     * <code>typeStringInt.check(validator,scope,node,1);</code>
     */
    public abstract boolean check(
        SqlCall call,
        SqlValidator validator,
        SqlValidator.Scope scope,
        SqlNode node,
        int ruleOrdinal,
        boolean throwOnFailure);

    public abstract boolean check(
        SqlValidator validator,
        SqlValidator.Scope scope,
        SqlCall call,
        boolean throwOnFailure);

    /**
     * Returns the argument count
     */
    public abstract int getArgCount();

    /**
         * Returns a string describing the expected argument types of a call, e.g.
         * "SUBSTR(VARCHAR, INTEGER, INTEGER)".
         */
    public abstract String getAllowedSignatures(SqlOperator op);

    /**
     * Operand type-checking strategy which checks operands against an array
     * of types.
     *
     * <p>For example, the following would allow
     * <code>foo(Date)</code> or
     * <code>foo(INTEGER, INTEGER)</code> but disallow
     * <code>foo(INTEGER)</code>.
     *
     * <blockquote><pre>SimpleOperandsTypeChecking(new SqlTypeName[][] {
     *     {SqlTypeName.Date},
     *     {SqlTypeName.Int, SqlTypeName.Int}})</pre></blockquote>
     */
    public static class SimpleOperandsTypeChecking extends OperandsTypeChecking
    {
        protected SqlTypeName[][] types;

        /**
         * Creates a SimpleOperandsTypeChecking object.
         *
         * @pre types != null
         * @pre types.length > 0
         */
        public SimpleOperandsTypeChecking(SqlTypeName[][] typeses)
        {
            Util.pre(null != typeses, "null!=types");
//            Util.pre(typeses.length > 0, "types.length>0");

            //only Null types specified? Prohibit! need more than null
            for (int i = 0; i < typeses.length; i++) {
                final SqlTypeName[] types = typeses[i];
                Util.pre(types.length > 0, "Need to define a type");
                boolean foundOne = false;
                for (int j = 0; j < types.length; j++) {
                    SqlTypeName type = types[j];
                    if (!type.equals(SqlTypeName.Null)) {
                        foundOne = true;
                        break;
                    }
                }

                if (!foundOne) {
                    Util.pre(false, "Must have at least one non-null type");
                }
            }

            this.types = typeses;
        }

        public boolean check(
            SqlCall call,
            SqlValidator validator,
            SqlValidator.Scope scope,
            SqlNode node,
            int ruleOrdinal,
            boolean throwOnFailure)
        {
            RelDataType anyType = validator.anyType;
            RelDataType actualType = null;

            //for each operand, iterater over its allowed types...
            for (int j = 0; j < types[ruleOrdinal].length; j++) {
                SqlTypeName typeName = types[ruleOrdinal][j];
                RelDataType expectedType =
                    RelDataTypeFactoryImpl.createSqlTypeIgnorePrecOrScale(validator.typeFactory,
                        typeName);
                assert (expectedType != null);
                if (anyType.equals(expectedType)) {
                    // If the argument type is defined as any type, we don't need to check
                    return true;
                } else {
                    if( null == actualType) {
                        actualType = validator.deriveType(scope, node);
                    }

                    if (expectedType.isAssignableFrom(actualType, false)) {
                        return true;
                    }
                }
            }

            if (throwOnFailure) {
                throw call.newValidationSignatureError(validator, scope);
            }
            return false;
        }

        public boolean check(
            SqlValidator validator,
            SqlValidator.Scope scope,
            SqlCall call,
            boolean throwOnFailure)
        {
           assert (getArgCount() == call.operands.length);

            for (int i = 0; i < call.operands.length; i++) {
                SqlNode operand = call.operands[i];
                if (!check(call, validator, scope, operand, i, throwOnFailure)) {
                    return false;
                }
            }
            return true;
        }

        public int getArgCount()
        {
            return types.length;
        }

        public SqlTypeName [][] getTypes()
        {
            return types;
        }

        public String getAllowedSignatures(SqlOperator op)
        {
            StringBuffer buf = new StringBuffer();
            ArrayList list = new ArrayList();
            getAllowedSignatures(0, list, buf, op);
            return buf.toString().trim();
        }

        /**
         * Helper function to {@link #getAllowedSignatures(org.eigenbase.sql.SqlOperator)}
         */
        protected void getAllowedSignatures(
            int depth,
            ArrayList list,
            StringBuffer buf,
            SqlOperator op)
        {
            assert (null != types[depth]);
            assert (types[depth].length > 0);

            for (int i = 0; i < types[depth].length; i++) {
                SqlTypeName type = types[depth][i];
                if (type.equals(SqlTypeName.Null)) {
                    continue;
                }

                list.add(type);
                if ((depth + 1) < types.length) {
                    getAllowedSignatures(depth + 1, list, buf, op);
                } else {
                    buf.append(op.getAnonymousSignature(list));
                    buf.append(SqlOperator.NL);
                }
                list.remove(list.size() - 1);
            }
        }
    }

    /**
     * This class allows multiple existing {@link SimpleOperandsTypeChecking} rules
     * to be combined into one rule.<p>
     * For example, giving an operand the signature of both a string or a numeric
     * could be done by:
     * <blockquote><pre><code>
     *
     * CompositeOperandsTypeChecking newCompositeRule =
     *  new SqlOperator.CompositeOperandsTypeChecking(
     *    Composition.OR,
     *    new SqlOperator.OperandsTypeChecking[]{stringRule, numericRule});
     *
     * </code></pre></blockquote>
     * Simmilary a rule that would only allow a numeric literal can be done by:
     * <blockquote><pre><code>
     *
     * CompositeOperandsTypeChecking newCompositeRule =
     *  new SqlOperator.CompositeOperandsTypeChecking(
     *    Composition.AND,
     *    new SqlOperator.OperandsTypeChecking[]{literalRule, numericRule});
     *
     * </code></pre></blockquote>
     */
    public static class CompositeOperandsTypeChecking
        extends OperandsTypeChecking
    {
        private OperandsTypeChecking[] allowedRules;
        private Composition composition;

        public CompositeOperandsTypeChecking(Composition composition,
            OperandsTypeChecking[] allowedRules)
        {
            Util.pre(null != allowedRules, "null != allowedRules");
            Util.pre(allowedRules.length > 1, "Not a composite type");
            int firstArgsLength = allowedRules[0].getArgCount();
            for (int i = 1; i < allowedRules.length; i++) {
                Util.pre(allowedRules[i].getArgCount() == firstArgsLength,
                    "All must have the same operand length");
            }
            this.allowedRules = allowedRules;
            this.composition = composition;
        }

        public OperandsTypeChecking[] getRules()
        {
            return allowedRules;
        }

        public String getAllowedSignatures(SqlOperator op)
        {
            StringBuffer ret = new StringBuffer();
            for (int i = 0; i < allowedRules.length; i++) {
                OperandsTypeChecking rule = allowedRules[i];
                if (i > 0) {
                    ret.append(SqlOperator.NL);
                }
                ret.append(rule.getAllowedSignatures(op));
            }
            return ret.toString();
        }

        public int getArgCount()
        {
            // Check made in constructor to verify that all rules have the same
            // number of arguments. Take and return the first one.
            return allowedRules[0].getArgCount();
        }

        public boolean check(
            SqlCall call,
            SqlValidator validator,
            SqlValidator.Scope scope,
            SqlNode node,
            int ruleOrdinal,
            boolean throwOnFailure)
        {
            Util.pre(allowedRules.length >= 1, "allowedRules.length>=1");
            int typeErrorCount = 0;

            boolean throwOnAndFailure =
                Composition.AND.equals(composition) && throwOnFailure;

            for (int i = 0; i < allowedRules.length; i++) {
                OperandsTypeChecking rule = allowedRules[i];
                if (!rule.check(call, validator, scope, node,
                    ruleOrdinal, throwOnAndFailure)) {
                    typeErrorCount++;
                }
            }

            boolean ret=false;
            if (Composition.AND.equals(composition)) {
                ret = typeErrorCount == 0;
            } else if (Composition.OR.equals(composition)) {
                ret = (typeErrorCount < allowedRules.length);
            } else {
                //should never come here
                throw Util.needToImplement(this);
            }

            if (!ret && throwOnFailure) {
                //in the case of a composite OR we want to throw an error
                //describing in more detail what the problem was, hence doing
                //the loop again
                for (int i = 0; i < allowedRules.length; i++) {
                    OperandsTypeChecking rule = allowedRules[i];
                    if (!rule.check(call, validator, scope, node,
                        ruleOrdinal, true)) {
                        typeErrorCount++;
                    }
                }
                //if no exception thrown, just throw a generic validation
                //signature error
                throw call.newValidationSignatureError(validator, scope);
            }

            return ret;
        }

        public boolean check(
            SqlValidator validator,
            SqlValidator.Scope scope,
            SqlCall call,
            boolean throwOnFailure)
        {
            int typeErrorCount = 0;

            for (int i = 0; i < allowedRules.length; i++) {
                OperandsTypeChecking rule = allowedRules[i];

                if (!rule.check(validator, scope, call, false)) {
                    typeErrorCount++;
                }
            }

            boolean failed = true;
            if (Composition.AND.equals(composition)) {
                failed = typeErrorCount>0;
            } else if (Composition.OR.equals(composition)) {
                failed = (typeErrorCount == allowedRules.length);
            }

            if (failed) {
                if (throwOnFailure) {
                    throw call.newValidationSignatureError(validator, scope);
                }
                return false;
            }
            return true;
        }

        //~ Inner Class ----------------------
        public static class Composition extends EnumeratedValues.BasicValue {
            private Composition(String name, int ordinal) {
                super(name, ordinal, null);
            }

            public static final Composition AND = new Composition("AND", 0);
            public static final Composition OR = new Composition("OR", 1);

            public static final EnumeratedValues enumeration =
                new EnumeratedValues(new Composition [] { AND, OR });
        }
    }

    /**
     * A Convenience class that is equivalent to
     * <code>CompositeOperandsTypeChecking(Composistion.AND, rules)</code>
     */
    public static class CompositeAndOperandsTypeChecking
        extends CompositeOperandsTypeChecking
    {
        public CompositeAndOperandsTypeChecking(
            OperandsTypeChecking[] allowedRules)
        {
            super(Composition.AND, allowedRules);
        }
    }

    /**
     * A Convenience class that is equivalent to
     * <code>CompositeOperandsTypeChecking(Composistion.OR, rules)</code>
     */
    public static class CompositeOrOperandsTypeChecking
        extends CompositeOperandsTypeChecking
    {
        public CompositeOrOperandsTypeChecking(
            OperandsTypeChecking[] allowedRules)
        {
            super(Composition.OR, allowedRules);
        }
    }

    //~ STRATEGIES  ------------------------------------------------------

    /**
     * Parameter type-checking strategy for a function which takes no
     * arguments.
     */
    public static final OperandsTypeChecking typeEmpty =
        new SimpleOperandsTypeChecking(new SqlTypeName[][] {});

    /**
     * Parameter type-checking strategy
     * type must be nullable boolean, nullable boolean.
     */
    public static final OperandsTypeChecking typeNullableBoolBool =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.booleanNullableTypes, SqlTypeName.booleanNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * type must be numeric.
     */
    public static final OperandsTypeChecking typeNumeric =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.numericTypes });

    /**
     * Parameter type-checking strategy
     * type must be a literal, but NOT a NULL literal
     */
    public static final OperandsTypeChecking typeLiteral =
        new OperandsTypeChecking() {
            public boolean check(
                SqlCall call,
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlNode node,
                int ruleOrdinal,
                boolean throwOnFailure) {

                Util.discard(ruleOrdinal);

                if (SqlUtil.isNullLiteral(node, true)) {
                    if (throwOnFailure) {
                        throw EigenbaseResource.instance()
                            .newArgumentMustNotBeNull(call.operator.name);
                    }
                    return false;
                }
                if (!SqlUtil.isLiteral(node)) {
                    if (throwOnFailure) {
                        throw EigenbaseResource.instance()
                            .newArgumentMustBeLiteral(call.operator.name);
                    }
                    return false;
                }

                return true;
            }

            public boolean check(
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlCall call,
                boolean throwOnFailure) {

                Util.pre(null != call, "null != call");
                return check(call, validator, scope,
                    call.operands[0], 0, throwOnFailure);
            }

            public int getArgCount() {
                return 1;
            }

            public String getAllowedSignatures(SqlOperator op) {
                return "<LITERAL>";
            }
        };

    /**
     * Parameter type-checking strategy
     * type must be a positive integer literal. Null literal not allowed
     */
    public static final OperandsTypeChecking typePositiveIntegerLiteral =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.intTypes
        }) {
            public boolean check(
                SqlCall call,
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlNode node,
                int ruleOrdinal, boolean throwOnFailure)
            {
                if (!typeLiteral.check(call, validator, scope, node,
                                        ruleOrdinal, throwOnFailure)) {
                    return false;
                }

                if (!super.check(call, validator, scope, node,
                                  ruleOrdinal, throwOnFailure)) {
                    return false;
                }

                final SqlLiteral arg = ((SqlLiteral) node);
                final int value = arg.intValue();
                if (value < 0) {
                    if (throwOnFailure) {
                        throw EigenbaseResource.instance()
                            .newArgumentMustBePositiveInteger(call.operator.name);
                    }
                    return false;
                }
                return true;
            }
        };

    /**
     * Parameter type-checking strategy
     * type must be numeric, numeric.
     */
    public static final OperandsTypeChecking typeNumericNumeric =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.numericTypes, SqlTypeName.numericTypes
        });

    /**
     * Parameter type-checking strategy
     * type must be integer.
     */
    public static final OperandsTypeChecking typeInteger =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.intTypes
        });

    /**
     * Parameter type-checking strategy
     * type must be integer, integer.
     */
    public static final OperandsTypeChecking typeIntegerInteger =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.intTypes, SqlTypeName.intTypes
        });

    /**
     * Parameter type-checking strategy
     * type must be nullable integer, nullable integer. (exact types)
     */
    public static final OperandsTypeChecking typeNullableIntegerInteger =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.intNullableTypes, SqlTypeName.intNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * type must be nullable aType, nullable sameType
     */
    public static final OperandsTypeChecking typeNullableSameSame =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            { SqlTypeName.Any },
            { SqlTypeName.Any }
        }) {
            public boolean check(
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlCall call, boolean throwOnFailure)
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
                if (!(type1.isSameType(type2) || type2.isSameType(type1))) {
                    if (throwOnFailure) {
                        throw EigenbaseResource.instance()
                        .newNeedSameTypeParameter(
                            call.getParserPosition().toString());
                    }
                    return false;
                }
                return true;
            }
        };

    /**
     * Parameter type-checking strategy
     * type must be nullable aType, nullable sameType, nullable sameType
     */
    public static final OperandsTypeChecking typeNullableSameSameSame =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            { SqlTypeName.Any },
            { SqlTypeName.Any },
            { SqlTypeName.Any }
        }) {
            public boolean check(
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlCall call, boolean throwOnFailure)
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
                if (!(
                    (type1.equals(nullType) || type2.equals(nullType) ||
                    type1.isSameType(type2) || type2.isSameType(type1)) &&
                    (type1.equals(nullType) || type3.equals(nullType) ||
                    type1.isSameType(type3) || type3.isSameType(type1)) &&
                    (type2.equals(nullType) || type3.equals(nullType) ||
                    type2.isSameType(type3) || type3.isSameType(type2)))) {

                    if (throwOnFailure) {
                        throw EigenbaseResource.instance()
                            .newNeedSameTypeParameter(
                                call.getParserPosition().toString());
                    }
                    return false;
                }
                return true;
            }
        };

    /**
     * Parameter type-checking strategy
     * type must be nullable numeric, nullable numeric.
     */
    public static final OperandsTypeChecking typeNullableNumericNumeric =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.numericNullableTypes, SqlTypeName.numericNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * type must be nullable numeric, nullable numeric, nullabl numeric.
     */
    public static final OperandsTypeChecking typeNullableNumericNumericNumeric =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.numericNullableTypes, SqlTypeName.numericNullableTypes,
            SqlTypeName.numericNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * type must be nullable binary, nullable binary.
     */
    public static final OperandsTypeChecking typeNullableBinariesBinaries =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.binaryNullableTypes, SqlTypeName.binaryNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * type must be nullable binary, nullable binary, nullable binary.
     */
    public static final OperandsTypeChecking typeNullableBinariesBinariesBinaries =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.binaryNullableTypes,
            SqlTypeName.binaryNullableTypes,
            SqlTypeName.binaryNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * type must be null | charstring | bitstring | hexstring
     */
    public static final OperandsTypeChecking typeNullableString =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.stringNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * types must be null | charstring | bitstring | hexstring
     */
    public static final OperandsTypeChecking typeNullableStringString =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.stringNullableTypes, SqlTypeName.stringNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * type must be a varchar literal.
     */
    public static final OperandsTypeChecking typeVarcharLiteral =
        new CompositeAndOperandsTypeChecking(
        new OperandsTypeChecking[] {
        new SimpleOperandsTypeChecking(
            new SqlTypeName[][]{SqlTypeName.charTypes})
        , typeLiteral
        });

    /**
     * Parameter type-checking strategy
     * type must be a nullable varchar literal.
     */
    public static final OperandsTypeChecking typeNullableVarcharLiteral =
        new CompositeAndOperandsTypeChecking(
        new OperandsTypeChecking[] {
        new SimpleOperandsTypeChecking(
            new SqlTypeName[][]{SqlTypeName.charNullableTypes})
        , typeLiteral
        });

    /**
     * Parameter type-checking strategy
     * type must be nullable varchar, NOT nullable varchar literal.
     * the expression <code>CAST(NULL AS TYPE)</code> is considered a NULL literal.
     */
    public static final OperandsTypeChecking
        typeNullableVarcharNotNullableVarcharLiteral =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.charNullableTypes, SqlTypeName.charTypes
        }) {
            public boolean check(
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlCall call, boolean throwOnFailure)
            {
                //checking if char types
                if (!super.check(validator, scope, call, throwOnFailure)) {
                    return false;
                }

                // check that the 2nd argument is a literal
                return typeLiteral.check(call,validator, scope,
                    call.operands[1],0, throwOnFailure);
            }
        };

    /**
     * Parameter type-checking strategy
     * types must be null | charstring | bitstring | hexstring
     * AND types must be identical to eachother
     */
    public static final OperandsTypeChecking typeNullableStringStringOfSameType =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.stringNullableTypes, SqlTypeName.stringNullableTypes
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

            public boolean check(
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlCall call, boolean throwOnFailure)
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
                        if (throwOnFailure) {
                            //parser postition retrieved in
                            //newValidationSignatureError()
                            throw call.newValidationSignatureError(
                                validator, scope);
                        }
                        return false;
                    }
                }
                return super.check(validator, scope, call, throwOnFailure);
            }
        };

    /**
     * Parameter type-checking strategy
     * types must be null | charstring | bitstring | hexstring
     * AND types must be identical to eachother
     */
    public static final OperandsTypeChecking
        typeNullableStringStringStringOfSameType =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.stringNullableTypes,
            SqlTypeName.stringNullableTypes,
            SqlTypeName.stringNullableTypes
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

            public boolean check(
                SqlValidator validator,
                SqlValidator.Scope scope,
                SqlCall call, boolean throwOnFailure)
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
                        if (throwOnFailure) {
                            throw call.newValidationSignatureError(validator, scope);
                        }
                        return false;
                    }
                }
                return super.check(validator, scope, call, throwOnFailure);
            }
        };

    /**
     * Parameter type-checking strategy
     * types must be null | charstring | bitstring | hexstring
     */
    public static final OperandsTypeChecking typeNullableStringStringString =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.stringNullableTypes, SqlTypeName.stringNullableTypes,
            SqlTypeName.stringNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * types must be
     * null | charstring | bitstring | hexstring
     * null | int
     */
    public static final OperandsTypeChecking typeNullableStringInt =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.stringNullableTypes, SqlTypeName.intNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * types must be
     * null | charstring | bitstring | hexstring
     * null | int
     * null | int
     */
    public static final OperandsTypeChecking typeNullableStringIntInt =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.stringTypes, SqlTypeName.intNullableTypes,
            SqlTypeName.intNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * 2 first types must be null | charstring | bitstring | hexstring
     * 3 type must be integer
     */
    public static final OperandsTypeChecking typeNullableStringStringNotNullableInt =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.stringTypes, SqlTypeName.stringTypes,
            SqlTypeName.intTypes
        });

    /**
     * Parameter type-checking strategy
     * 2 first types must be null | charstring | bitstring | hexstring
     * 3&4 type must be integer
     */
    public static final OperandsTypeChecking
        typeNullableStringStringNotNullableIntInt =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.stringTypes, SqlTypeName.stringTypes,
            SqlTypeName.intTypes,
            SqlTypeName.intTypes
        });

    /**
     * Parameter type-checking strategy
     * type must be nullable numeric
     */
    public static final OperandsTypeChecking typeNullableNumeric =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.numericNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * types must be varchar, int, int
     */
    public static final OperandsTypeChecking typeVarcharIntInt =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.charTypes, SqlTypeName.intTypes, SqlTypeName.intTypes
        });

    /**
     * Parameter type-checking strategy
     * types must be [nullable] varchar, [nullable] int, [nullable] int
     */
    public static final OperandsTypeChecking typeNullableVarcharIntInt =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.charNullableTypes, SqlTypeName.intNullableTypes,
            SqlTypeName.intNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * types must be not nullable varchar, not nullable int
     */
    public static final OperandsTypeChecking typeVarcharInt =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.charTypes, SqlTypeName.intTypes
        });

    /**
     * Parameter type-checking strategy
     * types must be [nullable] varchar, [nullable] varchar,
     */
    public static final OperandsTypeChecking typeNullableVarcharVarchar =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.charNullableTypes, SqlTypeName.charNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * types must be [nullable] varchar, [nullable] varchar, [nullable] varchar
     */
    public static final OperandsTypeChecking typeNullableVarcharVarcharVarchar =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.charNullableTypes, SqlTypeName.charNullableTypes,
            SqlTypeName.charNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * types must be nullable boolean
     */
    public static final OperandsTypeChecking typeNullableBool =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.booleanNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * types can be any,any
     */
    public static final OperandsTypeChecking typeAnyAny =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            { SqlTypeName.Any },
            { SqlTypeName.Any }
        });

    /**
     * Parameter type-checking strategy
     * types can be any
     */
    public static final OperandsTypeChecking typeAny =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            { SqlTypeName.Any }
        });

    /**
     * Parameter type-checking strategy
     * types must be varchar,varchar
     */
    public static final OperandsTypeChecking typeVarcharVarchar =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.charTypes, SqlTypeName.charTypes
        });

    /**
     * Parameter type-checking strategy
     * types must be nullable varchar
     */
    public static final OperandsTypeChecking typeNullableVarchar =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.charNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * type must be
     * positive integer literal
     * OR varchar literal
     */
    public static final OperandsTypeChecking
        typePositiveIntegerLiteral_or_VarcharLiteral =
        new CompositeOrOperandsTypeChecking(new OperandsTypeChecking [] {
            typePositiveIntegerLiteral, typeVarcharLiteral
        });

    /**
     * Parameter type-checking strategy
     * type must be
     * nullable aType, nullable aType
     * and must be comparable to eachother
     */
    public static final OperandsTypeChecking typeNullableComparable =
        new CompositeOrOperandsTypeChecking(new OperandsTypeChecking [] {
            typeNullableSameSame, typeNullableNumericNumeric,
            typeNullableBinariesBinaries, typeNullableVarcharVarchar
        });

    /**
     * Parameter type-checking strategy
     * type must be
     * nullable string, nullable string, nulalble string
     * OR nullable string, nullable numeric, nullable numeric.
     */
    public static final OperandsTypeChecking
        typeNullabeStringStringString_or_NullableStringIntInt =
        new CompositeOrOperandsTypeChecking(new OperandsTypeChecking [] {
            typeNullableStringStringString, typeNullableStringIntInt
        });

    /**
     * Parameter type-checking strategy
     * type must be
     * nullable String
     * OR nullable numeric
     */
    public static final OperandsTypeChecking typeNullableString_or_NullableNumeric =
        new CompositeOrOperandsTypeChecking(new OperandsTypeChecking [] {
            typeNullableString, typeNullableNumeric
        });

    /**
     * Parameter type-checking strategy
     * type must a nullable time interval
     */
    public static final OperandsTypeChecking typeNullableInterval =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.timeIntervalNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * type must a nullable time interval, nullable time interval
     */
    public static final OperandsTypeChecking typeNullableIntervalInterval =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.timeIntervalNullableTypes,
            SqlTypeName.timeIntervalNullableTypes
        });

}

// End OperandsTypeChecking.java