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
     * Calls {@link #check(SqlCall, SqlValidator,org.eigenbase.sql.SqlValidator.Scope,SqlNode,int)}
     * with {@param node}
     * @param node one of the operands of {@param call}
     * @param operandOrdinal
     * @pre call != null
     */
    public void checkThrows(
        SqlValidator validator,
        SqlValidator.Scope scope,
        SqlCall call,
        SqlNode node,
        int operandOrdinal)
    {
        Util.pre(null != call, "null != call");
        if (!check(call, validator, scope, node, operandOrdinal)) {
            throw call.newValidationSignatureError(validator, scope);
        }
    }

    /**
     * Checks if a node is of correct type
     * @param call
     * @param validator
     * @param scope
     * @param node
     * @param operandOrdinal
     *
     * Note that <code>operandOrdinal</code> is <i>not</i> an index in any
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
        int operandOrdinal);

    public abstract void check(
        SqlValidator validator,
        SqlValidator.Scope scope,
        SqlCall call);

    public boolean checkNoThrowing(
        SqlCall call,
        SqlValidator validator,
        SqlValidator.Scope scope)
    {
        assert (getArgCount() == call.operands.length);

        for (int i = 0; i < call.operands.length; i++) {
            SqlNode operand = call.operands[i];
            if (!check(call, validator, scope, operand, i)) {
                return false;
            }
        }
        return true;
    }

    public abstract int getArgCount();

    /**
         * Returns a string describing the expected argument types of a call, e.g.
         * "SUBSTR(VARCHAR, INTEGER, INTEGER)".
         */
    public abstract String getAllowedSignatures(SqlOperator op);


    public static class SimpleOperandsTypeChecking extends OperandsTypeChecking {
        protected SqlTypeName[][] types;

        public SimpleOperandsTypeChecking()
        { //empty constructor
        }

        public SimpleOperandsTypeChecking(SqlTypeName[][] types)
        {
            Util.pre(null != types, "null!=types");
            Util.pre(types.length > 0, "types.length>0");

            //only Null types specified? Prohibit! need more than null
            for (int i = 0; i < types.length; i++) {
                Util.pre(types[i].length > 0, "Need to define a type");
                boolean foundOne = false;
                for (int j = 0; j < types[i].length; j++) {
                    SqlTypeName sqlType = types[i][j];
                    if (!sqlType.equals(SqlTypeName.Null)) {
                        foundOne = true;
                        break;
                    }
                }

                if (!foundOne) {
                    Util.pre(false, "Must have at least one non-null type");
                }
            }

            this.types = types;
        }

        public boolean check(
            SqlCall call,
            SqlValidator validator,
            SqlValidator.Scope scope,
            SqlNode node,
            int operandOrdinal)
        {
            RelDataType anyType = validator.anyType;
            RelDataType actualType = null;

            //for each operand, iterater over its allowed types...
            for (int j = 0; j < types[operandOrdinal].length; j++) {
                SqlTypeName typeName = types[operandOrdinal][j];
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
            return false;
        }

        public void check(
            SqlValidator validator,
            SqlValidator.Scope scope,
            SqlCall call)
        {
            if (!checkNoThrowing(call, validator, scope)) {
                throw call.newValidationSignatureError(validator, scope);
            }
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
     * For example, giving an operand the signature of both a string and a numeric
     * could be done by:
     * <blockquote><pre><code>
     *
     * CompositeOperandsTypeChecking newCompositeRule =
     *  new SqlOperator.CompositeOperandsTypeChecking(
     *    new SqlOperator.OperandsTypeChecking[]{stringRule, numericRule});
     *
     * </code></pre></blockquote>
     */
    public static class CompositeOperandsTypeChecking
        extends OperandsTypeChecking
    {
        private OperandsTypeChecking[] allowedRules;

        public CompositeOperandsTypeChecking(
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
            int operandOrdinal)
        {
            Util.pre(allowedRules.length >= 1, "allowedRules.length>=1");
            for (int i = 0; i < allowedRules.length; i++) {
                OperandsTypeChecking rule = allowedRules[i];
                if (rule.check(call, validator, scope, node, operandOrdinal)) {
                    return true;
                }
            }
            return false;
        }

        public void check(
            SqlValidator validator,
            SqlValidator.Scope scope,
            SqlCall call)
        {
            int typeErrorCount = 0;

            for (int i = 0; i < allowedRules.length; i++) {
                OperandsTypeChecking rule = allowedRules[i];

                if (!rule.checkNoThrowing(call, validator, scope)) {
                    typeErrorCount++;
                }
            }

            if (typeErrorCount == allowedRules.length) {
                throw call.newValidationSignatureError(validator, scope);
            }
        }
    }

    //~ STRATEGIES  ------------------------------------------------------

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
        new SimpleOperandsTypeChecking(new SqlTypeName [][] { SqlTypeName.numericTypes });
    /**
     * Parameter type-checking strategy
     * type must be a numeric literal.
     */
    public static final OperandsTypeChecking typeNumericLiteral =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] { SqlTypeName.numericTypes }) {
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
     * type must be a positive integer literal. Null literal not allowed
     */
    public static final OperandsTypeChecking typePositiveIntegerLiteral =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
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
    public static final OperandsTypeChecking typeNumericNumeric =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.numericTypes, SqlTypeName.numericTypes
        });
    /**
     * Parameter type-checking strategy
     * type must be integer, integer. (exact types)
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
    public static final OperandsTypeChecking typeNullableSameSameSame =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
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
            SqlTypeName.binaryNullableTypes, SqlTypeName.binaryNullableTypes, SqlTypeName.binaryNullableTypes
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
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {SqlTypeName.charTypes}) {
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
    public static final OperandsTypeChecking typeNullableVarcharLiteral =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.charNullableTypes
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
    public static final OperandsTypeChecking typeNullableVarcharNotNullableVarcharLiteral =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.charNullableTypes, SqlTypeName.charTypes
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
    public static final OperandsTypeChecking typeNullableStringStringStringOfSameType =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.stringNullableTypes, SqlTypeName.stringNullableTypes, SqlTypeName.stringNullableTypes
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
    public static final OperandsTypeChecking typeNullableStringStringString =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.stringNullableTypes, SqlTypeName.stringNullableTypes, SqlTypeName.stringNullableTypes
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
            SqlTypeName.stringTypes, SqlTypeName.intNullableTypes, SqlTypeName.intNullableTypes
        });
    /**
     * Parameter type-checking strategy
     * 2 first types must be null | charstring | bitstring | hexstring
     * 3 type must be integer
     */
    public static final OperandsTypeChecking typeNullableStringStringNotNullableInt =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.stringTypes, SqlTypeName.stringTypes, SqlTypeName.intTypes
        });
    /**
     * Parameter type-checking strategy
     * 2 first types must be null | charstring | bitstring | hexstring
     * 3&4 type must be integer
     */
    public static final OperandsTypeChecking typeNullableStringStringNotNullableIntInt =
        new SimpleOperandsTypeChecking(new SqlTypeName [][] {
            SqlTypeName.stringTypes, SqlTypeName.stringTypes, SqlTypeName.intTypes, SqlTypeName.intTypes
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
            SqlTypeName.charNullableTypes, SqlTypeName.intNullableTypes, SqlTypeName.intNullableTypes
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
            SqlTypeName.charNullableTypes, SqlTypeName.charNullableTypes, SqlTypeName.charNullableTypes
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
    public static final OperandsTypeChecking typePositiveIntegerLiteral_or_VarcharLiteral =
        new CompositeOperandsTypeChecking(new OperandsTypeChecking [] {
            typePositiveIntegerLiteral, typeVarcharLiteral
        });
    /**
     * Parameter type-checking strategy
     * type must be
     * nullable aType, nullable aType
     * and must be comparable to eachother
     */
    public static final OperandsTypeChecking typeNullableComparable =
        new CompositeOperandsTypeChecking(new OperandsTypeChecking [] {
            typeNullableSameSame, typeNullableNumericNumeric,
            typeNullableBinariesBinaries, typeNullableVarcharVarchar
        });
    /**
     * Parameter type-checking strategy
     * type must be
     * nullable string, nullable string, nulalble string
     * OR nullable string, nullable numeric, nullable numeric.
     */
    public static final OperandsTypeChecking typeNullabeStringStringString_or_NullableStringIntInt =
        new CompositeOperandsTypeChecking(new OperandsTypeChecking [] {
            typeNullableStringStringString, typeNullableStringIntInt
        });
    /**
     * Parameter type-checking strategy
     * type must be
     * nullable String
     * OR nullable numeric
     */
    public static final OperandsTypeChecking typeNullableString_or_NullableNumeric =
        new CompositeOperandsTypeChecking(new OperandsTypeChecking [] {
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
            SqlTypeName.timeIntervalNullableTypes, SqlTypeName.timeIntervalNullableTypes
        });

}

// End OperandsTypeChecking.java