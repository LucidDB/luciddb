/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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

package org.eigenbase.sql.fun;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.SqlValidatorScope;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.util.*;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.test.SqlOperatorTests;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;

import java.util.ArrayList;

/**
 * Implementation of {@link org.eigenbase.sql.SqlOperatorTable} containing the
 * standard operators and functions.
 *
 * @author jhyde
 * @since May 28, 2004
 * @version $Id$
 **/
public class SqlStdOperatorTable extends ReflectiveSqlOperatorTable
{
    //~ Instance fields -------------------------------------------
    /**
     * The standard operator table.
     */
    private static SqlStdOperatorTable instance;


    //~ INNER CLASSES ------------------------------------------
    /**
     * Returns the standard operator table, creating it if necessary.
     */
    public static synchronized SqlStdOperatorTable instance()
    {
        if (instance == null) {
            // Creates and initializes the standard operator table.
            // Uses two-phase construction, because we can't intialize the
            // table until the constructor of the sub-class has completed.
            instance = new SqlStdOperatorTable();
            instance.init();
        }
        return instance;
    }

    /**
     * Base class for time functions such as "LOCALTIME", "LOCALTIME(n)".
     */
    private abstract static class SqlAbstractTimeFunction extends SqlFunction {
        private final SqlTypeName typeName;

        public SqlAbstractTimeFunction(String name, SqlTypeName typeName) {
            super(name, SqlKind.Function, null, null, null,
                    SqlFunctionCategory.TimeDate);
            this.typeName = typeName;
        }
        // no argTypeInference, so must override these methods.
        // Probably need a niladic version of that.
        public OperandsCountDescriptor getOperandsCountDescriptor()
        {
            return new OperandsCountDescriptor(0, 1);
        }

        public SqlSyntax getSyntax()
        {
            return SqlSyntax.FunctionId;
        }

        protected boolean checkArgTypes(
            SqlCall call,
            SqlValidator validator,
            SqlValidatorScope scope, boolean throwOnFailure)
        {
            if (null != operandsCheckingRule) {
                return super.checkArgTypes(
                    call, validator, scope, throwOnFailure);
            } else if (1==call.operands.length) {
                if (!OperandsTypeChecking.typePositiveIntegerLiteral.check(
                        validator,  scope, call, false)) {
                    if (throwOnFailure) {
                        throw EigenbaseResource.instance().
                            newArgumentMustBePositiveInteger(
                                call.operator.name);
                    }
                    return false;
                }
            }
            return true;
        }

        protected RelDataType getType(
            SqlValidator validator,
            SqlValidatorScope scope,
            RelDataTypeFactory typeFactory,
            CallOperands callOperands)
        {
            // REVIEW jvs 20-Feb-2005:  SqlTypeName says Time and Timestamp
            // don't take precision, but they should (according to the
            // standard). Also, need to take care of time zones.
            int precision = 0;
            if (callOperands.size() == 1) {
                RelDataType type = callOperands.getType(0);
                if (SqlTypeUtil.isNumeric(type)) {
                    precision = callOperands.getIntLiteral(0);
                }
            }
            assert(precision >= 0);
            return typeFactory.createSqlType(typeName, precision);
        }
        // All of the time functions are monotonic.
        public boolean isMonotonic(SqlCall call, SqlValidatorScope scope) {
            return true;
        }

    }

    /**
     * Abstract base class for functions such as "USER", "CURRENT_ROLE",
     * and "CURRENT_PATH".
     */
    private static abstract class SqlStringContextVariable extends SqlFunction {
        public SqlStringContextVariable(String name) {
            super(
                name, SqlKind.Function, ReturnTypeInferenceImpl.useVarchar2000,
                null, OperandsTypeChecking.typeEmpty,
                SqlFunctionCategory.System);
        }

        public OperandsCountDescriptor getOperandsCountDescriptor()
        {
            return OperandsCountDescriptor.niladicCountDescriptor;
        }

        public SqlSyntax getSyntax() {
            return SqlSyntax.FunctionId;
        }

        // All of the string constants are monotonic.
        public boolean isMonotonic(SqlCall call, SqlValidatorScope scope) {
            return true;
        }
    }

    //~ OPERATORS AND FUNCTIONS -----------------------------------

    //-------------------------------------------------------------
    //                   SET OPERATORS
    //-------------------------------------------------------------
    // The set operators can be compared to the arthimetic operators
    // UNION -> +
    // EXCEPT -> -
    // INTERSECT -> *
    // which explains the different precedence values
    public final SqlSetOperator unionOperator =
        new SqlSetOperator("UNION", SqlKind.Union, 7, false);
    public final SqlSetOperator unionAllOperator =
        new SqlSetOperator("UNION ALL", SqlKind.Union, 7, true);
    public final SqlSetOperator exceptOperator =
        new SqlSetOperator("EXCEPT", SqlKind.Except, 7, false);
    public final SqlSetOperator exceptAllOperator =
        new SqlSetOperator("EXCEPT ALL", SqlKind.Except, 7, true);
    public final SqlSetOperator intersectOperator =
        new SqlSetOperator("INTERSECT", SqlKind.Intersect, 9, false);
    public final SqlSetOperator intersectAllOperator =
        new SqlSetOperator("INTERSECT ALL", SqlKind.Intersect, 9, true);

    /** The "MULTISET UNION" operator. */
    public final SqlMultisetSetOperator multisetUnionOperator =
        new SqlMultisetSetOperator("MULTISET UNION", 7, false);
    /** The "MULTISET UNION ALL" operator. */
    public final SqlMultisetSetOperator multisetUnionAllOperator =
        new SqlMultisetSetOperator("MULTISET UNION ALL", 7, true);
    /** The "MULTISET EXCEPT" operator. */
    public final SqlMultisetSetOperator multisetExceptOperator =
        new SqlMultisetSetOperator("MULTISET EXCEPT", 7, false);
    /** The "MULTISET EXCEPT ALL" operator. */
    public final SqlMultisetSetOperator multisetExceptAllOperator =
        new SqlMultisetSetOperator("MULTISET EXCEPT ALL", 7, true);
    /** The "MULTISET INTERSECT" operator. */
    public final SqlMultisetSetOperator multisetIntersectOperator =
        new SqlMultisetSetOperator("MULTISET INTERSECT", 9, false);
    /** The "MULTISET INTERSECT ALL" operator. */
    public final SqlMultisetSetOperator multisetIntersectAllOperator =
        new SqlMultisetSetOperator("MULTISET INTERSECT ALL", 9, true);

    /**
     * An operator which performs set operations on multisets, such as
     * "MULTISET UNION ALL". Not to be confused with {@link SqlMultisetOperator}.
     *
     * <p>todo: Represent the ALL keyword to MULTISET UNION ALL etc. as a
     * hidden operand. Then we can obsolete this class.
     */
    public static class SqlMultisetSetOperator extends SqlBinaryOperator {
        private final boolean all;

        public SqlMultisetSetOperator(String name, int prec, boolean all)
        {
            super(name, SqlKind.Other, prec, true,
                ReturnTypeInferenceImpl.useNullableMultiset,
                UnknownParamInference.useFirstKnown,
                OperandsTypeChecking.typeNullableMultisetMultiset);
            this.all = all;
        }
    }

    //-------------------------------------------------------------
    //                   BINARY OPERATORS
    //-------------------------------------------------------------

    /**
     * Logical <code>AND</code> operator.
     */
    public final SqlBinaryOperator andOperator =
        new SqlBinaryOperator("AND", SqlKind.And, 14, true,
            ReturnTypeInferenceImpl.useNullableBoolean,
            UnknownParamInference.useBoolean,
            OperandsTypeChecking.typeNullableBoolBool) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testAndOperator(tester);
            }
        };

    /**
     * <code>AS</code> operator associates an expression in the SELECT
     * clause with an alias.
     */
    public final SqlBinaryOperator asOperator =
        new SqlBinaryOperator("AS", SqlKind.As, 10, true,
            ReturnTypeInferenceImpl.useFirstArgType,
            UnknownParamInference.useReturnType,
            OperandsTypeChecking.typeAnyAny)
        {
            public void validateCall(
                SqlCall call,
                SqlValidator validator,
                SqlValidatorScope scope,
                SqlValidatorScope operandScope)
            {
                // The base method validates all operands. We override because
                // we don't want to validate the identifier.
                final SqlNode [] operands = call.operands;
                assert operands.length == 2;
                assert operands[1] instanceof SqlIdentifier;
                operands[0].validate(validator, scope);
                SqlIdentifier id = (SqlIdentifier) operands[1];
                if (!id.isSimple()) {
                    throw validator.newValidationError(id,
                        EigenbaseResource.instance()
                        .newAliasMustBeSimpleIdentifier());
                }
            }

            public void acceptCall(SqlVisitor visitor, SqlCall call) {
                // Do not visit operands[1] -- it is not an expression.
                visitor.visitChild(call, 0, call.operands[0]);
            }
        };

    /**
     * String concatenation operator, '<code>||</code>'.
     */
    public final SqlBinaryOperator concatOperator =
        new SqlBinaryOperator("||", SqlKind.Other, 30, true,
            ReturnTypeInferenceImpl.useNullableVaryingDyadicStringSumPrecision, null,
            OperandsTypeChecking.typeNullableStringStringOfSameType) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testConcatOperator(tester);
            }
        };

    /**
     * Arithmetic division operator, '<code>/</code>'.
     */
    public final SqlBinaryOperator divideOperator =
        new SqlBinaryOperator("/", SqlKind.Divide, 30, true,
            ReturnTypeInferenceImpl.useNullableMutliplyDivison,
            UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeDivisionOperator) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testDivideOperator(tester);
            }
        };

    /**
     * Dot operator, '<code>.</code>', used for referencing fields of records.
     */
    public final SqlBinaryOperator dotOperator =
        new SqlBinaryOperator(".", SqlKind.Dot, 40, true, null, null,
            OperandsTypeChecking.typeAnyAny);

    /**
     * Logical equals operator, '<code>=</code>'.
     */
    public final SqlBinaryOperator equalsOperator =
        new SqlBinaryOperator("=", SqlKind.Equals, 15, true,
            ReturnTypeInferenceImpl.useNullableBoolean, UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeNullableComparableUnordered) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testEqualsOperator(tester);
            }
        };

    /**
     * Logical greater-than operator, '<code>&gt;</code>'.
     */
    public final SqlBinaryOperator greaterThanOperator =
        new SqlBinaryOperator(">", SqlKind.GreaterThan, 15, true,
            ReturnTypeInferenceImpl.useNullableBoolean, UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeNullableComparableOrdered) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testGreaterThanOperator(tester);
            }
        };

    /**
     * <code>IS DISTINCT FROM</code> operator.
     */
    public final SqlBinaryOperator isDistinctFromOperator =
        new SqlBinaryOperator("IS DISTINCT FROM", SqlKind.Other, 15, true,
            ReturnTypeInferenceImpl.useNullableBoolean, UnknownParamInference.useFirstKnown, OperandsTypeChecking.typeAnyAny) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testIsDistinctFromOperator(tester);
            }
        };

    /**
     * Logical greater-than-or-equal operator, '<code>&gt;=</code>'.
     */
    public final SqlBinaryOperator greaterThanOrEqualOperator =
        new SqlBinaryOperator(">=", SqlKind.GreaterThanOrEqual, 15, true,
            ReturnTypeInferenceImpl.useNullableBoolean, UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeNullableComparableOrdered) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testGreaterThanOrEqualOperator(tester);
            }
        };

    /**
     * <code>IN</code> operator tests for a value's membership in a subquery
     * or a list of values.
     */
    public final SqlBinaryOperator inOperator =
        new SqlBinaryOperator("IN", SqlKind.In, 15, true, ReturnTypeInferenceImpl.useNullableBoolean,
            UnknownParamInference.useFirstKnown, null) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testInOperator(tester);
            }
        };



    /**
     * Logical less-than operator, '<code>&lt;</code>'.
     */
    public final SqlBinaryOperator lessThanOperator =
        new SqlBinaryOperator("<", SqlKind.LessThan, 15, true,
            ReturnTypeInferenceImpl.useNullableBoolean, UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeNullableComparableOrdered) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testLessThanOperator(tester);
            }
        };

    /**
     * Logical less-than-or-equal operator, '<code>&lt;=</code>'.
     */
    public final SqlBinaryOperator lessThanOrEqualOperator =
        new SqlBinaryOperator("<=", SqlKind.LessThanOrEqual, 15, true,
            ReturnTypeInferenceImpl.useNullableBoolean, UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeNullableComparableOrdered) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testLessThanOrEqualOperator(tester);
            }
        };

    /**
     * Arithmetic minus operator, '<code>-</code>'.
     */
    public final SqlBinaryOperator minusOperator =
        new SqlBinaryOperator("-", SqlKind.Minus, 20, true,
            ReturnTypeInferenceImpl.useNullableBiggest,
            UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeMinusOperator) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testMinusOperator(tester);
            }

            public boolean isMonotonic(SqlCall call, SqlValidatorScope scope)
            {
                SqlValidator val = scope.getValidator();
                // Check for (c - m) where c is a constant
                if (val.isConstant(call.operands[0])) {
                    SqlNode node = (SqlNode)call.operands[1];
                    return scope.isMonotonic(node);
                }
                // Check for (m - c) where c is a constant
                if (val.isConstant(call.operands[1])) {
                    SqlNode node = (SqlNode)call.operands[0];
                    return scope.isMonotonic(node);
                }

                return super.isMonotonic(call, scope);
            }
        };

    /**
     * Arithmetic multiplication operator, '<code>*</code>'.
     */
    public final SqlBinaryOperator multiplyOperator =
        new SqlBinaryOperator("*", SqlKind.Times, 30, true,
            ReturnTypeInferenceImpl.useNullableMutliplyDivison,
            UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeMultiplyOperator) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testMultiplyOperator(tester);
            }

            public boolean isMonotonic(SqlCall call, SqlValidatorScope scope)
            {
                SqlValidator val = scope.getValidator();
                // First check for (m * c) where c is a constant
                if (val.isConstant(call.operands[1])) {
                    SqlNode node = (SqlNode)call.operands[0];
                    return scope.isMonotonic(node);
                }
                // Check the converse (c * m)
                if (val.isConstant(call.operands[0])) {
                    SqlNode node = (SqlNode)call.operands[1];
                    return scope.isMonotonic(node);
                }

                return super.isMonotonic(call, scope);
            }
        };

    /**
     * Logical not-equals operator, '<code>&lt;&gt;</code>'.
     */
    public final SqlBinaryOperator notEqualsOperator =
        new SqlBinaryOperator("<>", SqlKind.NotEquals, 15, true,
            ReturnTypeInferenceImpl.useNullableBoolean, UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeNullableComparableUnordered) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testNotEqualsOperator(tester);
            }
        };

    /**
     * Logical <code>OR</code> operator.
     */
    public final SqlBinaryOperator orOperator =
        new SqlBinaryOperator("OR", SqlKind.Or, 13, true,
            ReturnTypeInferenceImpl.useNullableBoolean,
            UnknownParamInference.useBoolean,
            OperandsTypeChecking.typeNullableBoolBool) {

            public void test(SqlTester tester)
            {
                SqlOperatorTests.testOrOperator(tester);
            }
        };

    public final SqlBinaryOperator plusOperator =
        new SqlBinaryOperator("+", SqlKind.Plus, 20, true,
            ReturnTypeInferenceImpl.useNullableBiggest,
            UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typePlusOperator) {

            public void test(SqlTester tester)
            {
                SqlOperatorTests.testPlusOperator(tester);
            }

            public boolean isMonotonic(SqlCall call, SqlValidatorScope scope)
            {
                SqlValidator val = scope.getValidator();
                // First check for (m + c) where c is a constant
                if (val.isConstant(call.operands[1])) {
                    SqlNode node = (SqlNode)call.operands[0];
                    return scope.isMonotonic(node);
                }
                // Check the converse (c + m)
                if (val.isConstant(call.operands[0])) {
                    SqlNode node = (SqlNode)call.operands[1];
                    return scope.isMonotonic(node);
                }

                return super.isMonotonic(call, scope);
            }

        };

    /**
     * Multiset Member of. Checks to see if a element belongs to a multiset.<br>
     * Example:<br>
     * <code>'green' MEMBER OF MULTISET['red','almost green','blue']</code>
     * returns <code>false</code>.
     */
    public final SqlBinaryOperator memberOfOperator =
            //TODO check if precedence is correct
            new SqlBinaryOperator("MEMBER OF", SqlKind.Other, 15, true,
                ReturnTypeInferenceImpl.useNullableBoolean,
                null, null) {
                public void test(SqlTester tester)
                {
                    SqlOperatorTests.testMemberOfOperator(tester);
                }

                protected boolean checkArgTypes(
                    SqlCall call,
                    SqlValidator validator,
                    SqlValidatorScope scope,
                    boolean throwOnFailure) {

                    if (!OperandsTypeChecking.typeNullableMultiset.check(
                        call, validator, scope,
                        call.operands[1], 0, throwOnFailure)) {
                        return false;
                    }

                    MultisetSqlType mt = (MultisetSqlType)
                        validator.deriveType(scope, call.operands[1]);

                    RelDataType t0 = validator.deriveType(scope, call.operands[0]);
                    RelDataType t1 = mt.getComponentType();

                    if (t0.getFamily() != t1.getFamily()) {
                        if (throwOnFailure) {
                            throw validator.newValidationError(call,
                                EigenbaseResource.instance().
                                newTypeNotComparableNear(
                                    t0.toString(), t1.toString()));
                        }
                        return false;
                    }
                    return true;
                }

                public SqlOperator.OperandsCountDescriptor getOperandsCountDescriptor() {
                    return new SqlOperator.OperandsCountDescriptor(2);
                }

            };

    /**
     * Submultiset. Checks to see if an multiset is a sub-set of another
     * multiset.<br>
     * Example:<br>
     * <code>
     *  MULTISET['green'] SUBMULTISET OF MULTISET['red','almost green','blue']
     * </code>
     * returns <code>false</code>.<p>
     * But
     * <code>
     *  MULTISET['blue', 'red'] SUBMULTISET OF MULTISET['red','almost green','blue']
     * </code>
     * returns <code>true</code> (<b>NB</b> multisets is order independant)
     */
    public final SqlBinaryOperator submultisetOfOperator =
            //TODO check if precedence is correct
            new SqlBinaryOperator("SUBMULTISET OF", SqlKind.Other, 15, true,
                ReturnTypeInferenceImpl.useNullableBoolean,
                null,
                OperandsTypeChecking.typeNullableMultisetMultiset) {
                public void test(SqlTester tester)
                {
                    //todo
                }
            };


    //-------------------------------------------------------------
    //                   POSTFIX OPERATORS
    //-------------------------------------------------------------
    public final SqlPostfixOperator descendingOperator =
        new SqlPostfixOperator("DESC", SqlKind.Descending, 10, null,
            UnknownParamInference.useReturnType, OperandsTypeChecking.typeAny) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testDescendingOperator(tester);
            }
        };

    public final SqlPostfixOperator isNotNullOperator =
        new SqlPostfixOperator("IS NOT NULL", SqlKind.Other, 15, ReturnTypeInferenceImpl.useBoolean,
            UnknownParamInference.useBoolean, OperandsTypeChecking.typeAny) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testIsNotNullOperator(tester);
            }
        };

    public final SqlPostfixOperator isNullOperator =
        new SqlPostfixOperator("IS NULL", SqlKind.IsNull, 15, ReturnTypeInferenceImpl.useBoolean,
            UnknownParamInference.useBoolean, OperandsTypeChecking.typeAny) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testIsNullOperator(tester);
            }
        };

    public final SqlPostfixOperator isNotTrueOperator =
        new SqlPostfixOperator("IS NOT TRUE", SqlKind.Other, 15, ReturnTypeInferenceImpl.useBoolean,
            UnknownParamInference.useBoolean, OperandsTypeChecking.typeNullableBool) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testIsNotTrueOperator(tester);
            }
        };

    public final SqlPostfixOperator isTrueOperator =
        new SqlPostfixOperator("IS TRUE", SqlKind.IsTrue, 15, ReturnTypeInferenceImpl.useBoolean,
            UnknownParamInference.useBoolean, OperandsTypeChecking.typeNullableBool) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testIsTrueOperator(tester);
            }
        };

    public final SqlPostfixOperator isNotFalseOperator =
        new SqlPostfixOperator("IS NOT FALSE", SqlKind.Other, 15, ReturnTypeInferenceImpl.useBoolean,
            UnknownParamInference.useBoolean, OperandsTypeChecking.typeNullableBool) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testIsNotFalseOperator(tester);
            }
        };

    public final SqlPostfixOperator isFalseOperator =
        new SqlPostfixOperator("IS FALSE", SqlKind.IsFalse, 15, ReturnTypeInferenceImpl.useBoolean,
            UnknownParamInference.useBoolean, OperandsTypeChecking.typeNullableBool) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testIsFalseOperator(tester);
            }
        };

    public final SqlPostfixOperator isNotUnknownOperator =
        new SqlPostfixOperator("IS NOT UNKNOWN", SqlKind.Other, 15,
            ReturnTypeInferenceImpl.useBoolean, UnknownParamInference.useBoolean, OperandsTypeChecking.typeNullableBool) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testIsNotUnknownOperator(tester);
            }
        };

    public final SqlPostfixOperator isUnknownOperator =
        new SqlPostfixOperator("IS UNKNOWN", SqlKind.IsNull, 15, ReturnTypeInferenceImpl.useBoolean,
            UnknownParamInference.useBoolean, OperandsTypeChecking.typeNullableBool) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testIsUnknownOperator(tester);
            }
        };

    public final SqlPostfixOperator isASetOperator =
        new SqlPostfixOperator("IS A SET", SqlKind.Other, 15,
            ReturnTypeInferenceImpl.useBoolean,
            null,
            OperandsTypeChecking.typeNullableMultiset) {
            public void test(SqlTester tester)
            {
                //todo
            }
        };

    //-------------------------------------------------------------
    //                   PREFIX OPERATORS
    //-------------------------------------------------------------
    public final SqlPrefixOperator existsOperator =
        new SqlPrefixOperator("EXISTS", SqlKind.Exists, 20, ReturnTypeInferenceImpl.useBoolean, null,
            OperandsTypeChecking.typeAny) {
            public void test(SqlTester tester)
            {
                testExistsOperator(tester);
            }
        };

    private static void testExistsOperator(SqlTester tester)
    {
        Util.discard(tester);
        // TODO:
    }

    public final SqlPrefixOperator notOperator =
        new SqlPrefixOperator("NOT", SqlKind.Not, 15, ReturnTypeInferenceImpl.useNullableBoolean,
            UnknownParamInference.useBoolean, OperandsTypeChecking.typeNullableBool) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testNotOperator(tester);
            }
        };

    public final SqlPrefixOperator prefixMinusOperator =
        new SqlPrefixOperator("-", SqlKind.MinusPrefix, 20,
            ReturnTypeInferenceImpl.useFirstArgType,
            UnknownParamInference.useReturnType,
            OperandsTypeChecking.typeNullableNumericOrInterval) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testPrefixMinusOperator(tester);
            }
        };

    public final SqlPrefixOperator prefixPlusOperator =
        new SqlPrefixOperator("+", SqlKind.PlusPrefix, 20,
            ReturnTypeInferenceImpl.useFirstArgType,
            UnknownParamInference.useReturnType,
            OperandsTypeChecking.typeNullableNumericOrInterval) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testPrefixPlusOperator(tester);
            }
        };

    public final SqlPrefixOperator explicitTableOperator =
        new SqlPrefixOperator("TABLE", SqlKind.ExplicitTable, 1, null, null,
            null) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testExplicitTableOperator(tester);
            }
        };


    // ------------------------------------------------------------------------
    // AGGREGATE OPERATORS
    //
    /**
     * <code>SUM</code> aggregate function.
     */
    public final SqlAggFunction sumOperator = new SqlSumAggFunction(null);
    /**
     * <code>COUNT</code> aggregate function.
     */
    public final SqlAggFunction countOperator = new SqlCountAggFunction();
    /**
     * <code>SUM</code> aggregate function.
     */
    public final SqlAggFunction minOperator =
        new SqlMinMaxAggFunction(new RelDataType[0], true,
            SqlMinMaxAggFunction.MINMAX_COMPARABLE);
    /**
     * <code>SUM</code> aggregate function.
     */
    public final SqlAggFunction maxOperator =
        new SqlMinMaxAggFunction(new RelDataType[0], false,
            SqlMinMaxAggFunction.MINMAX_COMPARABLE);

    //-------------------------------------------------------------
    //                   SPECIAL OPERATORS
    //-------------------------------------------------------------
    public final SqlRowOperator rowConstructor = new SqlRowOperator();

    /**
     * A special operator for the substraction of two DATETIMEs.
     * The format of DATETIME substraction is: <br>
     * <code>"(" &lt;datetime&gt; "-" &lt;datetime&gt; ")" <interval qualifier></code>
     * This special operator is special since it needs to hold the additional
     * interval qualifier specification.
     */
    public final SqlOperator minusDateOperator =
            new SqlSpecialOperator("-", SqlKind.Minus, 20, true,
                ReturnTypeInferenceImpl.useThirdArgType,
                UnknownParamInference.useFirstKnown,
                OperandsTypeChecking.typeMinusDateOperator) {
                public void test(SqlTester tester)
                {
                    SqlOperatorTests.testMinusDateOperator(tester);
                }

                public SqlSyntax getSyntax() {
                    return SqlSyntax.Special;
                }

                public void unparse(
                    SqlWriter writer,
                    SqlNode[] operands,
                    int leftPrec,
                    int rightPrec) {
                    writer.print("(");
                    operands[0].unparse(writer, leftPrec, rightPrec);
                    writer.print(" - ");
                    operands[1].unparse(writer, leftPrec, rightPrec);
                    writer.print(") ");
                    operands[2].unparse(writer, leftPrec, rightPrec);
                }
            };

    /**
     * The MULTISET Value Constructor.
     * e.g. "<code>MULTISET[1,2,3]</code>".
     */
    public final SqlMultisetOperator multisetValueConstructor =
        new SqlMultisetOperator(SqlKind.MultisetValueConstructor);

    /**
     * The MULTISET Query Constructor.
     * e.g. "<code>SELECT dname, MULTISET(SELECT * FROM
     * emp WHERE deptno = dept.deptno) FROM dept</code>".
     */
    public final SqlMultisetOperator multisetQueryConstructor =
        new SqlMultisetOperator(SqlKind.MultisetQueryConstructor);

    public final SqlSpecialOperator unnestOperator =
        new SqlSpecialOperator ("UNNEST", SqlKind.Unnest,
            100, true, null, null,
            OperandsTypeChecking.typeNullableMultisetOrRecordTypeMultiset) {

            protected RelDataType getType(
                SqlValidator validator,
                SqlValidatorScope scope,
                RelDataTypeFactory typeFactory,
                CallOperands callOperands)
            {
                RelDataType type = callOperands.getType(0);
                if (type.isStruct()) {
                    type = type.getFields()[0].getType();
                }
                MultisetSqlType t = (MultisetSqlType) type;
                return t.getComponentType();
            }

            public void unparse(
                SqlWriter writer,
                SqlNode[] operands,
                int leftPrec,
                int rightPrec) {
                writer.print(name);
                writer.print("(");
                operands[0].unparse(writer,0,0);
                writer.print(")");
            }
        };

    public final SqlSpecialOperator lateralOperator =
        new SqlSpecialOperator ("LATERAL", SqlKind.Lateral,
            100, true,
            ReturnTypeInferenceImpl.useFirstArgType,
            null,
            OperandsTypeChecking.typeAny) {

            public void unparse(
                SqlWriter writer,
                SqlNode[] operands,
                int leftPrec,
                int rightPrec) {
                writer.print(name);
                writer.print("(");
                operands[0].unparse(writer,0,0);
                writer.print(")");
            }
        };

    public final SqlOverlapsOperator overlapsOperator =
        new SqlOverlapsOperator();

    public final SqlSpecialOperator valuesOperator =
        new SqlSpecialOperator("VALUES", SqlKind.Values) {
            public void unparse(
                SqlWriter writer,
                SqlNode [] operands,
                int leftPrec,
                int rightPrec)
            {
                writer.print("VALUES ");
                for (int i = 0; i < operands.length; i++) {
                    if (i > 0) {
                        writer.print(", ");
                    }
                    SqlNode operand = operands[i];
                    operand.unparse(writer, 0, 0);
                }
            }

            public void test(SqlTester tester)
            {
                SqlOperatorTests.testValuesOperator(tester);
            }
        };

    public final SqlInternalOperator literalChainOperator =
        new SqlLiteralChainOperator();
    public final SqlBetweenOperator betweenOperator =
        new SqlBetweenOperator(
            SqlBetweenOperator.Flag.createAsymmetric(SqlParserPos.ZERO),
            false);
    public final SqlBetweenOperator symmetricBetweenOperator =
        new SqlBetweenOperator(
            SqlBetweenOperator.Flag.createSymmetric(SqlParserPos.ZERO),
            false);
    public final SqlBetweenOperator notBetweenOperator =
        new SqlBetweenOperator(
            SqlBetweenOperator.Flag.createAsymmetric(SqlParserPos.ZERO),
            true);
    public final SqlBetweenOperator symmetricNotBetweenOperator =
        new SqlBetweenOperator(
            SqlBetweenOperator.Flag.createSymmetric(SqlParserPos.ZERO),
            true);
    public final SqlSpecialOperator notLikeOperator =
        new SqlLikeOperator("NOT LIKE", SqlKind.Like, true) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testNotLikeOperator(tester);
            }
        };

    public final SqlSpecialOperator likeOperator =
        new SqlLikeOperator("LIKE", SqlKind.Like, false) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testLikeOperator(tester);
            }
        };

    public final SqlSpecialOperator notSimilarOperator =
        new SqlLikeOperator("NOT SIMILAR TO", SqlKind.Similar, true) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testNotSimilarToOperator(tester);
            }
        };

    public final SqlSpecialOperator similarOperator =
        new SqlLikeOperator("SIMILAR TO", SqlKind.Similar, false) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testSimilarToOperator(tester);

            }
        };

    /**
     * Internal operator used to represent the ESCAPE clause of a LIKE or
     * SIMILAR TO expression.
     */
    public final SqlSpecialOperator escapeOperator =
        new SqlSpecialOperator("Escape", SqlKind.Escape, 15) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testEscapeOperator(tester);
            }
        };

    /**
     * The standard SELECT operator.
     */
    public SqlSelectOperator selectOperator = new SqlSelectOperator();

    public final SqlCaseOperator caseOperator = new SqlCaseOperator();
    public final SqlJoinOperator joinOperator = new SqlJoinOperator();
    public final SqlSpecialOperator insertOperator =
        new SqlSpecialOperator("INSERT", SqlKind.Insert);
    public final SqlSpecialOperator deleteOperator =
        new SqlSpecialOperator("DELETE", SqlKind.Delete);
    public final SqlSpecialOperator updateOperator =
        new SqlSpecialOperator("UPDATE", SqlKind.Update);
    public final SqlSpecialOperator explainOperator =
        new SqlSpecialOperator("EXPLAIN", SqlKind.Explain);
    public final SqlOrderByOperator orderByOperator = new SqlOrderByOperator();

    public final SqlOperator procedureCallOperator =
        new SqlProcedureCallOperator();

    public final SqlOperator newOperator = new SqlNewOperator();

    /**
     * The WINDOW clause of a SELECT statment.
     *
     * @see #overOperator
     */
    public final SqlWindowOperator windowOperator = new SqlWindowOperator();

    /**
     * The <code>OVER</code> operator, which applies an aggregate functions to
     * a {@link SqlWindow window}.
     *
     * <p>Operands are as follows:<ol>
     * <li>name of window function ({@link org.eigenbase.sql.SqlCall})</li>
     * <li>window name ({@link org.eigenbase.sql.SqlLiteral})
     *     or window in-line specification (@link SqlWindowOperator})</li>
     * </ul>
     */
    public final SqlBinaryOperator overOperator = new SqlOverOperator();


    //-------------------------------------------------------------
    //                   FUNCTIONS
    //-------------------------------------------------------------
    /**
     * The character substring function:
     * <code>SUBSTRING(string FROM start [FOR length])</code>.
     *
     * <p>If the length parameter is a constant, the length
     * of the result is the minimum of the length of the input
     * and that length. Otherwise it is the length of the input.<p>
     */
    public final SqlFunction substringFunc = new SqlSubstringFunction();

    public final SqlFunction convertFunc =
        new SqlFunction("CONVERT", SqlKind.Function, null, null, null,
            SqlFunctionCategory.String) {
            public void unparse(
                SqlWriter writer,
                SqlNode [] operands,
                int leftPrec,
                int rightPrec)
            {
                writer.print(name);
                writer.print("(");
                operands[0].unparse(writer, leftPrec, rightPrec);
                writer.print(" USING ");
                operands[1].unparse(writer, leftPrec, rightPrec);
                writer.print(")");
            }

            protected String getSignatureTemplate(final int operandsCount)
            {
                switch (operandsCount) {
                case 2:
                    return "{0}({1} USING {2})";
                }
                assert (false);
                return null;
            }

            public void test(SqlTester tester)
            {
                SqlOperatorTests.testConvertFunc(tester);
            }
        };

    public final SqlFunction translateFunc =
        new SqlFunction("TRANSLATE", SqlKind.Function, null, null, null,
            SqlFunctionCategory.String) {
            public void unparse(
                SqlWriter writer,
                SqlNode [] operands,
                int leftPrec,
                int rightPrec)
            {
                writer.print(name);
                writer.print("(");
                operands[0].unparse(writer, leftPrec, rightPrec);
                writer.print(" USING ");
                operands[1].unparse(writer, leftPrec, rightPrec);
                writer.print(")");
            }

            protected String getSignatureTemplate(final int operandsCount)
            {
                switch (operandsCount) {
                case 2:
                    return "{0}({1} USING {2})";
                }
                assert (false);
                return null;
            }

            public void test(SqlTester tester)
            {
                SqlOperatorTests.testTranslateFunc(tester);
            }
        };

    public final SqlFunction overlayFunc =
        new SqlFunction("OVERLAY", SqlKind.Function,
            ReturnTypeInferenceImpl.useNullableDyadicStringSumPrecision, null, null,
            SqlFunctionCategory.String) {
            public OperandsCountDescriptor getOperandsCountDescriptor()
            {
                return new OperandsCountDescriptor(3, 4);
            }

            protected boolean checkArgTypes(
                SqlCall call,
                SqlValidator validator,
                SqlValidatorScope scope,
                boolean throwOnFailure)
            {
                switch (call.operands.length) {
                case 3:
                    return OperandsTypeChecking.typeNullableStringStringNotNullableInt
                        .check(validator, scope, call, throwOnFailure);
                case 4:
                    return OperandsTypeChecking.typeNullableStringStringNotNullableIntInt
                        .check(validator, scope, call, throwOnFailure);
                default:
                    throw Util.needToImplement(this);
                }
            }

            public String getAllowedSignatures(String name)
            {
                StringBuffer ret = new StringBuffer();
                for (int i = 0; i < SqlTypeName.stringTypes.length;
                        i++) {
                    if (i > 0) {
                        ret.append(NL);
                    }
                    ArrayList list = new ArrayList();
                    list.add(SqlTypeName.stringTypes[i]);
                    list.add(SqlTypeName.stringTypes[i]); //adding same twice
                    list.add(SqlTypeName.Integer);
                    ret.append(this.getAnonymousSignature(list));
                    ret.append(NL);
                    list.add(SqlTypeName.Integer);
                    ret.append(this.getAnonymousSignature(list));
                }
                return replaceAnonymous(
                    ret.toString(),
                    name);
            }

            public void unparse(
                SqlWriter writer,
                SqlNode [] operands,
                int leftPrec,
                int rightPrec)
            {
                writer.print(name);
                writer.print("(");
                operands[0].unparse(writer, leftPrec, rightPrec);
                writer.print(" PLACING ");
                operands[1].unparse(writer, leftPrec, rightPrec);
                writer.print(" FROM ");
                operands[2].unparse(writer, leftPrec, rightPrec);
                if (4 == operands.length) {
                    writer.print(" FOR ");
                    operands[3].unparse(writer, leftPrec, rightPrec);
                }
                writer.print(")");
            }

            protected String getSignatureTemplate(final int operandsCount)
            {
                switch (operandsCount) {
                case 3:
                    return "{0}({1} PLACING {2} FROM {3})";
                case 4:
                    return "{0}({1} PLACING {2} FROM {3} FOR {4})";
                }
                assert (false);
                return null;
            }

            public void test(SqlTester tester)
            {
                SqlOperatorTests.testOverlayFunc(tester);
            }
        };

    /**
     * The "TRIM" function.
     * */
    public final SqlFunction trimFunc = new SqlTrimFunction();

    /**
     * Represents Position function
     */
    public final SqlFunction positionFunc =
        new SqlFunction("POSITION", SqlKind.Function,
            ReturnTypeInferenceImpl.useNullableInteger, null,
            OperandsTypeChecking.typeNullableStringString,
            SqlFunctionCategory.Numeric) {
            public void unparse(
                SqlWriter writer,
                SqlNode [] operands,
                int leftPrec,
                int rightPrec)
            {
                writer.print(name);
                writer.print("(");
                operands[0].unparse(writer, leftPrec, rightPrec);
                writer.print(" IN ");
                operands[1].unparse(writer, leftPrec, rightPrec);
                writer.print(")");
            }

            protected String getSignatureTemplate(final int operandsCount)
            {
                switch (operandsCount) {
                case 2:
                    return "{0}({1} IN {2})";
                }
                assert (false);
                return null;
            }

            protected boolean checkArgTypes(
                SqlCall call,
                SqlValidator validator,
                SqlValidatorScope scope,
                boolean throwOnFailure)
            {
                //check that the two operands are of same type.
                RelDataType type0 =
                    validator.getValidatedNodeType(call.operands[0]);
                RelDataType type1 =
                    validator.getValidatedNodeType(call.operands[1]);
                if (!SqlTypeUtil.inSameFamily(type0, type1)) {
                    if (throwOnFailure) {
                        throw call.newValidationSignatureError(validator, scope);
                    }
                    return false;
                }

                return operandsCheckingRule.check(validator, scope, call, throwOnFailure);
            }

            public void test(SqlTester tester)
            {
                SqlOperatorTests.testPositionFunc(tester);
            }
        };

    public final SqlFunction charLengthFunc =
        new SqlFunction("CHAR_LENGTH", SqlKind.Function,
            ReturnTypeInferenceImpl.useNullableInteger, null,
            OperandsTypeChecking.typeNullableVarchar,
            SqlFunctionCategory.Numeric) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testCharLengthFunc(tester);
            }
        };

    public final SqlFunction characterLengthFunc =
        new SqlFunction("CHARACTER_LENGTH", SqlKind.Function,
            ReturnTypeInferenceImpl.useNullableInteger, null,
            OperandsTypeChecking.typeNullableVarchar,
            SqlFunctionCategory.Numeric) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testCharacterLengthFunc(tester);
            }
        };

    public final SqlFunction upperFunc =
        new SqlFunction("UPPER", SqlKind.Function,
            ReturnTypeInferenceImpl.useNullableFirstArgType, null,
            OperandsTypeChecking.typeNullableVarchar,
            SqlFunctionCategory.String) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testUpperFunc(tester);
            }
        };

    public final SqlFunction lowerFunc =
        new SqlFunction("LOWER", SqlKind.Function,
            ReturnTypeInferenceImpl.useNullableFirstArgType, null,
            OperandsTypeChecking.typeNullableVarchar,
            SqlFunctionCategory.String) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testLowerFunc(tester);
            }
        };

    public final SqlFunction initcapFunc =
        new SqlFunction("INITCAP", SqlKind.Function,
            ReturnTypeInferenceImpl.useNullableFirstArgType, null,
            OperandsTypeChecking.typeNullableVarchar,
            SqlFunctionCategory.String) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testInitcapFunc(tester);
            }
        };

    /**
     * Uses SqlOperatorTable.useDouble for its return type since we dont know
     * what the result type will be by just looking at the operand types.
     * For example POW(int, int) can return a non integer if the second operand
     * is negative.
     */
    public final SqlFunction powFunc =
        new SqlFunction("POW", SqlKind.Function,
            ReturnTypeInferenceImpl.useNullableDouble, null,
            OperandsTypeChecking.typeNumericNumeric,
            SqlFunctionCategory.Numeric) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testPowFunc(tester);
            }
        };

    public final SqlFunction modFunc =
        new SqlFunction("MOD", SqlKind.Function,
            ReturnTypeInferenceImpl.useNullableBiggest, null,
            OperandsTypeChecking.typeNullableIntegerInteger,
            SqlFunctionCategory.Numeric) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testModFunc(tester);
            }
        };

    public final SqlFunction lnFunc =
        new SqlFunction("LN", SqlKind.Function,
            ReturnTypeInferenceImpl.useNullableDouble, null,
            OperandsTypeChecking.typeNumeric, SqlFunctionCategory.Numeric) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testLnFunc(tester);
            }
        };

    public final SqlFunction logFunc =
        new SqlFunction("LOG", SqlKind.Function,
            ReturnTypeInferenceImpl.useNullableDouble, null,
            OperandsTypeChecking.typeNumeric, SqlFunctionCategory.Numeric) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testLogFunc(tester);
            }
        };

    public final SqlFunction absFunc =
        new SqlFunction("ABS", SqlKind.Function,
            ReturnTypeInferenceImpl.useFirstArgType, null,
            OperandsTypeChecking.typeNullableNumericOrInterval,
            SqlFunctionCategory.Numeric) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testAbsFunc(tester);
            }
        };

    public final SqlFunction nullIfFunc =
        new SqlFunction("NULLIF", SqlKind.Function, null, null, null,
            SqlFunctionCategory.System)
        {
            // override SqlOperator
            public SqlNode rewriteCall(SqlCall call)
            {
                SqlNode [] operands = call.getOperands();
                SqlParserPos pos = call.getParserPosition();

                if (2 != operands.length) {
                    //todo put this in the validator
                    throw EigenbaseResource.instance().newValidatorContext(
                        new Integer(pos.getLineNum()),
                        new Integer(pos.getColumnNum()),
                        EigenbaseResource.instance().newInvalidArgCount(
                            name,
                            new Integer(2)));
                }

                SqlNodeList whenList = new SqlNodeList(pos);
                SqlNodeList thenList = new SqlNodeList(pos);
                whenList.add(operands[1]);
                thenList.add(SqlLiteral.createNull(null));
                return caseOperator.createCall(operands[0], whenList,
                    thenList, operands[0], pos);
            }

            public SqlOperator.OperandsCountDescriptor getOperandsCountDescriptor()
            {
                return new OperandsCountDescriptor(2);
            }

            public void test(SqlTester tester)
            {
                SqlOperatorTests.testNullifFunc(tester);
            }
        };

    /**
     * The COALESCE builtin function.
     */
    public final SqlFunction coalesceFunc =
        new SqlFunction("COALESCE", SqlKind.Function, null, null, null,
            SqlFunctionCategory.System)
        {
            // override SqlOperator
            public SqlNode rewriteCall(SqlCall call)
            {
                SqlNode [] operands = call.getOperands();
                SqlParserPos pos = call.getParserPosition();

                SqlNodeList whenList = new SqlNodeList(pos);
                SqlNodeList thenList = new SqlNodeList(pos);

                //todo optimize when know operand is not null.

                int i;
                for (i = 0; ((i + 1) < operands.length); ++i) {
                    whenList.add(
                        isNotNullOperator.createCall(operands[i], pos));
                    thenList.add(operands[i]);
                }
                return caseOperator.createCall(null, whenList, thenList,
                    operands[i], pos);
            }

            // REVIEW jvs 1-Jan-2005:  should this be here?  It's
            // not entirely accurate,
            public SqlOperator.OperandsCountDescriptor getOperandsCountDescriptor()
            {
                return new OperandsCountDescriptor(2);
            }

            public void test(SqlTester tester)
            {
                SqlOperatorTests.testCoalesceFunc(tester);
            }
        };

    /** The <code>FLOOR</code> function. */
    public final SqlFunction floorFunc =
        new SqlFunction("FLOOR", SqlKind.Function,
            ReturnTypeInferenceImpl.useFirstArgType, null,
            OperandsTypeChecking.typeNullableNumericOrInterval,
            SqlFunctionCategory.Numeric) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testFloorFunc(tester);
            }

            public boolean isMonotonic(SqlCall call, SqlValidatorScope scope)
            {
                SqlNode node = (SqlNode)call.operands[0];
                return scope.isMonotonic(node);
            }
        };

    /** The <code>CEIL</code> function. */
    public final SqlFunction ceilFunc =
        new SqlFunction("CEIL", SqlKind.Function,
            ReturnTypeInferenceImpl.useFirstArgType, null,
            OperandsTypeChecking.typeNullableNumericOrInterval,
            SqlFunctionCategory.Numeric) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testCeilFunc(tester);
            }

            public boolean isMonotonic(SqlCall call, SqlValidatorScope scope)
            {
                SqlNode node = (SqlNode)call.operands[0];
                return scope.isMonotonic(node);
            }
        };
    /** The <code>USER</code> function. */
    public final SqlFunction userFunc = new SqlStringContextVariable("USER") {
        public void test(SqlTester tester)
        {
            SqlOperatorTests.testUserFunc(tester);
        }
    };

    /** The <code>CURRENT_USER</code> function. */
    public final SqlFunction currentUserFunc = new SqlStringContextVariable(
            "CURRENT_USER") {
        public void test(SqlTester tester)
        {
            SqlOperatorTests.testCurrentUserFunc(tester);
        }
    };

    /** The <code>SESSION_USER</code> function. */
    public final SqlFunction sessionUserFunc = new SqlStringContextVariable(
            "SESSION_USER") {
        public void test(SqlTester tester)
        {
            SqlOperatorTests.testSessionUserFunc(tester);
        }
    };

    /** The <code>SYSTEM_USER</code> function. */
    public final SqlFunction systemUserFunc = new SqlStringContextVariable(
            "SYSTEM_USER") {
        public void test(SqlTester tester)
        {
            SqlOperatorTests.testSystemUserFunc(tester);
        }
    };

    /** The <code>CURRENT_PATH</code> function. */
    public final SqlFunction currentPathFunc = new SqlStringContextVariable(
            "CURRENT_PATH") {
        public void test(SqlTester tester)
        {
            SqlOperatorTests.testCurrentPathFunc(tester);
        }
    };

    /** The <code>CURRENT_ROLE</code> function. */
    public final SqlFunction currentRoleFunc = new SqlStringContextVariable(
            "CURRENT_ROLE") {
        public void test(SqlTester tester)
        {
            SqlOperatorTests.testCurrentRoleFunc(tester);
        }
    };

    /** The <code>LOCALTIME [(<i>precision</i>)]</code> function. */
    public final SqlFunction localTimeFunc = new SqlAbstractTimeFunction(
            "LOCALTIME", SqlTypeName.Time) {
        public void test(SqlTester tester)
        {
            SqlOperatorTests.testLocalTimeFunc(tester);
        }
    };

    /** The <code>LOCALTIMESTAMP [(<i>precision</i>)]</code> function. */
    public final SqlFunction localTimestampFunc = new SqlAbstractTimeFunction(
            "LOCALTIMESTAMP", SqlTypeName.Timestamp) {
        public void test(SqlTester tester)
        {
            SqlOperatorTests.testLocalTimestampFunc(tester);
        }
    };

    /** The <code>CURRENT_TIME [(<i>precision</i>)]</code> function. */
    public final SqlFunction currentTimeFunc = new SqlAbstractTimeFunction(
            "CURRENT_TIME", SqlTypeName.Time) {
        public void test(SqlTester tester)
        {
            SqlOperatorTests.testCurrentTimeFunc(tester);
        }
    };

    /** The <code>CURRENT_TIMESTAMP [(<i>precision</i>)]</code> function. */
    public final SqlFunction currentTimestampFunc = new SqlAbstractTimeFunction(
            "CURRENT_TIMESTAMP", SqlTypeName.Timestamp) {
        public void test(SqlTester tester)
        {
            SqlOperatorTests.testCurrentTimestampFunc(tester);
        }
    };

    /** The <code>CURRENT_DATE</code> function. */
    public final SqlFunction currentDateFunc = new SqlFunction("CURRENT_DATE",
            SqlKind.Function, ReturnTypeInferenceImpl.useDate, null, null,
            SqlFunctionCategory.TimeDate) {
        public void test(SqlTester tester)
        {
            SqlOperatorTests.testCurrentDateFunc(tester);
        }
        public SqlSyntax getSyntax()
        {
            return SqlSyntax.FunctionId;
        }

        protected boolean checkArgTypes(SqlCall call, SqlValidator validator,
                SqlValidatorScope scope, boolean throwOnFailure)
        {
            Util.discard(call);
            Util.discard(validator);
            Util.discard(scope);
            Util.discard(throwOnFailure);
            return true;
        }

        public OperandsCountDescriptor getOperandsCountDescriptor()
        {
            return OperandsCountDescriptor.niladicCountDescriptor;
        }

        public boolean isMonotonic(SqlCall call, SqlValidatorScope scope) {
            return true;
        }

    };

    /**
     * The SQL <code>CAST</code> operator.
     *
     * <p/>The target type is simply stored as
     * the return type, not an explicit operand. For example, the expression
     * <code>CAST(1 + 2 AS DOUBLE)</code> will become a call to
     * <code>CAST</code> with the expression <code>1 + 2</code> as its only
     * operand.
     */
    public final SqlFunction castFunc = new SqlCastFunction();

    /**
     * The SQL <code>EXTRACT</code> operator.
     * Extracts a specified field value from a DATETIME or an INTERVAL.
     * E.g.<br>
     * <code>EXTRACT(HOUR FROM INTERVAL '364 23:59:59')</code> returns <code>23</code>
     */
    public final SqlFunction extractFunc =
        new SqlFunction("EXTRACT", SqlKind.Function,
            ReturnTypeInferenceImpl.useNullableDouble, null,
            OperandsTypeChecking.typeNullableIntervalInterval,
            SqlFunctionCategory.System) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testExtractFunc(tester);
            }

            protected String getSignatureTemplate(int operandsCount) {
                Util.discard(operandsCount);
                return "{0}({1} FROM {2})";
            }

            public void unparse(
                SqlWriter writer,
                SqlNode[] operands,
                int leftPrec,
                int rightPrec) {
                writer.print(name);
                writer.print("(");
                operands[0].unparse(writer, leftPrec, rightPrec);
                writer.print(" FROM ");
                operands[1].unparse(writer, leftPrec, rightPrec);
                writer.print(")");
            }


        };

    /**
     * The ELEMENT SQL operator, used to convert a multiset with only one item
     * to a "regular" type. Example
     * ... log(ELEMENT(MULTISET[1])) ...
     */
     public final SqlFunction elementFunc =
        new SqlFunction("ELEMENT", SqlKind.Function,
            ReturnTypeInferenceImpl.useNullableMultisetElementType, null,
            OperandsTypeChecking.typeNullableMultiset,
            SqlFunctionCategory.System) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testElementFunc(tester);
            }
        };

    /**
     * The CARDINALITY SQL operator, used to retreive the number of elements in
     * a MULTISET
     */
     public final SqlFunction cardinalityFunc =
        new SqlFunction("CARDINALITY", SqlKind.Function,
            ReturnTypeInferenceImpl.useNullableInteger, null,
            OperandsTypeChecking.typeNullableMultiset, SqlFunctionCategory.System) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testCardinalityFunc(tester);
            }
        };


    /**
     * The COLLECT SQL operator. Multiset aggregator function.
     */
     public final SqlFunction collectFunc =
        new SqlFunction("COLLECT", SqlKind.Function,
            ReturnTypeInferenceImpl.useFirstArgType, null,
            OperandsTypeChecking.typeAny, SqlFunctionCategory.System) {
            public void test(SqlTester tester)
            {
//                SqlOperatorTests.testCollect(tester);
            }
        };

    /**
     * The FUSION SQL operator. Multiset aggregator function.
     */
     public final SqlFunction fusionFunc =
        new SqlFunction("FUSION", SqlKind.Function,
            ReturnTypeInferenceImpl.useFirstArgType, null,
            OperandsTypeChecking.typeNullableMultiset, SqlFunctionCategory.System) {
            public void test(SqlTester tester)
            {
//                SqlOperatorTests.testFusion(tester);
            }
        };
}


// End SqlStdOperatorTable.java


