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

package org.eigenbase.sql.fun;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeFactoryImpl;
import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.ParserPosition;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.sql.test.SqlOperatorTests;
import org.eigenbase.sql.type.OperandsTypeChecking;
import org.eigenbase.sql.type.ReturnTypeInference;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.type.UnknownParamInference;
import org.eigenbase.util.Util;

import java.util.ArrayList;

/**
 * Extension to {@link org.eigenbase.sql.SqlOperatorTable} containing the
 * standard operators and functions.
 *
 * @author jhyde
 * @since May 28, 2004
 * @version $Id$
 **/
public class SqlStdOperatorTable extends SqlOperatorTable
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
    public static synchronized SqlStdOperatorTable std() {
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
                    SqlFunction.SqlFuncTypeName.TimeDate);
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

        public RelDataType getType(SqlValidator validator,
                SqlValidator.Scope scope, SqlCall call)
        {
            return inferType(validator, scope, call);
        }

        public RelDataType getType(RelDataTypeFactory typeFactory,
            RelDataType[] argTypes)
        {
            // REVIEW jvs 20-Feb-2004:  SqlTypeName says Time and Timestamp
            // don't take precision, but they should (according to the
            // standard). Also, need to take care of time zones.

            // TODO: use first arg as precision
            int precision = 0;
            return typeFactory.createSqlType(typeName, precision);
        }

        protected RelDataType inferType(SqlValidator validator,
                SqlValidator.Scope scope, SqlCall call)
        {
            int precision = 0;
            if (call.operands.length == 1) {
                precision = validator.getOperandAsPositiveInteger(call, 0);
            }
            return validator.typeFactory.createSqlType(typeName, precision);
        }
    }

    /**
     * Abstract base class for user functions such as "USER", "CURRENT_ROLE".
     */
    private static abstract class SqlAbstractUserFunction extends SqlFunction {
        public SqlAbstractUserFunction(String name) {
            super(name, SqlKind.Function, ReturnTypeInference.useVarchar30,
                null, OperandsTypeChecking.typeEmpty, SqlFunction.SqlFuncTypeName.System);
        }

        public OperandsCountDescriptor getOperandsCountDescriptor()
        {
            return new OperandsCountDescriptor(0);
        }

        public SqlSyntax getSyntax() {
            return SqlSyntax.FunctionId;
        }

        protected void checkArgTypes(SqlCall call, SqlValidator validator,
                SqlValidator.Scope scope) {
            if (call.operands.length != 0) {
                throw call.newValidationSignatureError(validator, scope);
            }
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

    public final SqlSetOperator multisetUnionOperator =
        new SqlSetOperator("MULTISET UNION", SqlKind.Other, 7, false,
            ReturnTypeInference.useNullableMultiset,
            UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeNullableMultisetMultiset);
    public final SqlSetOperator multisetUnionAllOperator =
        new SqlSetOperator("MULTISET UNION ALL", SqlKind.Other, 7, true,
            ReturnTypeInference.useNullableMultiset,
            UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeNullableMultisetMultiset);
    public final SqlSetOperator multisetExceptOperator =
        new SqlSetOperator("MULTISET EXCEPT", SqlKind.Other, 7, false,
            ReturnTypeInference.useNullableMultiset,
            UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeNullableMultisetMultiset);
    public final SqlSetOperator multisetExceptAllOperator =
        new SqlSetOperator("MULTISET EXCEPT ALL", SqlKind.Other, 7, true,
            ReturnTypeInference.useNullableMultiset,
            UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeNullableMultisetMultiset);
    public final SqlSetOperator multisetIntersectOperator =
        new SqlSetOperator("MULTISET INTERSECT", SqlKind.Other, 9, false,
            ReturnTypeInference.useNullableMultiset,
            UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeNullableMultisetMultiset);
    public final SqlSetOperator multisetIntersectAllOperator =
        new SqlSetOperator("MULTISET INTERSECT ALL", SqlKind.Other, 9, true,
            ReturnTypeInference.useNullableMultiset,
            UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeNullableMultisetMultiset);




    //-------------------------------------------------------------
    //                   BINARY OPERATORS
    //-------------------------------------------------------------
    public final SqlBinaryOperator andOperator =
        new SqlBinaryOperator("AND", SqlKind.And, 14, true,
            ReturnTypeInference.useNullableBoolean,
            UnknownParamInference.useBoolean,
            OperandsTypeChecking.typeNullableBoolBool) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testAndOperator(tester);
            }
        };

    public final SqlBinaryOperator asOperator =
        new SqlBinaryOperator("AS", SqlKind.As, 10, true, ReturnTypeInference.useFirstArgType,
            UnknownParamInference.useReturnType, OperandsTypeChecking.typeAnyAny) {
        };

    public final SqlBinaryOperator overOperator =
        new SqlBinaryOperator("OVER", SqlKind.Over, 10, true,
            ReturnTypeInference.useFirstArgType, null, null);

    public final SqlBinaryOperator concatOperator =
        new SqlBinaryOperator("||", SqlKind.Other, 30, true,
            ReturnTypeInference.useNullableVaryingDyadicStringSumPrecision, null,
            OperandsTypeChecking.typeNullableStringStringOfSameType) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testConcatOperator(tester);
            }
        };

    public final SqlBinaryOperator divideOperator =
        new SqlBinaryOperator("/", SqlKind.Divide, 30, true,
            ReturnTypeInference.useNullableBiggest, UnknownParamInference.useFirstKnown, OperandsTypeChecking.typeNumericNumeric) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testDivideOperator(tester);
            }
        };

    public final SqlBinaryOperator dotOperator =
        new SqlBinaryOperator(".", SqlKind.Dot, 40, true, null, null,
            OperandsTypeChecking.typeAnyAny);

    public final SqlBinaryOperator equalsOperator =
        new SqlBinaryOperator("=", SqlKind.Equals, 15, true,
            ReturnTypeInference.useNullableBoolean, UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeNullableComparable) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testEqualsOperator(tester);
            }
        };

    public final SqlBinaryOperator greaterThanOperator =
        new SqlBinaryOperator(">", SqlKind.GreaterThan, 15, true,
            ReturnTypeInference.useNullableBoolean, UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeNullableComparable) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testGreaterThanOperator(tester);
            }
        };

    public final SqlBinaryOperator isDistinctFromOperator =
        new SqlBinaryOperator("IS DISTINCT FROM", SqlKind.Other, 15, true,
            ReturnTypeInference.useNullableBoolean, UnknownParamInference.useFirstKnown, OperandsTypeChecking.typeAnyAny) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testIsDistinctFromOperator(tester);
            }
        };

    public final SqlBinaryOperator greaterThanOrEqualOperator =
        new SqlBinaryOperator(">=", SqlKind.GreaterThanOrEqual, 15, true,
            ReturnTypeInference.useNullableBoolean, UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeNullableComparable) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testGreaterThanOrEqualOperator(tester);
            }
        };

    public final SqlBinaryOperator inOperator =
        new SqlBinaryOperator("IN", SqlKind.In, 15, true, ReturnTypeInference.useNullableBoolean,
            UnknownParamInference.useFirstKnown, null) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testInOperator(tester);
            }
        };

    public final SqlBinaryOperator overlapsOperator =
        new SqlBinaryOperator("OVERLAPS", SqlKind.Overlaps, 15, true,
            ReturnTypeInference.useNullableBoolean, UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeNullableIntervalInterval) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testOverlapsOperator(tester);
            }
        };

    public final SqlBinaryOperator lessThanOperator =
        new SqlBinaryOperator("<", SqlKind.LessThan, 15, true,
            ReturnTypeInference.useNullableBoolean, UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeNullableComparable) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testLessThanOperator(tester);
            }
        };

    public final SqlBinaryOperator lessThanOrEqualOperator =
        new SqlBinaryOperator("<=", SqlKind.LessThanOrEqual, 15, true,
            ReturnTypeInference.useNullableBoolean, UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeNullableComparable) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testLessThanOrEqualOperator(tester);
            }
        };

    public final SqlBinaryOperator minusOperator =
        new SqlBinaryOperator("-", SqlKind.Minus, 20, true,
            ReturnTypeInference.useNullableBiggest,
            UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeMinusOperator) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testMinusOperator(tester);
            }
        };

    public final SqlBinaryOperator multiplyOperator =
        new SqlBinaryOperator("*", SqlKind.Times, 30, true,
            ReturnTypeInference.useNullableBiggest, UnknownParamInference.useFirstKnown, OperandsTypeChecking.typeNullableNumericNumeric) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testMultiplyOperator(tester);
            }
        };

    public final SqlBinaryOperator notEqualsOperator =
        new SqlBinaryOperator("<>", SqlKind.NotEquals, 15, true,
            ReturnTypeInference.useNullableBoolean, UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeNullableComparable) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testNotEqualsOperator(tester);
            }
        };

    public final SqlBinaryOperator orOperator =
        new SqlBinaryOperator("OR", SqlKind.Or, 13, true,
            ReturnTypeInference.useNullableBoolean,
            UnknownParamInference.useBoolean,
            OperandsTypeChecking.typeNullableBoolBool) {

            public void test(SqlTester tester)
            {
                SqlOperatorTests.testOrOperator(tester);
            }
        };

    public final SqlBinaryOperator plusOperator =
        new SqlBinaryOperator("+", SqlKind.Plus, 20, true,
            ReturnTypeInference.useNullableBiggest,
            UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typePlusOperator) {

            public void test(SqlTester tester)
            {
                SqlOperatorTests.testPlusOperator(tester);
            }
        };

    public final SqlBinaryOperator memberOfOperator =
            //TODO check precedence is correct
            new SqlBinaryOperator("MEMBER OF", SqlKind.Other, 15, true,
                ReturnTypeInference.useNullableBoolean,
                null, null) {
                public void test(SqlTester tester)
                {
                    SqlOperatorTests.testMemberOfOperator(tester);
                }

                protected void checkArgTypes(
                    SqlCall call,
                    SqlValidator validator,
                    SqlValidator.Scope scope) {

                    OperandsTypeChecking.typeNullableMultiset.
                        check(call, validator, scope, call.operands[1], 0, true);

                    RelDataTypeFactoryImpl.MultisetSqlType mt =
                        (RelDataTypeFactoryImpl.MultisetSqlType)
                        validator.deriveType(scope, call.operands[1]);

                    RelDataType t0 = validator.deriveType(scope, call.operands[0]);
                    RelDataType t1 = mt.getComponentType();

                    if (!t0.isAssignableFrom(t1, false) &&
                        !t1.isAssignableFrom(t0, false)) {
                        throw validator.newValidationError(call,
                            EigenbaseResource.instance().
                            newTypeNotComparableNear(
                                t0.toString(), t1.toString()));
                    }
                }

                public SqlOperator.OperandsCountDescriptor getOperandsCountDescriptor() {
                    return new SqlOperator.OperandsCountDescriptor(2);
                }

            };

    public final SqlBinaryOperator subMultisetOfOperator =
            //TODO check if precedence is correct
            new SqlBinaryOperator("SUBMULTISET OF", SqlKind.Other, 15, true,
                ReturnTypeInference.useNullableBoolean,
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
        new SqlPostfixOperator("IS NOT NULL", SqlKind.Other, 15, ReturnTypeInference.useBoolean,
            UnknownParamInference.useBoolean, OperandsTypeChecking.typeAny) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testIsNotNullOperator(tester);
            }
        };

    public final SqlPostfixOperator isNullOperator =
        new SqlPostfixOperator("IS NULL", SqlKind.IsNull, 15, ReturnTypeInference.useBoolean,
            UnknownParamInference.useBoolean, OperandsTypeChecking.typeAny) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testIsNullOperator(tester);
            }
        };

    public final SqlPostfixOperator isNotTrueOperator =
        new SqlPostfixOperator("IS NOT TRUE", SqlKind.Other, 15, ReturnTypeInference.useBoolean,
            UnknownParamInference.useBoolean, OperandsTypeChecking.typeNullableBool) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testIsNotTrueOperator(tester);
            }
        };

    public final SqlPostfixOperator isTrueOperator =
        new SqlPostfixOperator("IS TRUE", SqlKind.IsTrue, 15, ReturnTypeInference.useBoolean,
            UnknownParamInference.useBoolean, OperandsTypeChecking.typeNullableBool) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testIsTrueOperator(tester);
            }
        };

    public final SqlPostfixOperator isNotFalseOperator =
        new SqlPostfixOperator("IS NOT FALSE", SqlKind.Other, 15, ReturnTypeInference.useBoolean,
            UnknownParamInference.useBoolean, OperandsTypeChecking.typeNullableBool) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testIsNotFalseOperator(tester);
            }
        };

    public final SqlPostfixOperator isFalseOperator =
        new SqlPostfixOperator("IS FALSE", SqlKind.IsFalse, 15, ReturnTypeInference.useBoolean,
            UnknownParamInference.useBoolean, OperandsTypeChecking.typeNullableBool) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testIsFalseOperator(tester);
            }
        };

    public final SqlPostfixOperator isNotUnknownOperator =
        new SqlPostfixOperator("IS NOT UNKNOWN", SqlKind.Other, 15,
            ReturnTypeInference.useBoolean, UnknownParamInference.useBoolean, OperandsTypeChecking.typeNullableBool) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testIsNotUnknownOperator(tester);
            }
        };

    public final SqlPostfixOperator isUnknownOperator =
        new SqlPostfixOperator("IS UNKNOWN", SqlKind.IsNull, 15, ReturnTypeInference.useBoolean,
            UnknownParamInference.useBoolean, OperandsTypeChecking.typeNullableBool) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testIsUnknownOperator(tester);
            }
        };

    public final SqlPostfixOperator isASetOperator =
        new SqlPostfixOperator("IS A SET", SqlKind.Other, 15,
            ReturnTypeInference.useBoolean,
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
        new SqlPrefixOperator("EXISTS", SqlKind.Exists, 20, ReturnTypeInference.useBoolean, null,
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
        new SqlPrefixOperator("NOT", SqlKind.Not, 15, ReturnTypeInference.useNullableBoolean,
            UnknownParamInference.useBoolean, OperandsTypeChecking.typeNullableBool) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testNotOperator(tester);
            }
        };

    public final SqlPrefixOperator prefixMinusOperator =
        new SqlPrefixOperator("-", SqlKind.MinusPrefix, 20, ReturnTypeInference.useFirstArgType,
            UnknownParamInference.useReturnType, OperandsTypeChecking.typeNullableNumeric) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testPrefixMinusOperator(tester);
            }
        };

    public final SqlPrefixOperator prefixPlusOperator =
        new SqlPrefixOperator("+", SqlKind.PlusPrefix, 20, ReturnTypeInference.useFirstArgType,
            UnknownParamInference.useReturnType, OperandsTypeChecking.typeNullableNumeric) {
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



    //-------------------------------------------------------------
    //                   SPECIAL OPERATORS
    //-------------------------------------------------------------
    public final SqlRowOperator rowConstructor = new SqlRowOperator();

    public final SqlMultisetOperator multisetOperator =
        new SqlMultisetOperator();

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

    public final SqlInternalOperator litChainOperator =
        new SqlLiteralChainOperator();
    public final SqlBetweenOperator betweenOperator =
        new SqlBetweenOperator(
            SqlBetweenOperator.Flag.createAsymmetric(ParserPosition.ZERO),
            false);
    public final SqlBetweenOperator symmetricBetweenOperator =
        new SqlBetweenOperator(
            SqlBetweenOperator.Flag.createSymmetric(ParserPosition.ZERO),
            false);
    public final SqlBetweenOperator notBetweenOperator =
        new SqlBetweenOperator(
            SqlBetweenOperator.Flag.createAsymmetric(ParserPosition.ZERO),
            true);
    public final SqlBetweenOperator symmetricNotBetweenOperator =
        new SqlBetweenOperator(
            SqlBetweenOperator.Flag.createSymmetric(ParserPosition.ZERO),
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
    public final SqlWindowOperator windowOperator = new SqlWindowOperator();
    public final SqlOverOperator windowFuncOp = new SqlOverOperator();


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
            SqlFunction.SqlFuncTypeName.String) {
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
            SqlFunction.SqlFuncTypeName.String) {
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
            ReturnTypeInference.useNullableDyadicStringSumPrecision, null, null,
            SqlFunction.SqlFuncTypeName.String) {
            public OperandsCountDescriptor getOperandsCountDescriptor()
            {
                return new OperandsCountDescriptor(3, 4);
            }

            protected void checkArgTypes(
                SqlCall call,
                SqlValidator validator,
                SqlValidator.Scope scope)
            {
                switch (call.operands.length) {
                case 3:
                    OperandsTypeChecking.typeNullableStringStringNotNullableInt
                        .check(validator, scope, call, true);
                    break;
                case 4:
                    OperandsTypeChecking.typeNullableStringStringNotNullableIntInt
                        .check(validator, scope, call, true);
                    break;
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
            ReturnTypeInference.useNullableInteger, null,
            OperandsTypeChecking.typeNullableStringString,
            SqlFunction.SqlFuncTypeName.Numeric) {
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

            protected void checkArgTypes(
                SqlCall call,
                SqlValidator validator,
                SqlValidator.Scope scope)
            {
                //check that the two operands are of same type.
                RelDataType type0 =
                    validator.getValidatedNodeType(call.operands[0]);
                RelDataType type1 =
                    validator.getValidatedNodeType(call.operands[1]);
                if (!type0.isSameTypeFamily(type1)
                        && !type1.isSameTypeFamily(type0)) {
                    throw call.newValidationSignatureError(validator, scope);
                }

                operandsCheckingRule.check(validator, scope, call, true);
            }

            public void test(SqlTester tester)
            {
                SqlOperatorTests.testPositionFunc(tester);
            }
        };

    public final SqlFunction charLengthFunc =
        new SqlFunction("CHAR_LENGTH", SqlKind.Function,
            ReturnTypeInference.useNullableInteger, null,
            OperandsTypeChecking.typeNullableVarchar,
            SqlFunction.SqlFuncTypeName.Numeric) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testCharLengthFunc(tester);
            }
        };

    public final SqlFunction characterLengthFunc =
        new SqlFunction("CHARACTER_LENGTH", SqlKind.Function,
            ReturnTypeInference.useNullableInteger, null,
            OperandsTypeChecking.typeNullableVarchar,
            SqlFunction.SqlFuncTypeName.Numeric) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testCharacterLengthFunc(tester);
            }
        };

    public final SqlFunction upperFunc =
        new SqlFunction("UPPER", SqlKind.Function,
            ReturnTypeInference.useNullableFirstArgType, null,
            OperandsTypeChecking.typeNullableVarchar,
            SqlFunction.SqlFuncTypeName.String) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testUpperFunc(tester);
            }
        };

    public final SqlFunction lowerFunc =
        new SqlFunction("LOWER", SqlKind.Function,
            ReturnTypeInference.useNullableFirstArgType, null,
            OperandsTypeChecking.typeNullableVarchar,
            SqlFunction.SqlFuncTypeName.String) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testLowerFunc(tester);
            }
        };

    public final SqlFunction initcapFunc =
        new SqlFunction("INITCAP", SqlKind.Function,
            ReturnTypeInference.useNullableFirstArgType, null,
            OperandsTypeChecking.typeNullableVarchar,
            SqlFunction.SqlFuncTypeName.String) {
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
            ReturnTypeInference.useNullableDouble, null,
            OperandsTypeChecking.typeNumericNumeric,
            SqlFunction.SqlFuncTypeName.Numeric) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testPowFunc(tester);
            }
        };

    public final SqlFunction modFunc =
        new SqlFunction("MOD", SqlKind.Function,
            ReturnTypeInference.useNullableBiggest, null,
            OperandsTypeChecking.typeNullableIntegerInteger,
            SqlFunction.SqlFuncTypeName.Numeric) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testModFunc(tester);
            }
        };

    public final SqlFunction lnFunc =
        new SqlFunction("LN", SqlKind.Function,
            ReturnTypeInference.useNullableDouble, null,
            OperandsTypeChecking.typeNumeric, SqlFunction.SqlFuncTypeName.Numeric) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testLnFunc(tester);
            }
        };

    public final SqlFunction logFunc =
        new SqlFunction("LOG", SqlKind.Function,
            ReturnTypeInference.useNullableDouble, null,
            OperandsTypeChecking.typeNumeric, SqlFunction.SqlFuncTypeName.Numeric) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testLogFunc(tester);
            }
        };

    public final SqlFunction absFunc =
        new SqlFunction("ABS", SqlKind.Function,
            ReturnTypeInference.useNullableBiggest, null,
            OperandsTypeChecking.typeNumeric, SqlFunction.SqlFuncTypeName.Numeric) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testAbsFunc(tester);
            }
        };

    public final SqlFunction nullIfFunc =
        new SqlFunction("NULLIF", SqlKind.Function, null, null, null,
            SqlFunction.SqlFuncTypeName.System) {
            public SqlCall createCall(
                SqlNode [] operands,
                ParserPosition pos)
            {
                if (2 != operands.length) {
                    //todo put this in the validator
                    throw EigenbaseResource.instance().newValidatorContext(
                        new Integer(pos.getBeginLine()),
                        new Integer(pos.getBeginColumn()),
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
            SqlFunction.SqlFuncTypeName.System) {
            public SqlCall createCall(
                SqlNode [] operands,
                ParserPosition pos)
            {
                Util.pre(operands.length >= 2, "operands.length>=2");
                return createCall(operands, 0, pos);
            }

            private SqlCall createCall(
                SqlNode [] operands,
                int start,
                ParserPosition pos)
            {
                SqlNodeList whenList = new SqlNodeList(pos);
                SqlNodeList thenList = new SqlNodeList(pos);

                //todo optimize when know operand is not null.
                whenList.add(
                    isNotNullOperator.createCall(operands[start], pos));
                thenList.add(operands[start]);
                if (2 == (operands.length - start)) {
                    return caseOperator.createCall(null, whenList, thenList,
                        operands[start + 1], pos);
                }
                return caseOperator.createCall(
                    null,
                    whenList,
                    thenList,
                    this.createCall(operands, start + 1, pos),
                    pos);
            }

            public SqlOperator.OperandsCountDescriptor getOperandsCountDescriptor()
            {
                return new OperandsCountDescriptor(2);
            }

            public void test(SqlTester tester)
            {
                SqlOperatorTests.testCoalesceFunc(tester);
            }
        };

    /** The <code>USER</code> function. */
    public final SqlFunction userFunc = new SqlAbstractUserFunction("USER") {
        public void test(SqlTester tester)
        {
            SqlOperatorTests.testUserFunc(tester);
        }
    };

    /** The <code>CURRENT_USER</code> function. */
    public final SqlFunction currentUserFunc = new SqlAbstractUserFunction(
            "CURRENT_USER") {
        public void test(SqlTester tester)
        {
            SqlOperatorTests.testCurrentUserFunc(tester);
        }
    };

    /** The <code>SESSION_USER</code> function. */
    public final SqlFunction sessionUserFunc = new SqlAbstractUserFunction(
            "SESSION_USER") {
        public void test(SqlTester tester)
        {
            SqlOperatorTests.testSessionUserFunc(tester);
        }
    };

    /** The <code>SYSTEM_USER</code> function. */
    public final SqlFunction systemUserFunc = new SqlAbstractUserFunction(
            "SYSTEM_USER") {
        public void test(SqlTester tester)
        {
            SqlOperatorTests.testSystemUserFunc(tester);
        }
    };

    /** The <code>CURRENT_PATH</code> function. */
    public final SqlFunction currentPathFunc = new SqlAbstractUserFunction(
            "CURRENT_PATH") {
        public void test(SqlTester tester)
        {
            SqlOperatorTests.testCurrentPathFunc(tester);
        }
    };

    /** The <code>CURRENT_ROLE</code> function. */
    public final SqlFunction currentRoleFunc = new SqlAbstractUserFunction(
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
            SqlKind.Function, ReturnTypeInference.useDate, null, null,
            SqlFunction.SqlFuncTypeName.TimeDate) {
        public void test(SqlTester tester)
        {
            SqlOperatorTests.testCurrentDateFunc(tester);
        }
        public SqlSyntax getSyntax()
        {
            return SqlSyntax.FunctionId;
        }

        protected void checkArgTypes(SqlCall call, SqlValidator validator,
                SqlValidator.Scope scope)
        {
            if (call.operands.length != 0) {
                throw call.newValidationSignatureError(validator, scope);
            }
        }

        // no argTypeInference, so must override these methods.
        // Probably need a niladic version of that.
        public OperandsCountDescriptor getOperandsCountDescriptor()
        {
            return new OperandsCountDescriptor(0);
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
     * The ELEMENT SQL operator, used to convert a multiset with only one item
     * to a "regular" type. Example
     * ... log(ELEMENT(MULTISET[1])) ...
     */
     public final SqlFunction elementFunc =
        new SqlFunction("ELEMENT", SqlKind.Function,
            ReturnTypeInference.useNullableMultisetElementType, null,
            OperandsTypeChecking.typeNullableMultiset,
            SqlFunction.SqlFuncTypeName.System) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testElementFunc(tester);
            }
        };

    /**
     * The CARDINALITY SQL operator, used to retreive the nbr of elements in
     * the MULTISET
     */
     public final SqlFunction cardinalityFunc =
        new SqlFunction("CARDINALITY", SqlKind.Function,
            ReturnTypeInference.useNullableInteger, null,
            OperandsTypeChecking.typeNullableMultiset, SqlFunction.SqlFuncTypeName.System) {
            public void test(SqlTester tester)
            {
                SqlOperatorTests.testCardinalityFunc(tester);
            }
        };

}


// End SqlStdOperatorTable.java

