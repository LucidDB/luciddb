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
import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.ParserPosition;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.sql.type.OperandsTypeChecking;
import org.eigenbase.sql.type.ReturnTypeInference;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.type.UnknownParamInference;
import org.eigenbase.util.Util;

import java.util.ArrayList;
import java.util.regex.Pattern;


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

    /**
     * Regular expression for a SQL TIME(0) value.
     */
    private static final Pattern timePattern = Pattern.compile(
        "[0-9][0-9]:[0-9][0-9]:[0-9][0-9]");

    /**
     * Regular expression for a SQL TIMESTAMP(3) value.
     */
    private static final Pattern timestampPattern = Pattern.compile(
        "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] " +
        "[0-9][0-9]:[0-9][0-9]:[0-9][0-9]\\.[0-9]+");

    /**
     * Regular expression for a SQL DATE value.
     */
    private static final Pattern datePattern = Pattern.compile(
        "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]");


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
    public final SqlSetOperator exceptOperator =
        new SqlSetOperator("EXCEPT", SqlKind.Except, 9, false);
    public final SqlSetOperator exceptAllOperator =
        new SqlSetOperator("EXCEPT ALL", SqlKind.Except, 9, true);
    public final SqlSetOperator intersectOperator =
        new SqlSetOperator("INTERSECT", SqlKind.Intersect, 9, false);
    public final SqlSetOperator intersectAllOperator =
        new SqlSetOperator("INTERSECT ALL", SqlKind.Intersect, 9, true);
    public final SqlSetOperator unionOperator =
        new SqlSetOperator("UNION", SqlKind.Union, 7, false);
    public final SqlSetOperator unionAllOperator =
        new SqlSetOperator("UNION ALL", SqlKind.Union, 7, true);

    //-------------------------------------------------------------
    //                   BINARY OPERATORS
    //-------------------------------------------------------------
    public final SqlBinaryOperator andOperator =
        new SqlBinaryOperator("AND", SqlKind.And, 14, true,
            ReturnTypeInference.useNullableBoolean, UnknownParamInference.useBoolean, OperandsTypeChecking.typeNullableBoolBool) {
            public void test(SqlTester tester)
            {
                tester.checkBoolean("true and false", Boolean.FALSE);
                tester.checkBoolean("true and true", Boolean.TRUE);
                tester.checkBoolean("cast(null as boolean) and false",
                    Boolean.FALSE);
                tester.checkBoolean("false and cast(null as boolean)",
                    Boolean.FALSE);
                tester.checkNull("cast(null as boolean) and true");
            }
        };
    public final SqlBinaryOperator asOperator =
        new SqlBinaryOperator("AS", SqlKind.As, 10, true, ReturnTypeInference.useFirstArgType,
            UnknownParamInference.useReturnType, OperandsTypeChecking.typeAnyAny) {
            public void test(SqlTester tester)
            {
                /* empty implementation */
            }
        };
    public final SqlBinaryOperator concatOperator =
        new SqlBinaryOperator("||", SqlKind.Other, 30, true,
            ReturnTypeInference.useNullableVaryingDyadicStringSumPrecision, null,
            OperandsTypeChecking.typeNullableStringStringOfSameType) {
            public void test(SqlTester tester)
            {
                tester.checkString(" 'a'||'b' ", "ab");

                //not yet implemented
                //                    tester.checkString(" x'f'||x'f' ", "X'FF")
                //                    tester.checkString(" b'1'||b'0' ", "B'10'");
                //                    tester.checkString(" b'1'||b'' ", "B'1'");
                //                    tester.checkNull("x'ff' || cast(null as varbinary)");
            }
        };
    public final SqlBinaryOperator divideOperator =
        new SqlBinaryOperator("/", SqlKind.Divide, 30, true,
            ReturnTypeInference.useNullableBiggest, UnknownParamInference.useFirstKnown, OperandsTypeChecking.typeNumericNumeric) {
            public void test(SqlTester tester)
            {
                tester.checkScalarExact("10 / 5", "2");
                tester.checkScalarApprox("10.0 / 5", "2.0");
                tester.checkNull("1e1 / cast(null as float)");
            }
        };
    public final SqlBinaryOperator dotOperator =
        new SqlBinaryOperator(".", SqlKind.Dot, 40, true, null, null,
            OperandsTypeChecking.typeAnyAny) {
            public void test(SqlTester tester)
            {
                /* empty implementation */
            }
        };
    public final SqlBinaryOperator equalsOperator =
        new SqlBinaryOperator("=", SqlKind.Equals, 15, true,
            ReturnTypeInference.useNullableBoolean, UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeNullableComparable) {
            public void test(SqlTester tester)
            {
                tester.checkBoolean("1=1", Boolean.TRUE);
                tester.checkBoolean("'a'='b'", Boolean.FALSE);
                tester.checkNull("cast(null as boolean)=cast(null as boolean)");
                tester.checkNull("cast(null as integer)=1");
            }
        };
    public final SqlBinaryOperator greaterThanOperator =
        new SqlBinaryOperator(">", SqlKind.GreaterThan, 15, true,
            ReturnTypeInference.useNullableBoolean, UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeNullableComparable) {
            public void test(SqlTester tester)
            {
                tester.checkBoolean("1>2", Boolean.FALSE);
                tester.checkBoolean("1>1", Boolean.FALSE);
                tester.checkBoolean("2>1", Boolean.TRUE);
                tester.checkNull("3.0>cast(null as double)");
            }
        };
    public final SqlBinaryOperator isDistinctFromOperator =
        new SqlBinaryOperator("IS DISTINCT FROM", SqlKind.Other, 15, true,
            ReturnTypeInference.useNullableBoolean, UnknownParamInference.useFirstKnown, OperandsTypeChecking.typeAnyAny) {
            public void test(SqlTester tester)
            {
                /* empty implementation */
            }
        };
    public final SqlBinaryOperator greaterThanOrEqualOperator =
        new SqlBinaryOperator(">=", SqlKind.GreaterThanOrEqual, 15, true,
            ReturnTypeInference.useNullableBoolean, UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeNullableComparable) {
            public void test(SqlTester tester)
            {
                tester.checkBoolean("1>=2", Boolean.FALSE);
                tester.checkBoolean("1>=1", Boolean.TRUE);
                tester.checkBoolean("2>=1", Boolean.TRUE);
                tester.checkNull("cast(null as real)>=999");
            }
        };
    public final SqlBinaryOperator inOperator =
        new SqlBinaryOperator("IN", SqlKind.In, 15, true, ReturnTypeInference.useNullableBoolean,
            UnknownParamInference.useFirstKnown, null) {
            public void test(SqlTester tester)
            {
                /* empty implementation */
            }
        };
    public final SqlBinaryOperator overlapsOperator =
        new SqlBinaryOperator("OVERLAPS", SqlKind.Overlaps, 15, true,
            ReturnTypeInference.useNullableBoolean, UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeNullableIntervalInterval) {
            public void test(SqlTester tester)
            {
                //?todo
            }
        };
    public final SqlBinaryOperator lessThanOperator =
        new SqlBinaryOperator("<", SqlKind.LessThan, 15, true,
            ReturnTypeInference.useNullableBoolean, UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeNullableComparable) {
            public void test(SqlTester tester)
            {
                tester.checkBoolean("1<2", Boolean.TRUE);
                tester.checkBoolean("1<1", Boolean.FALSE);
                tester.checkBoolean("2<1", Boolean.FALSE);
                tester.checkNull("123<cast(null as bigint)");
            }
        };
    public final SqlBinaryOperator lessThanOrEqualOperator =
        new SqlBinaryOperator("<=", SqlKind.LessThanOrEqual, 15, true,
            ReturnTypeInference.useNullableBoolean, UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeNullableComparable) {
            public void test(SqlTester tester)
            {
                tester.checkBoolean("1<=2", Boolean.TRUE);
                tester.checkBoolean("1<=1", Boolean.TRUE);
                tester.checkBoolean("2<=1", Boolean.FALSE);
                tester.checkNull("cast(null as integer)<=3");
            }
        };
    public final SqlBinaryOperator minusOperator =
        new SqlBinaryOperator("-", SqlKind.Minus, 20, true,
            ReturnTypeInference.useNullableBiggest, UnknownParamInference.useFirstKnown, OperandsTypeChecking.typeNullableNumericNumeric) {
            public void test(SqlTester tester)
            {
                tester.checkScalarExact("2-1", "1");
                tester.checkScalarApprox("2.0-1", "1.0");
                tester.checkScalarExact("1-2", "-1");
                tester.checkNull("1e1-cast(null as double)");
            }
        };
    public final SqlBinaryOperator multiplyOperator =
        new SqlBinaryOperator("*", SqlKind.Times, 30, true,
            ReturnTypeInference.useNullableBiggest, UnknownParamInference.useFirstKnown, OperandsTypeChecking.typeNullableNumericNumeric) {
            public void test(SqlTester tester)
            {
                tester.checkScalarExact("2*3", "6");
                tester.checkScalarExact("2*-3", "-6");
                tester.checkScalarExact("2*0", "0");
                tester.checkScalarApprox("2.0*3", "6.0");
                tester.checkNull("2e-3*cast(null as integer)");
            }
        };
    public final SqlBinaryOperator notEqualsOperator =
        new SqlBinaryOperator("<>", SqlKind.NotEquals, 15, true,
            ReturnTypeInference.useNullableBoolean, UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeNullableComparable) {
            public void test(SqlTester tester)
            {
                tester.checkBoolean("1<>1", Boolean.FALSE);
                tester.checkBoolean("'a'<>'A'", Boolean.TRUE);
                tester.checkNull("'a'<>cast(null as varchar)");
            }
        };
    public final SqlBinaryOperator orOperator =
        new SqlBinaryOperator("OR", SqlKind.Or, 13, true, ReturnTypeInference.useNullableBoolean,
            UnknownParamInference.useBoolean, OperandsTypeChecking.typeNullableBoolBool) {
            public void test(SqlTester tester)
            {
                tester.checkBoolean("true or false", Boolean.TRUE);
                tester.checkBoolean("false or false", Boolean.FALSE);
                tester.checkBoolean("true or cast(null as boolean)",
                    Boolean.TRUE);
                tester.checkNull("false or cast(null as boolean)");
            }
        };

    public final SqlBinaryOperator plusOperator =
        new SqlBinaryOperator("+", SqlKind.Plus, 20, true, ReturnTypeInference.useNullableBiggest,
            UnknownParamInference.useFirstKnown, OperandsTypeChecking.typeNullableNumericNumeric) {
            public void test(SqlTester tester)
            {
                tester.checkScalarExact("1+2", "3");
                tester.checkScalarApprox("1+2.0", "3.0");
                tester.checkNull("cast(null as tinyint)+1");
                tester.checkNull("1e-2+cast(null as double)");
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
                /* empty implementation */
            }
        };
    public final SqlPostfixOperator isNotNullOperator =
        new SqlPostfixOperator("IS NOT NULL", SqlKind.Other, 15, ReturnTypeInference.useBoolean,
            UnknownParamInference.useBoolean, OperandsTypeChecking.typeAny) {
            public void test(SqlTester tester)
            {
                tester.checkBoolean("true is not null", Boolean.TRUE);
                tester.checkBoolean("cast(null as boolean) is not null",
                    Boolean.FALSE);
            }
        };
    public final SqlPostfixOperator isNullOperator =
        new SqlPostfixOperator("IS NULL", SqlKind.IsNull, 15, ReturnTypeInference.useBoolean,
            UnknownParamInference.useBoolean, OperandsTypeChecking.typeAny) {
            public void test(SqlTester tester)
            {
                tester.checkBoolean("true is null", Boolean.FALSE);
                tester.checkBoolean("cast(null as boolean) is null",
                    Boolean.TRUE);
            }
        };
    public final SqlPostfixOperator isNotTrueOperator =
        new SqlPostfixOperator("IS NOT TRUE", SqlKind.Other, 15, ReturnTypeInference.useBoolean,
            UnknownParamInference.useBoolean, OperandsTypeChecking.typeNullableBool) {
            public void test(SqlTester tester)
            {
                tester.checkBoolean("true is not true", Boolean.FALSE);
                tester.checkBoolean("false is not true", Boolean.TRUE);
                tester.checkBoolean("cast(null as boolean) is not true",
                    Boolean.TRUE);
            }
        };
    public final SqlPostfixOperator isTrueOperator =
        new SqlPostfixOperator("IS TRUE", SqlKind.IsTrue, 15, ReturnTypeInference.useBoolean,
            UnknownParamInference.useBoolean, OperandsTypeChecking.typeNullableBool) {
            public void test(SqlTester tester)
            {
                tester.checkBoolean("true is true", Boolean.TRUE);
                tester.checkBoolean("false is true", Boolean.FALSE);
                tester.checkBoolean("cast(null as boolean) is true",
                    Boolean.FALSE);
            }
        };
    public final SqlPostfixOperator isNotFalseOperator =
        new SqlPostfixOperator("IS NOT FALSE", SqlKind.Other, 15, ReturnTypeInference.useBoolean,
            UnknownParamInference.useBoolean, OperandsTypeChecking.typeNullableBool) {
            public void test(SqlTester tester)
            {
                tester.checkBoolean("false is not false", Boolean.FALSE);
                tester.checkBoolean("true is not false", Boolean.TRUE);
                tester.checkBoolean("cast(null as boolean) is not false",
                    Boolean.TRUE);
            }
        };
    public final SqlPostfixOperator isFalseOperator =
        new SqlPostfixOperator("IS FALSE", SqlKind.IsFalse, 15, ReturnTypeInference.useBoolean,
            UnknownParamInference.useBoolean, OperandsTypeChecking.typeNullableBool) {
            public void test(SqlTester tester)
            {
                tester.checkBoolean("false is false", Boolean.TRUE);
                tester.checkBoolean("true is false", Boolean.FALSE);
                tester.checkBoolean("cast(null as boolean) is false",
                    Boolean.FALSE);
            }
        };
    public final SqlPostfixOperator isNotUnknownOperator =
        new SqlPostfixOperator("IS NOT UNKNOWN", SqlKind.Other, 15,
            ReturnTypeInference.useBoolean, UnknownParamInference.useBoolean, OperandsTypeChecking.typeNullableBool) {
            public void test(SqlTester tester)
            {
                tester.checkBoolean("false is not unknown", Boolean.TRUE);
                tester.checkBoolean("true is not unknown", Boolean.TRUE);
                tester.checkBoolean("cast(null as boolean) is not unknown",
                    Boolean.FALSE);
                tester.checkBoolean("unknown is not unknown", Boolean.FALSE);
            }
        };
    public final SqlPostfixOperator isUnknownOperator =
        new SqlPostfixOperator("IS UNKNOWN", SqlKind.IsNull, 15, ReturnTypeInference.useBoolean,
            UnknownParamInference.useBoolean, OperandsTypeChecking.typeNullableBool) {
            public void test(SqlTester tester)
            {
                tester.checkBoolean("false is unknown", Boolean.FALSE);
                tester.checkBoolean("true is unknown", Boolean.FALSE);
                tester.checkBoolean("cast(null as boolean) is unknown",
                    Boolean.TRUE);
                tester.checkBoolean("unknown is unknown", Boolean.TRUE);
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
                /* empty implementation */
            }
        };
    public final SqlPrefixOperator notOperator =
        new SqlPrefixOperator("NOT", SqlKind.Not, 15, ReturnTypeInference.useNullableBoolean,
            UnknownParamInference.useBoolean, OperandsTypeChecking.typeNullableBool) {
            public void test(SqlTester tester)
            {
                tester.checkBoolean("not true", Boolean.FALSE);
                tester.checkBoolean("not false", Boolean.TRUE);
                tester.checkBoolean("not unknown", null);
                tester.checkNull("not cast(null as boolean)");
            }
        };
    public final SqlPrefixOperator prefixMinusOperator =
        new SqlPrefixOperator("-", SqlKind.MinusPrefix, 20, ReturnTypeInference.useFirstArgType,
            UnknownParamInference.useReturnType, OperandsTypeChecking.typeNullableNumeric) {
            public void test(SqlTester tester)
            {
                tester.checkScalarExact("-1", "-1");
                tester.checkScalarApprox("-1.0", "-1.0");
                tester.checkNull("-cast(null as integer)");
            }
        };
    public final SqlPrefixOperator prefixPlusOperator =
        new SqlPrefixOperator("+", SqlKind.PlusPrefix, 20, ReturnTypeInference.useFirstArgType,
            UnknownParamInference.useReturnType, OperandsTypeChecking.typeNullableNumeric) {
            public void test(SqlTester tester)
            {
                tester.checkScalarExact("+1", "1");
                tester.checkScalarApprox("+1.0", "1.0");
                tester.checkNull("+cast(null as integer)");
            }
        };
    public final SqlPrefixOperator explicitTableOperator =
        new SqlPrefixOperator("TABLE", SqlKind.ExplicitTable, 1, null, null,
            null) {
            public void test(SqlTester tester)
            {
                /* empty implementation */
            }
        };

    //-------------------------------------------------------------
    //                   SPECIAL OPERATORS
    //-------------------------------------------------------------
    public final SqlRowOperator rowConstructor = new SqlRowOperator();

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
                tester.check("select 'abc' from values(true)", "abc",
                    SqlTypeName.Varchar);
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
                tester.checkBoolean("'abc' not like '_b_'", Boolean.FALSE);
            }
        };
    public final SqlSpecialOperator likeOperator =
        new SqlLikeOperator("LIKE", SqlKind.Like, false) {
            public void test(SqlTester tester)
            {
                tester.checkBoolean("''  like ''", Boolean.TRUE);
                tester.checkBoolean("'a' like 'a'", Boolean.TRUE);
                tester.checkBoolean("'a' like 'b'", Boolean.FALSE);
                tester.checkBoolean("'a' like 'A'", Boolean.FALSE);
                tester.checkBoolean("'a' like 'a_'", Boolean.FALSE);
                tester.checkBoolean("'a' like '_a'", Boolean.FALSE);
                tester.checkBoolean("'a' like '%a'", Boolean.TRUE);
                tester.checkBoolean("'a' like '%a%'", Boolean.TRUE);
                tester.checkBoolean("'a' like 'a%'", Boolean.TRUE);
                tester.checkBoolean("'ab'   like 'a_'", Boolean.TRUE);
                tester.checkBoolean("'abc'  like 'a_'", Boolean.FALSE);
                tester.checkBoolean("'abcd' like 'a%'", Boolean.TRUE);
                tester.checkBoolean("'ab'   like '_b'", Boolean.TRUE);
                tester.checkBoolean("'abcd' like '_d'", Boolean.FALSE);
                tester.checkBoolean("'abcd' like '%d'", Boolean.TRUE);
            }
        };
    public final SqlSpecialOperator notSimilarOperator =
        new SqlLikeOperator("NOT SIMILAR TO", SqlKind.Similar, true) {
            public void test(SqlTester tester)
            {
                tester.checkBoolean("'ab' not similar to 'a_'", Boolean.FALSE);
            }
        };
    public final SqlSpecialOperator similarOperator =
        new SqlLikeOperator("SIMILAR TO", SqlKind.Similar, false) {
            public void test(SqlTester tester)
            {
                // like LIKE
                tester.checkBoolean("''  similar to ''", Boolean.TRUE);
                tester.checkBoolean("'a' similar to 'a'", Boolean.TRUE);
                tester.checkBoolean("'a' similar to 'b'", Boolean.FALSE);
                tester.checkBoolean("'a' similar to 'A'", Boolean.FALSE);
                tester.checkBoolean("'a' similar to 'a_'", Boolean.FALSE);
                tester.checkBoolean("'a' similar to '_a'", Boolean.FALSE);
                tester.checkBoolean("'a' similar to '%a'", Boolean.TRUE);
                tester.checkBoolean("'a' similar to '%a%'", Boolean.TRUE);
                tester.checkBoolean("'a' similar to 'a%'", Boolean.TRUE);
                tester.checkBoolean("'ab'   similar to 'a_'", Boolean.TRUE);
                tester.checkBoolean("'abc'  similar to 'a_'", Boolean.FALSE);
                tester.checkBoolean("'abcd' similar to 'a%'", Boolean.TRUE);
                tester.checkBoolean("'ab'   similar to '_b'", Boolean.TRUE);
                tester.checkBoolean("'abcd' similar to '_d'", Boolean.FALSE);
                tester.checkBoolean("'abcd' similar to '%d'", Boolean.TRUE);

                // simple regular expressions
                // ab*c+d matches acd, abcd, acccd, abcccd but not abd, aabc
                tester.checkBoolean("'acd'    similar to 'ab*c+d'",
                    Boolean.TRUE);
                tester.checkBoolean("'abcd'   similar to 'ab*c+d'",
                    Boolean.TRUE);
                tester.checkBoolean("'acccd'  similar to 'ab*c+d'",
                    Boolean.TRUE);
                tester.checkBoolean("'abcccd' similar to 'ab*c+d'",
                    Boolean.TRUE);
                tester.checkBoolean("'abd'    similar to 'ab*c+d'",
                    Boolean.FALSE);
                tester.checkBoolean("'aabc'   similar to 'ab*c+d'",
                    Boolean.FALSE);

                // compound regular expressions
                // x(ab|c)*y matches xy, xccy, xababcy but not xbcy
                tester.checkBoolean("'xy'      similar to 'x(ab|c)*y'",
                    Boolean.TRUE);
                tester.checkBoolean("'xccy'    similar to 'x(ab|c)*y'",
                    Boolean.TRUE);
                tester.checkBoolean("'xababcy' similar to 'x(ab|c)*y'",
                    Boolean.TRUE);
                tester.checkBoolean("'xbcy'    similar to 'x(ab|c)*y'",
                    Boolean.FALSE);

                // x(ab|c)+y matches xccy, xababcy but not xy, xbcy
                tester.checkBoolean("'xy'      similar to 'x(ab|c)+y'",
                    Boolean.FALSE);
                tester.checkBoolean("'xccy'    similar to 'x(ab|c)+y'",
                    Boolean.TRUE);
                tester.checkBoolean("'xababcy' similar to 'x(ab|c)+y'",
                    Boolean.TRUE);
                tester.checkBoolean("'xbcy'    similar to 'x(ab|c)+y'",
                    Boolean.FALSE);
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
            }
        };
    public final SqlSelectOperator selectOperator = new SqlSelectOperator();
    public final SqlCaseOperator caseOperator =
        new SqlCaseOperator() {
            public void test(SqlTester tester)
            {
                tester.checkScalarExact("case when 'a'='a' then 1 end", "1");
                tester.checkString("case 2 when 1 then 'a' when 2 then 'b' end",
                    "b");
                tester.checkScalarExact("case 'a' when 'a' then 1 end", "1");
                tester.checkNull("case 'a' when 'b' then 1 end");
                tester.checkScalarExact("case when 'a'=cast(null as varchar) then 1 else 2 end",
                    "2");
            }
        };
    public final SqlJoinOperator joinOperator = new SqlJoinOperator();
    public final SqlSpecialOperator insertOperator =
        new SqlSpecialOperator("INSERT", SqlKind.Insert) {
            public void test(SqlTester tester)
            {
                /* empty implementation */
            }
        };
    public final SqlSpecialOperator deleteOperator =
        new SqlSpecialOperator("DELETE", SqlKind.Delete) {
            public void test(SqlTester tester)
            {
                /* empty implementation */
            }
        };
    public final SqlSpecialOperator updateOperator =
        new SqlSpecialOperator("UPDATE", SqlKind.Update) {
            public void test(SqlTester tester)
            {
                /* empty implementation */
            }
        };
    public final SqlSpecialOperator explainOperator =
        new SqlSpecialOperator("EXPLAIN", SqlKind.Explain) {
            public void test(SqlTester tester)
            {
                /* empty implementaion */
            }
        };
    public final SqlOrderByOperator orderByOperator = new SqlOrderByOperator();


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
                //todo: implement when convert exist in the calculator
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
                //todo: implement when translate exist in the calculator
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
                tester.checkString("overlay('ABCdef' placing 'abc' from 1)",
                    "abcdef");
                tester.checkString("overlay('ABCdef' placing 'abc' from 1 for 2)",
                    "abcCdef");
                tester.checkNull(
                    "overlay('ABCdef' placing 'abc' from 1 for cast(null as integer))");
                tester.checkNull(
                    "overlay(cast(null as varchar) placing 'abc' from 1)");

                //hex and bit strings not yet implemented in calc
                //                    tester.checkNull("overlay(x'abc' placing x'abc' from cast(null as integer))");
                //                    tester.checkNull("overlay(b'1' placing cast(null as bit) from 1)");
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
                tester.checkScalarExact("position('b' in 'abc')", "2");
                tester.checkScalarExact("position('' in 'abc')", "1");

                //bit not yet implemented
                //                    tester.checkScalarExact("position(b'10' in b'0010')", "3");
                tester.checkNull("position(cast(null as varchar) in '0010')");
                tester.checkNull("position('a' in cast(null as varchar))");
            }
        };
    public final SqlFunction charLengthFunc =
        new SqlFunction("CHAR_LENGTH", SqlKind.Function,
            ReturnTypeInference.useNullableInteger, null,
            OperandsTypeChecking.typeNullableVarchar,
            SqlFunction.SqlFuncTypeName.Numeric) {
            public void test(SqlTester tester)
            {
                tester.checkScalarExact("char_length('abc')", "3");
                tester.checkNull("char_length(cast(null as varchar))");
            }
        };
    public final SqlFunction characterLengthFunc =
        new SqlFunction("CHARACTER_LENGTH", SqlKind.Function,
            ReturnTypeInference.useNullableInteger, null,
            OperandsTypeChecking.typeNullableVarchar,
            SqlFunction.SqlFuncTypeName.Numeric) {
            public void test(SqlTester tester)
            {
                tester.checkScalarExact("CHARACTER_LENGTH('abc')", "3");
                tester.checkNull("CHARACTER_LENGTH(cast(null as varchar))");
            }
        };
    public final SqlFunction upperFunc =
        new SqlFunction("UPPER", SqlKind.Function,
            ReturnTypeInference.useNullableFirstArgType, null,
            OperandsTypeChecking.typeNullableVarchar,
            SqlFunction.SqlFuncTypeName.String) {
            public void test(SqlTester tester)
            {
                tester.checkString("upper('a')", "A");
                tester.checkString("upper('A')", "A");
                tester.checkString("upper('1')", "1");
                tester.checkNull("upper(cast(null as varchar))");
            }
        };
    public final SqlFunction lowerFunc =
        new SqlFunction("LOWER", SqlKind.Function,
            ReturnTypeInference.useNullableFirstArgType, null,
            OperandsTypeChecking.typeNullableVarchar,
            SqlFunction.SqlFuncTypeName.String) {
            public void test(SqlTester tester)
            {
                tester.checkString("lower('A')", "a");
                tester.checkString("lower('a')", "a");
                tester.checkString("lower('1')", "1");
                tester.checkNull("lower(cast(null as varchar))");
            }
        };
    public final SqlFunction initcapFunc =
        new SqlFunction("INITCAP", SqlKind.Function,
            ReturnTypeInference.useNullableFirstArgType, null,
            OperandsTypeChecking.typeNullableVarchar,
            SqlFunction.SqlFuncTypeName.String) {
            public void test(SqlTester tester)
            {
                //not yet supported
                //                    tester.checkString("initcap('aA')", "'Aa'");
                //                    tester.checkString("initcap('Aa')", "'Aa'");
                //                    tester.checkString("initcap('1a')", "'1a'");
                //                    tester.checkNull("initcap(cast(null as varchar))");
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
                tester.checkScalarApprox("pow(2,-2)", "0.25");
                tester.checkNull("pow(cast(null as integer),2)");
                tester.checkNull("pow(2,cast(null as double))");
            }
        };
    public final SqlFunction modFunc =
        new SqlFunction("MOD", SqlKind.Function,
            ReturnTypeInference.useNullableBiggest, null,
            OperandsTypeChecking.typeNullableIntegerInteger,
            SqlFunction.SqlFuncTypeName.Numeric) {
            public void test(SqlTester tester)
            {
                tester.checkScalarExact("mod(4,2)", "0");
                tester.checkNull("mod(cast(null as integer),2)");
                tester.checkNull("mod(4,cast(null as tinyint))");
            }
        };
    public final SqlFunction lnFunc =
        new SqlFunction("LN", SqlKind.Function,
            ReturnTypeInference.useNullableDouble, null,
            OperandsTypeChecking.typeNumeric, SqlFunction.SqlFuncTypeName.Numeric) {
            public void test(SqlTester tester)
            {
                //todo not very platform independant
                tester.checkScalarApprox("ln(2.71828)", "0.999999327347282");
                tester.checkNull("ln(cast(null as tinyint))");
            }
        };
    public final SqlFunction logFunc =
        new SqlFunction("LOG", SqlKind.Function,
            ReturnTypeInference.useNullableDouble, null,
            OperandsTypeChecking.typeNumeric, SqlFunction.SqlFuncTypeName.Numeric) {
            public void test(SqlTester tester)
            {
                tester.checkScalarApprox("log(10)", "1.0");
                tester.checkNull("log(cast(null as real))");
            }
        };
    public final SqlFunction absFunc =
        new SqlFunction("ABS", SqlKind.Function,
            ReturnTypeInference.useNullableBiggest, null,
            OperandsTypeChecking.typeNumeric, SqlFunction.SqlFuncTypeName.Numeric) {
            public void test(SqlTester tester)
            {
                tester.checkScalarExact("abs(-1)", "1");
                tester.checkNull("abs(cast(null as double))");
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
                    throw EigenbaseResource.instance().newInvalidNbrOfArgument(
                        name,
                        pos.toString(),
                        new Integer(2));
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
                tester.checkNull("nullif(1,1)");
                tester.checkString("nullif('a','b')", "a");

                //todo renable when type checking for case is fixe
                //                    tester.checkString("nullif('a',cast(null as varchar))", "a");
                //                    tester.checkNull("nullif(cast(null as varchar),'a')");
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
                tester.checkString("coalesce('a','b')", "a");
                tester.checkScalarExact("coalesce(null,null,3)", "3");
            }
        };

    /** The <code>USER</code> function. */
    public final SqlFunction userFunc = new SqlAbstractUserFunction("USER") {
        public void test(SqlTester tester)
        {
            tester.checkScalar("USER", null, "VARCHAR(30) NOT NULL");
        }
    };

    /** The <code>CURRENT_USER</code> function. */
    public final SqlFunction currentUserFunc = new SqlAbstractUserFunction(
            "CURRENT_USER") {
        public void test(SqlTester tester)
        {
            tester.checkScalar("CURRENT_USER", null, "VARCHAR(30) NOT NULL");
        }
    };

    /** The <code>SESSION_USER</code> function. */
    public final SqlFunction sessionUserFunc = new SqlAbstractUserFunction(
            "SESSION_USER") {
        public void test(SqlTester tester)
        {
            tester.checkScalar("SESSION_USER", null, "VARCHAR(30) NOT NULL");
        }
    };


    /** The <code>SYSTEM_USER</code> function. */
    public final SqlFunction systemUserFunc = new SqlAbstractUserFunction(
            "SYSTEM_USER") {
        public void test(SqlTester tester)
        {
            String user = System.getProperty("user.name"); // e.g. "jhyde"
            tester.checkScalar("SYSTEM_USER", user, "VARCHAR(30) NOT NULL");
        }
    };

    /** The <code>CURRENT_PATH</code> function. */
    public final SqlFunction currentPathFunc = new SqlAbstractUserFunction(
            "CURRENT_PATH") {
        public void test(SqlTester tester)
        {
            tester.checkScalar("CURRENT_PATH", "", "VARCHAR(30) NOT NULL");
        }
    };

    /** The <code>CURRENT_ROLE</code> function. */
    public final SqlFunction currentRoleFunc = new SqlAbstractUserFunction(
            "CURRENT_ROLE") {
        public void test(SqlTester tester)
        {
            // We don't have roles yet, so the CURRENT_ROLE function returns
            // the empty string.
            tester.checkScalar("CURRENT_ROLE", "", "VARCHAR(30) NOT NULL");
        }
    };

    /** The <code>LOCALTIME [(<i>precision</i>)]</code> function. */
    public final SqlFunction localTimeFunc = new SqlAbstractTimeFunction(
            "LOCALTIME", SqlTypeName.Time) {
        public void test(SqlTester tester)
        {
            tester.checkScalar("LOCALTIME", timePattern, "TIME(0) NOT NULL");
            //TODO: tester.checkFails("LOCALTIME()", "?", SqlTypeName.Time);
            tester.checkScalar("LOCALTIME(1)", timePattern,
                "TIME(1) NOT NULL");
        }

    };

    /** The <code>LOCALTIMESTAMP [(<i>precision</i>)]</code> function. */
    public final SqlFunction localTimestampFunc = new SqlAbstractTimeFunction(
            "LOCALTIMESTAMP", SqlTypeName.Timestamp) {
        public void test(SqlTester tester) {
            tester.checkScalar("LOCALTIMESTAMP", timestampPattern,
                "TIMESTAMP(0) NOT NULL");
            tester.checkFails("LOCALTIMESTAMP()", "?");
            tester.checkScalar("LOCALTIMESTAMP(1)", timestampPattern,
                "TIMESTAMP(1) NOT NULL");
        }
    };

    /** The <code>CURRENT_TIME [(<i>precision</i>)]</code> function. */
    public final SqlFunction currentTimeFunc = new SqlAbstractTimeFunction(
            "CURRENT_TIME", SqlTypeName.Time) {
        public void test(SqlTester tester) {
            tester.checkScalar("CURRENT_TIME", timePattern,
                "TIME(0) NOT NULL");
            tester.checkFails("CURRENT_TIME()", "?");
            tester.checkScalar("CURRENT_TIME(1)", timePattern,
                "TIME(1) NOT NULL");
        }
    };

    /** The <code>CURRENT_TIMESTAMP [(<i>precision</i>)]</code> function. */
    public final SqlFunction currentTimestampFunc = new SqlAbstractTimeFunction(
            "CURRENT_TIMESTAMP", SqlTypeName.Timestamp) {
        public void test(SqlTester tester) {
            tester.checkScalar("CURRENT_TIMESTAMP", timestampPattern,
                "TIMESTAMP(0) NOT NULL");
            tester.checkFails("CURRENT_TIMESTAMP()", "?");
            tester.checkScalar("CURRENT_TIMESTAMP(1)", timestampPattern,
                "TIMESTAMP(1) NOT NULL");
        }
    };

    /** The <code>CURRENT_DATE</code> function. */
    public final SqlFunction currentDateFunc = new SqlFunction("CURRENT_DATE",
            SqlKind.Function, ReturnTypeInference.useDate, null, null,
            SqlFunction.SqlFuncTypeName.TimeDate) {
        public void test(SqlTester tester)
        {
            tester.checkScalar("CURRENT_DATE", datePattern, "DATE NOT NULL");
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

}


// End SqlStdOperatorTable.java

