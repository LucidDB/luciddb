/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
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
package net.sf.saffron.sql.fun;

import net.sf.saffron.core.SaffronType;
import net.sf.saffron.core.SaffronTypeFactory;
import net.sf.saffron.oj.rel.RexToJavaTranslator;
import net.sf.saffron.resource.SaffronResource;
import net.sf.saffron.rex.RexCall;
import net.sf.saffron.sql.test.SqlTester;
import net.sf.saffron.sql.type.SqlTypeName;
import net.sf.saffron.sql.fun.SqlTrimFunction;
import net.sf.saffron.sql.*;
import net.sf.saffron.util.Util;
import openjava.ptree.Expression;
import openjava.ptree.ExpressionList;
import openjava.ptree.MethodCall;

import java.util.ArrayList;
import java.util.List;

/**
 * Extension to {@link net.sf.saffron.sql.SqlOperatorTable} containing the standard
 * operators and functions.
 *
 * @author jhyde
 * @since May 28, 2004
 * @version $Id$
 **/
public class SqlStdOperatorTable extends SqlOperatorTable {

    // infix
    public final SqlBinaryOperator andOperator =
            new SqlBinaryOperator("AND", SqlKind.And, 14, true,
                    useNullableBoolean, booleanParam, typeNullableBoolBool) {
                public void test(SqlTester tester) {
                    tester.checkBoolean("true and false", "FALSE");
                    tester.checkBoolean("true and true", "TRUE");
                    tester.checkBoolean("null and false", "FALSE");
                    tester.checkBoolean("false and null", "FALSE");
                    tester.checkNull("cast(null as boolean) and true");
                }

            };

    public final SqlBinaryOperator asOperator =
            new SqlBinaryOperator("AS", SqlKind.As, 10, true,
                    useFirstArgType, useReturnForParam, typeAnyAny) {
                public void test(SqlTester tester) {
                    /* empty implementation */
                }

                protected void checkArgTypes(
                        SqlCall call, SqlValidator validator, SqlValidator.Scope scope) {
                    /* empty implementation */
                }
            };

    // FIXME jvs 23-Dec-2003:  useFirstKnownParam is incorrect here;
    // have to promote CHAR to VARCHAR
    public final SqlBinaryOperator concatOperator =
            new SqlBinaryOperator("||", SqlKind.Other, 30, true,
                    useFirstArgType, null, typeNullableStringStringOfSameType) {
                protected SaffronType _inferType(SqlValidator validator,
                        SqlValidator.Scope scope, SqlCall call) {
                    //todo: make return a string with the correct precision.
                    return useBiggest.getType(validator, scope, call);
                }


                public void test(SqlTester tester) {
                    tester.checkString(" 'a'||'b' ", "ab");
                    tester.checkString(" 'a'||'b' ", "a");
                    tester.checkString(" x'f'||x'f' ", "X'FF");
                    tester.checkString(" b'1'||b'0' ", "B'10'");
                    tester.checkString(" b'1'||b'' ", "B'1'");
                    tester.checkNull("x'ff' || cast(null as varbinary)");
                }

                protected void checkArgTypes(SqlCall call, SqlValidator validator,
                        SqlValidator.Scope scope) {
                    SqlNode op0 = call.operands[0];
                    SqlNode op1 = call.operands[1];
                    typeNullableString.checkThrows(validator, scope, call, op0, 0);
                    typeNullableString.checkThrows(validator, scope, call, op1, 0);
                    SaffronType t0 = validator.deriveType(scope, op0);
                    SaffronType t1 = validator.deriveType(scope, op1);
                    if (!t0.isSameTypeFamily(t1)) {
                        throw call.newValidationSignatureError(validator, scope);
                    }
                }
            };

    public final SqlBinaryOperator divideOperator =
            new SqlBinaryOperator("/", SqlKind.Divide, 30, true,
                    useBiggest, useFirstKnownParam, typeNumericNumeric) {
                public void test(SqlTester tester) {
                    tester.checkScalarExact("10 / 5", "2");
                    tester.checkScalarApprox("10.0 / 5", "2.0");
                    tester.checkNull("1e1 / cast(null as float)");
                }
            };

    public final SqlBinaryOperator dotOperator =
            new SqlBinaryOperator(".", SqlKind.Dot, 40, true, null, null, typeAnyAny) {
                public void test(SqlTester tester) {
                    /* empty implementation */
                }
            };

    public final SqlBinaryOperator equalsOperator =
            new SqlBinaryOperator("=", SqlKind.Equals, 15, true,
                    useNullableBoolean, useFirstKnownParam,
                    typeNullabeSameSame_or_NullableNumericNumeric_or_NullableBinariesBinaries) {
                public void test(SqlTester tester) {
                    tester.checkBoolean("1=1", "TRUE");
                    tester.checkBoolean("'a'='b'", "FALSE");
                    tester.checkNull("cast(null as boolean)=cast(null as boolean)");
                    tester.checkNull("cast(null as smallint)=1");
                }
            };

    public final SqlSetOperator exceptOperator =
            new SqlSetOperator("EXCEPT", SqlKind.Except, 9, false);

    public final SqlSetOperator exceptAllOperator =
            new SqlSetOperator("EXCEPT ALL", SqlKind.Except, 9, true);

    public final SqlBinaryOperator greaterThanOperator =
            new SqlBinaryOperator(">", SqlKind.GreaterThan, 15, true,
                    useNullableBoolean, useFirstKnownParam,
                    typeNullabeSameSame_or_NullableNumericNumeric_or_NullableBinariesBinaries) {
                public void test(SqlTester tester) {
                    tester.checkBoolean("1>2", "FALSE");
                    tester.checkBoolean("1>1", "FALSE");
                    tester.checkBoolean("2>1", "TRUE");
                    tester.checkNull("3.0>cast(null as double)");
                }
            };

    public final SqlBinaryOperator greaterThanOrEqualOperator =
            new SqlBinaryOperator(">=", SqlKind.GreaterThanOrEqual, 15, true,
                    useNullableBoolean, useFirstKnownParam,
                    typeNullabeSameSame_or_NullableNumericNumeric_or_NullableBinariesBinaries) {
                public void test(SqlTester tester) {
                    tester.checkBoolean("1>=2", "FALSE");
                    tester.checkBoolean("1>=1", "TRUE");
                    tester.checkBoolean("2>=1", "TRUE");
                    tester.checkNull("cast(null as real)>=999");
                }
            };

    public final SqlBinaryOperator inOperator =
            new SqlBinaryOperator("IN", SqlKind.In, 15, true,
                    useNullableBoolean, useFirstKnownParam, null) {
                public void test(SqlTester tester) {
                    /* empty implementation */
                }
            };


    public final SqlBinaryOperator overlapsOperator =
            new SqlBinaryOperator("OVERLAPS", SqlKind.Overlaps, 15, true,
                    useNullableBoolean, useFirstKnownParam,
                    typeNullableIntervalInterval) {
                public void test(SqlTester tester) {
                    //?todo
                }
            };

    public final SqlSetOperator intersectOperator =
            new SqlSetOperator("INTERSECT", SqlKind.Intersect, 9, false);

    public final SqlSetOperator intersectAllOperator =
            new SqlSetOperator("INTERSECT ALL", SqlKind.Intersect, 9, true);

    public final SqlBinaryOperator lessThanOperator =
            new SqlBinaryOperator("<", SqlKind.LessThan, 15, true,
                    useNullableBoolean, useFirstKnownParam,
                    typeNullabeSameSame_or_NullableNumericNumeric_or_NullableBinariesBinaries) {
                public void test(SqlTester tester) {
                    tester.checkBoolean("1<2", "TRUE");
                    tester.checkBoolean("1<1", "FALSE");
                    tester.checkBoolean("2<1", "FALSE");
                    tester.checkNull("123<cast(null as long)");
                }
            };

    public final SqlBinaryOperator lessThanOrEqualOperator =
            new SqlBinaryOperator("<=", SqlKind.LessThanOrEqual, 15, true,
                    useNullableBoolean, useFirstKnownParam,
                    typeNullabeSameSame_or_NullableNumericNumeric_or_NullableBinariesBinaries) {
                public void test(SqlTester tester) {
                    tester.checkBoolean("1<=2", "TRUE");
                    tester.checkBoolean("1<=1", "TRUE");
                    tester.checkBoolean("2<=1", "FALSE");
                    tester.checkNull("cast(null as tinyint)<=3");
                }
            };

    public final SqlBinaryOperator minusOperator =
            new SqlBinaryOperator("-", SqlKind.Minus, 20, true,
                    useBiggest, useFirstKnownParam, typeNullableNumericNumeric) {
                public void test(SqlTester tester) {
                    tester.checkScalarExact("2-1", "1");
                    tester.checkScalarApprox("2.0-1", "1.0");
                    tester.checkScalarExact("1-2", "-1");
                    tester.checkNull("1e1-cast(null as double)");
                }
            };

    public final SqlBinaryOperator multiplyOperator =
            new SqlBinaryOperator("*", SqlKind.Times, 30, true,
                    useBiggest, useFirstKnownParam, typeNullableNumericNumeric) {
                public void test(SqlTester tester) {
                    tester.checkScalarExact("2*3", "6");
                    tester.checkScalarExact("2*-3", "-6");
                    tester.checkScalarExact("2*0", "0");
                    tester.checkScalarApprox("2.0*3", "6.0");
                    tester.checkNull("2e-3*cast(null as integer)");
                }
            };

    public final SqlBinaryOperator notEqualsOperator =
            new SqlBinaryOperator("<>", SqlKind.NotEquals, 15, true,
                    useNullableBoolean, useFirstKnownParam,
                    typeNullabeSameSame_or_NullableNumericNumeric_or_NullableBinariesBinaries) {
                public void test(SqlTester tester) {
                    tester.checkBoolean("1<>1", "FALSE");
                    tester.checkBoolean("'a'<>'A'", "TRUE");
                    tester.checkNull("'a'<>cast(null as varchar)");
                }
            };

    public final SqlBinaryOperator orOperator =
            new SqlBinaryOperator("OR", SqlKind.Or, 13, true,
                    useNullableBoolean, booleanParam, typeNullableBoolBool) {
                public void test(SqlTester tester) {
                    tester.checkBoolean("true or false", "TRUE");
                    tester.checkBoolean("false or false", "FALSE");
                    tester.checkBoolean("true or null", "TRUE");
                    tester.checkNull("false or cast(null as boolean)");
                }
            };

    // todo: useFirstArgType isn't correct in general
    public final SqlBinaryOperator plusOperator =
            new SqlBinaryOperator("+", SqlKind.Plus, 20, true,
                    useBiggest, useFirstKnownParam, typeNullableNumericNumeric) {
                public void test(SqlTester tester) {
                    tester.checkScalarExact("1+2", "3");
                    tester.checkScalarApprox("1+2.0", "3.0");
                    tester.checkNull("cast(null as tinyint)+1");
                    tester.checkNull("1e-2+cast(null as double)");
                }
            };

    public final SqlSetOperator unionOperator =
            new SqlSetOperator("UNION", SqlKind.Union, 7, false);

    public final SqlSetOperator unionAllOperator =
            new SqlSetOperator("UNION ALL", SqlKind.Union, 7, true);

    public final SqlBinaryOperator isDistinctFromOperator =
            new SqlBinaryOperator("IS DISTINCT FROM", SqlKind.Other, 15, true, useNullableBoolean, useFirstKnownParam, typeAnyAny) {
                public void test(SqlTester tester) {
                    /* empty implementation */
                }
            };


    public final SqlRowOperator rowConstructor = new SqlRowOperator();

    // postfix
    public final SqlPostfixOperator descendingOperator =
            new SqlPostfixOperator("DESC", SqlKind.Descending, 10,
                    null, useReturnForParam, typeAny) {
                public void test(SqlTester tester) {
                    /* empty implementation */
                }
            };

    public final SqlPostfixOperator isNotNullOperator =
            new SqlPostfixOperator("IS NOT NULL", SqlKind.Other, 15,
                    useBoolean, booleanParam, typeAny) {
                public void test(SqlTester tester) {
                    tester.checkBoolean("true is not null", "TRUE");
                    tester.checkBoolean("null is not null", "FALSE");
                }
            };

    public final SqlPostfixOperator isNullOperator =
            new SqlPostfixOperator("IS NULL", SqlKind.IsNull, 15,
                    useBoolean, booleanParam, typeAny) {
                public void test(SqlTester tester) {
                    tester.checkBoolean("true is null", "FALSE");
                    tester.checkBoolean("null is null", "TRUE");
                }
            };


    public final SqlPostfixOperator isNotTrueOperator =
            new SqlPostfixOperator("IS NOT TRUE", SqlKind.Other, 15,
                    useBoolean, booleanParam, typeNullableBool) {
                public void test(SqlTester tester) {
                    tester.checkBoolean("true is not true", "FALSE");
                    tester.checkBoolean("false is not true", "TRUE");
                    tester.checkBoolean("nuLL is not true", "TRUE");
                }
            };

    public final SqlPostfixOperator isTrueOperator =
            new SqlPostfixOperator("IS TRUE", SqlKind.IsTrue, 15,
                    useBoolean, booleanParam, typeNullableBool) {
                public void test(SqlTester tester) {
                    tester.checkBoolean("true is true", "TRUE");
                    tester.checkBoolean("false is true", "FALSE");
                    tester.checkBoolean("null is true", "FALSE");
                }
            };

    public final SqlPostfixOperator isNotFalseOperator =
            new SqlPostfixOperator("IS NOT FALSE", SqlKind.Other, 15,
                    useBoolean, booleanParam, typeNullableBool) {
                public void test(SqlTester tester) {
                    tester.checkBoolean("false is not false", "FALSE");
                    tester.checkBoolean("true is not false", "TRUE");
                    tester.checkBoolean("null is not false", "TRUE");
                }
            };

    public final SqlPostfixOperator isFalseOperator =
            new SqlPostfixOperator("IS FALSE", SqlKind.IsFalse, 15,
                    useBoolean, booleanParam, typeNullableBool) {
                public void test(SqlTester tester) {
                    tester.checkBoolean("false is false", "TRUE");
                    tester.checkBoolean("true is false", "FALSE");
                    tester.checkBoolean("null is false", "FALSE");
                }
            };

    // prefix
    public final SqlPrefixOperator existsOperator =
            new SqlPrefixOperator("EXISTS", SqlKind.Exists, 20,
                    useBoolean, null, typeNullableBool) {
                public void test(SqlTester tester) {
                    /* empty implementation */
                }
            };

    public final SqlPrefixOperator notOperator =
            new SqlPrefixOperator("NOT", SqlKind.Not, 15,
                    useNullableBoolean, booleanParam, typeNullableBool) {
                public void test(SqlTester tester) {
                    tester.checkBoolean("not true", "FALSE");
                    tester.checkBoolean("not false", "TRUE");
                    tester.checkBoolean("not unknown", "UNKNOWN");
                    tester.checkNull("not cast(null as boolean)");
                }
            };

    public final SqlPrefixOperator prefixMinusOperator =
            new SqlPrefixOperator("-", SqlKind.MinusPrefix, 20,
                    useFirstArgType, useReturnForParam, typeNullableNumeric) {
                public void test(SqlTester tester) {
                    tester.checkScalarExact("-1", "-1");
                    tester.checkScalarApprox("-1.0", "-1.0");
                    tester.checkNull("-cast(null as integer)");
                }
            };

    public final SqlPrefixOperator prefixPlusOperator =
            new SqlPrefixOperator("+", SqlKind.PlusPrefix, 20,
                    useFirstArgType, useReturnForParam, typeNullableNumeric) {
                public void test(SqlTester tester) {
                    tester.checkScalarExact("+1", "1");
                    tester.checkScalarApprox("+1.0", "1.0");
                    tester.checkNull("+cast(null as integer)");
                }
            };

    public final SqlPrefixOperator explicitTableOperator =
            new SqlPrefixOperator("TABLE", SqlKind.ExplicitTable, 1, null, null, null) {
                public void test(SqlTester tester) {
                    /* empty implementation */
                }
            };

    // special
    public final SqlSpecialOperator valuesOperator =
            new SqlSpecialOperator("VALUES", SqlKind.Values) {
                public void unparse(
                        SqlWriter writer,
                        SqlNode[] operands,
                        int leftPrec,
                        int rightPrec) {
                    writer.print("VALUES ");
                    for (int i = 0; i < operands.length; i++) {
                        if (i > 0) {
                            writer.print(", ");
                        }
                        SqlNode operand = operands[i];
                        operand.unparse(writer, 0, 0);
                    }
                }

                public void test(SqlTester tester) {
                    tester.check("select true from values(true)", "TRUE", SqlTypeName.Boolean);
                }
            };

    public final SqlSpecialOperator betweenOperator =
            new SqlBetweenOperator("BETWEEN", SqlKind.Between) {
                public void test(SqlTester tester) {
                    tester.checkBoolean("2 between 1 and 3", "TRUE");
                    tester.checkBoolean("2 between 3 and 2", "FALSE");
                    tester.checkBoolean("2 between symmetric 3 and 2", "TRUE");
                    tester.checkBoolean("3 between 1 and 3", "TRUE");
                    tester.checkBoolean("4 between 1 and 3", "FALSE");
                    tester.checkBoolean("1 between 4 and -3", "FALSE");
                    tester.checkBoolean("1 between -1 and -3", "FALSE");
                    tester.checkBoolean("1 between -1 and 3", "TRUE");
                    tester.checkBoolean("1 between 1 and 1", "TRUE");
                    tester.checkNull("cast(null as integer) between -1 and 2");
                    tester.checkNull("1 between -1 and cast(null as integer)");
                    tester.checkNull("1 between cast(null as integer) and cast(null as integer)");
                    tester.checkNull("1 between cast(null as integer) and 1");
                }
            };

    public final SqlSpecialOperator notBetweenOperator =
            new SqlBetweenOperator("NOT BETWEEN", SqlKind.NotBetween) {
                public void test(SqlTester tester) {
                    tester.checkBoolean("2 not between 1 and 3", "FALSE");
                    tester.checkBoolean("3 not between 1 and 3", "FALSE");
                    tester.checkBoolean("4 not between 1 and 3", "TRUE");
                }
            };

    public final SqlSpecialOperator likeOperator =
            new SqlLikeOperator("LIKE", SqlKind.Like);

    public final SqlSpecialOperator similarOperator =
            new SqlLikeOperator("SIMILAR", SqlKind.Similar);

    public final SqlSelectOperator selectOperator = new SqlSelectOperator();

    public final SqlCaseOperator caseOperator = new SqlCaseOperator() {
        public void test(SqlTester tester) {
            tester.checkScalarExact("case when 'a'='a' then 1 end", "1");
            tester.checkString("case 2 when 1 then 'a' when 2 then 'b' end", "b");
            tester.checkNull("case 'a'='b' then 1 end");
            tester.checkScalarExact("case when 'a'=cast(null as varchar) then 1 else 2 end", "2");
        }
    };

    public final SqlJoinOperator joinOperator = new SqlJoinOperator();

    public final SqlSpecialOperator insertOperator =
            new SqlSpecialOperator("INSERT", SqlKind.Insert) {
                public void test(SqlTester tester) {
                    /* empty implementation */
                }
            };

    public final SqlSpecialOperator deleteOperator =
            new SqlSpecialOperator("DELETE", SqlKind.Delete) {
                public void test(SqlTester tester) {
                    /* empty implementation */
                }
            };

    public final SqlSpecialOperator updateOperator =
            new SqlSpecialOperator("UPDATE", SqlKind.Update) {
                public void test(SqlTester tester) {
                    /* empty implementation */
                }
            };

    public final SqlSpecialOperator explainOperator =
            new SqlSpecialOperator("EXPLAIN", SqlKind.Explain) {
                public void test(SqlTester tester) {
                    /* empty implementaion */
                }
            };

    public final SqlOrderByOperator orderByOperator = new SqlOrderByOperator();

    /**
     * The character substring function:
     * <code>SUBSTRING(string FROM start [FOR length])</code>.
     *
     * <p>If the length parameter is a constant, the length
     * of the result is the minimum of the length of the input
     * and that length. Otherwise it is the length of the input.<p>
     */
    public final SqlFunction substringFunc =
            new SqlFunction("SUBSTRING", SqlKind.Function, SqlOperatorTable.useFirstArgType, null,
                    null, SqlFunction.SqlFuncTypeName.String) {

                public String getAllowedSignatures() {
                    StringBuffer ret = new StringBuffer();
                    for (int i = 0; i < SqlOperatorTable.stringTypes.length; i++) {

                        if (i > 0) {
                            ret.append(NL);
                        }
                        ArrayList list = new ArrayList();
                        list.add(SqlOperatorTable.stringTypes[i]);
                        list.add(SqlTypeName.Integer);
                        ret.append(this.getSignature(list));
                        ret.append(NL);
                        list.add(SqlTypeName.Integer);
                        ret.append(this.getSignature(list));
                    }
                    return ret.toString();
                }

                protected void checkArgTypes(SqlCall call, SqlValidator validator,
                        SqlValidator.Scope scope) {
                    int n = call.operands.length;
                    assert(3 == n || 2 == n);
                    SqlOperatorTable.typeNullableString.
                            checkThrows(validator, scope, call, call.operands[0], 0);
                    if (2 == n) {
                        SqlOperatorTable.typeNullableNumeric.
                                checkThrows(validator, scope, call, call.operands[1], 0);
                    } else {
                        SaffronType t1 = validator.deriveType(scope, call.operands[1]);
                        SaffronType t2 = validator.deriveType(scope, call.operands[2]);

                        if (t1.isCharType()) {
                            SqlOperatorTable.typeNullableString.
                                    checkThrows(validator, scope, call, call.operands[1], 0);
                            SqlOperatorTable.typeNullableString.
                                    checkThrows(validator, scope, call, call.operands[2], 0);

                            isCharTypeComparableThrows(validator, scope, call.operands);

                            //todo length of escape must be 1, can check here or must do in calc?
                        } else {
                            SqlOperatorTable.typeNullableNumeric.
                                    checkThrows(validator, scope, call, call.operands[1], 0);
                            SqlOperatorTable.typeNullableNumeric.
                                    checkThrows(validator, scope, call, call.operands[2], 0);
                        }

                        if (!t1.isSameTypeFamily(t2)) {
                            throw call.newValidationSignatureError(validator, scope);
                        }
                    }
                }

                public List getPossibleNumOfOperands() {
                    List ret = new ArrayList(2);
                    ret.add(new Integer(2));
                    ret.add(new Integer(3));
                    return ret;
                }

                public int getNumOfOperands(int disiredCount) {
                    if (2 == disiredCount) {
                        return disiredCount;
                    }
                    return 3;
                }

                protected void checkNumberOfArg(SqlCall call) {
                    int n = call.operands.length;
                    if (n > 3 || n < 2) {
                        throw Util.newInternal("todo: Wrong number of arguments to " + call);
                    }
                    ;
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

                    if (3 == operands.length) {
                        writer.print(" FOR ");
                        operands[2].unparse(writer, leftPrec, rightPrec);
                    }

                    writer.print(")");
                }

                public void test(SqlTester tester) {
                    tester.checkString("substring('abc' from 1 for 2)", "'ab'");
                    tester.checkString("substring('abc' from 2)", "'bc'");
                    tester.checkString("substring('foobar' from '%#\"o_b#\"%' for '#')", "'oob'");
                    tester.checkNull("substring(cast(null as varchar),1,2)");
                }

            };
    //~ CONVERT ---------------
    public final SqlFunction convertFunc =
            new SqlFunction("CONVERT", SqlKind.Function, null, null, null, SqlFunction.SqlFuncTypeName.String) {

                public void unparse(SqlWriter writer, SqlNode[] operands,
                        int leftPrec, int rightPrec) {
                    writer.print(name);
                    writer.print("(");
                    operands[0].unparse(writer, leftPrec, rightPrec);
                    writer.print(" USING ");
                    operands[1].unparse(writer, leftPrec, rightPrec);
                    writer.print(")");
                }

                public void test(SqlTester tester) {
                    //todo: implement when convert exist in the calculator
                }
            };
    //~ TRANSLATE ---------------
    public final SqlFunction translateFunc =
            new SqlFunction("TRANSLATE", SqlKind.Function, null, null, null, SqlFunction.SqlFuncTypeName.String) {

                public void unparse(SqlWriter writer, SqlNode[] operands,
                        int leftPrec, int rightPrec) {
                    writer.print(name);
                    writer.print("(");
                    operands[0].unparse(writer, leftPrec, rightPrec);
                    writer.print(" USING ");
                    operands[1].unparse(writer, leftPrec, rightPrec);
                    writer.print(")");
                }

                public void test(SqlTester tester) {
                    //todo: implement when translate exist in the calculator
                }
            };
    //~ OVERLAY ---------------
    public final SqlFunction overlayFunc =
            new SqlFunction("OVERLAY", SqlKind.Function, null, null, null, SqlFunction.SqlFuncTypeName.String) {

                public SaffronType getType(SaffronTypeFactory typeFactory,
                        SaffronType[] argTypes) {
                    int n = argTypes.length;
                    assert(3 == n || 4 == n);

                    SaffronType ret;
                    if (argTypes[0].isCharType()) {
                        if (!isCharTypeComparable(argTypes, 0, 1)) {
                            throw SaffronResource.instance().newValidationError(
                                    argTypes[0].toString() +
                                    " is not comparable to " +
                                    argTypes[1].toString());
                        }


                        SqlCollation picked =
                                SqlCollation.getCoercibilityDyadicOperator(
                                        argTypes[0].getCollation(),
                                        argTypes[1].getCollation());
                        if (argTypes[0].getCollation().equals(picked)) {
                            //todo this is not correct.
                            //must return the right precision also
                            ret = argTypes[0];
                        } else if (argTypes[1].getCollation().equals(picked)) {
                            //todo this is not correct.
                            //must return the right precision also
                            ret = argTypes[1];
                        } else {
                            throw Util.newInternal("should never come here");
                        }
                    } else {
                        //todo this is not correct.
                        //must return the right precision also
                        ret = argTypes[0];
                    }

                    return ret;
                }

                public SaffronType getType(SqlValidator validator,
                        SqlValidator.Scope scope, SqlCall call) {
                    checkArgTypes(call, validator, scope);

                    int n = call.operands.length;
                    assert(3 == n || 4 == n);

                    SaffronType[] argTypes = new SaffronType[n];

                    for (int i = 0; i < n; i++) {
                        argTypes[i] = validator.deriveType(scope, call.operands[i]);
                    }

                    return getType(validator.typeFactory, argTypes);
                }

                public int getNumOfOperands(int disiredCount) {
                    switch (disiredCount) {
                    case 3:
                        return 3;
                    case 4:
                        return 4;
                    default:
                        return 4;
                    }
                }

                public List getPossibleNumOfOperands() {
                    List ret = new ArrayList(2);
                    ret.add(new Integer(3));
                    ret.add(new Integer(4));
                    return ret;
                }

                protected void checkArgTypes(SqlCall call, SqlValidator validator,
                        SqlValidator.Scope scope) {
                    switch (call.operands.length) {
                    case 3:
                        SqlOperatorTable.typeNullableStringStringNotNullableInt.
                                check(validator, scope, call);
                        break;
                    case 4:
                        SqlOperatorTable.typeNullableStringStringNotNullableIntInt.
                                check(validator, scope, call);
                        break;
                    default:
                        throw Util.needToImplement(this);
                    }
                }

                public void unparse(SqlWriter writer, SqlNode[] operands,
                        int leftPrec, int rightPrec) {
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

                public void test(SqlTester tester) {
                    tester.checkString("overlay('ABCdef' placing 'abc' from 1)", "'abcdef'");
                    tester.checkString("overlay('ABCdef' placing 'abc' from 1 for 2)", "'abcCdef'");
                    tester.checkNull("overlay('ABCdef' placing 'abc' from 1 for cast(null as integer))");
                    tester.checkNull("overlay(cast(null as varchar) placing 'abc' from 1)");
                    tester.checkNull("overlay(x'abc' placing x'abc' from cast(null as integer))");
                    tester.checkNull("overlay(b'1' placing cast(null as bit) from 1)");
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
            new SqlFunction("POSITION", SqlKind.Function, SqlOperatorTable.useInteger, null,
                    SqlOperatorTable.typeNullableStringString,
                    SqlFunction.SqlFuncTypeName.Numeric) {
                public void unparse(SqlWriter writer, SqlNode[] operands,
                        int leftPrec, int rightPrec) {
                    writer.print(name);
                    writer.print("(");
                    operands[0].unparse(writer, leftPrec, rightPrec);
                    writer.print(" IN ");
                    operands[1].unparse(writer, leftPrec, rightPrec);
                    writer.print(")");
                }

                protected void checkArgTypes(SqlCall call,
                        SqlValidator validator,
                        SqlValidator.Scope scope) {
                    //check that the two operands are of same type.
                    SaffronType type0 =
                            validator.getValidatedNodeType(call.operands[0]);
                    SaffronType type1 =
                            validator.getValidatedNodeType(call.operands[1]);
                    if (!type0.isSameTypeFamily(type1) &&
                            !type1.isSameTypeFamily(type0)) {
                        throw call.newValidationSignatureError(
                                validator, scope);
                    }

                    argTypeInference.check(validator, scope, call);
                }

                public void test(SqlTester tester) {
                    tester.checkScalarExact("position('b' in 'abc'", "2");
                    tester.checkScalarExact("position(b'10' in b'0010'", "3");
                    tester.checkNull("position(cast(null as bit) in b'0010')");
                    tester.checkNull("position('a' in cast(null as char))");
                }
            };
    //~ CHAR LENGTH ---------------
    public final SqlFunction charLengthFunc =
            new SqlFunction("CHAR_LENGTH", SqlKind.Function, SqlOperatorTable.useInteger, null,
                    SqlOperatorTable.typeNullableVarchar,
                    SqlFunction.SqlFuncTypeName.Numeric) {
                public void test(SqlTester tester) {
                    tester.checkScalarExact("char_length('abc')", "3");
                    tester.checkNull("char_length(cast(null as varchar))");
                }
            };
    public final SqlFunction characterLengthFunc =
            new SqlFunction("CHARACTER_LENGTH", SqlKind.Function, SqlOperatorTable.useInteger, null,
                    SqlOperatorTable.typeNullableVarchar,
                    SqlFunction.SqlFuncTypeName.Numeric) {
                public void test(SqlTester tester) {
                    tester.checkScalarExact("CHARACTER_LENGTH('abc')", "3");
                    tester.checkNull("CHARACTER_LENGTH(cast(null as varchar))");
                }
            };
    //~ UPPER ---------------
    public final SqlFunction upperFunc =
            new SqlFunction("UPPER", SqlKind.Function, SqlOperatorTable.useFirstArgType, null,
                    SqlOperatorTable.typeNullableVarchar,
                    SqlFunction.SqlFuncTypeName.String) {
                public SqlOperator.JavaRexImplementor getJavaImplementor() {
                    return new SqlOperator.JavaRexImplementor() {
                        public Expression translateToJava(RexCall call,
                                Expression[] operands,
                                RexToJavaTranslator translator) {
                            assert call.op == upperFunc;
                            assert call.operands.length == 1;
                            Expression expr = translator.go(call.operands[0]);
                            expr = translator.convertToJava(expr, String.class);
                            return new MethodCall(expr, "toUpper", new ExpressionList());
                        }
                    };
                }

                public void test(SqlTester tester) {
                    tester.checkString("upper('a')", "'A'");
                    tester.checkString("upper('A')", "'A'");
                    tester.checkString("upper('1')", "'1'");
                    tester.checkNull("upper(cast(null as varchar))");
                }
            };
    //~ LOWER ---------------
    public final SqlFunction lowerFunc =
            new SqlFunction("LOWER", SqlKind.Function, SqlOperatorTable.useFirstArgType, null,
                    SqlOperatorTable.typeNullableVarchar,
                    SqlFunction.SqlFuncTypeName.String) {
                public void test(SqlTester tester) {
                    tester.checkString("lower('A')", "'a'");
                    tester.checkString("lower('a')", "'a'");
                    tester.checkString("lower('1')", "'1'");
                    tester.checkNull("lower(cast(null as varchar))");
                }
            };
    //~ INIT CAP ---------------
    public final SqlFunction initcapFunc =
            new SqlFunction("INITCAP", SqlKind.Function, SqlOperatorTable.useFirstArgType, null,
                    SqlOperatorTable.typeNullableVarchar,
                    SqlFunction.SqlFuncTypeName.String) {
                public void test(SqlTester tester) {
                    tester.checkString("initcap('aA')", "'Aa'");
                    tester.checkString("initcap('Aa')", "'Aa'");
                    tester.checkString("initcap('1a')", "'1a'");
                    tester.checkNull("initcap(cast(null as varchar))");
                }
            };
    //~ POWER ---------------
    /**
     * Uses SqlOperatorTable.useDouble for its return type since we dont know
     * what the result type will be by just looking at the operand types.
     * For example POW(int, int) can return a non integer if the second operand
     * is negative.
     */
    public final SqlFunction powFunc =
            new SqlFunction("POW", SqlKind.Function, SqlOperatorTable.useDouble, null,
                    SqlOperatorTable.typeNumericNumeric,
                    SqlFunction.SqlFuncTypeName.Numeric) {
                public void test(SqlTester tester) {
                    tester.checkScalarApprox("power(2,-2)", "0.5");
                    tester.checkNull("power(cast(null as integer),2)");
                    tester.checkNull("power(2,cast(null as double))");
                }
            };
    //~ MOD ---------------
    public final SqlFunction modFunc =
            new SqlFunction("MOD", SqlKind.Function, SqlOperatorTable.useBiggest, null,
                    SqlOperatorTable.typeNumericNumeric,
                    SqlFunction.SqlFuncTypeName.Numeric) {
                public void test(SqlTester tester) {
                    tester.checkScalarExact("mod(4,2)", "0");
                    tester.checkNull("mod(cast(null as integer),2)");
                    tester.checkNull("mod(4,cast(null as float))");
                }
            };
    //~ LOGARITHMS ---------------
    public final SqlFunction lnFunc =
            new SqlFunction("LN", SqlKind.Function, SqlOperatorTable.useDouble, null,
                    SqlOperatorTable.typeNumeric, SqlFunction.SqlFuncTypeName.Numeric) {
                public void test(SqlTester tester) {
                    tester.checkScalarApprox("ln(2.71828)", "1.0");
                    tester.checkNull("ln(cast(null as tinyint))");
                }
            };
    public final SqlFunction logFunc =
            new SqlFunction("LOG", SqlKind.Function, SqlOperatorTable.useDouble, null,
                    SqlOperatorTable.typeNumeric, SqlFunction.SqlFuncTypeName.Numeric) {
                public void test(SqlTester tester) {
                    tester.checkScalarApprox("log(10)", "1.0");
                    tester.checkNull("log(cast(null as real))");
                }
            };
    //~ ABS VALUE ---------------
    public final SqlFunction absFunc =
            new SqlFunction("ABS", SqlKind.Function, SqlOperatorTable.useBiggest, null,
                    SqlOperatorTable.typeNumeric, SqlFunction.SqlFuncTypeName.Numeric) {
                public void test(SqlTester tester) {
                    tester.checkScalarExact("abs(-1)", "1");
                    tester.checkNull("abs(cast(null as double))");
                }
            };
    //~ NULLIF - Conditional Expression ---------------
    public final SqlFunction nullIfFunc =
            new SqlFunction("NULLIF", SqlKind.Function, null, null, null, SqlFunction.SqlFuncTypeName.System) {
                public SqlCall createCall(SqlNode[] operands) {
                    SqlNodeList whenList = new SqlNodeList();
                    SqlNodeList thenList = new SqlNodeList();
                    whenList.add(operands[1]);
                    thenList.add(SqlLiteral.createNull());
                    return caseOperator.createCall(
                            operands[0], whenList, thenList, operands[0]);
                }

                public int getNumOfOperands(int disiredCount) {
                    return 2;
                }

                public void test(SqlTester tester) {
                    tester.checkNull("nullif(1,1)");
                    tester.checkString("nullif('a','b')", "'a'");
                    tester.checkString("nullif('a',null)", "'a'");
                    tester.checkNull("nullif(cast(null as varchar),'a')");
                }
            };
    //~ COALESCE - Conditional Expression ---------------
    public final SqlFunction coalesceFunc =
            new SqlFunction("COALESCE", SqlKind.Function, null, null, null,
                    SqlFunction.SqlFuncTypeName.System) {
                public SqlCall createCall(SqlNode[] operands) {
                    Util.pre(operands.length >= 2, "operands.length>=2");
                    return createCall(operands, 0);
                }

                private SqlCall createCall(SqlNode[] operands, int start) {
                    SqlNodeList whenList = new SqlNodeList();
                    SqlNodeList thenList = new SqlNodeList();
                    whenList.add(
                            isNotNullOperator.createCall(operands[start]));
                    thenList.add(operands[start]);
                    if (2 == (operands.length - start)) {
                        return caseOperator.createCall(
                                null, whenList, thenList, operands[start + 1]);
                    }
                    return caseOperator.createCall(
                            null, whenList, thenList,
                            this.createCall(operands, start + 1));
                }

                public int getNumOfOperands(int disiredCount) {
                    return 2;
                }

                public void test(SqlTester tester) {
                    tester.checkString("coalesce('a','b'", "'a'");
                    tester.checkScalarExact("coalesce(null,null,3", "3");
                }
            };
    public final SqlFunction localTimeFunc =
            new SqlFunction("LOCALTIME", SqlKind.Function, null, null,
                    SqlOperatorTable.typePositiveIntegerLiteral, SqlFunction.SqlFuncTypeName.TimeDate) {
                protected SaffronType inferType(SqlValidator validator, SqlValidator.Scope scope, SqlCall call) {
                    SqlLiteral literal = (SqlLiteral) call.operands[0];
                    int precision = literal.intValue();
                    if (precision < 0) {
                        throw SaffronResource.instance().newArgumentMustBePositiveInteger(call.operator.name);
                    }
                    return validator.typeFactory.createSqlType(SqlTypeName.Time, precision);
                }

                public SaffronType getType(SaffronTypeFactory typeFactory,
                        SaffronType[] argTypes) {
                    int precision = 0; // todo: access the value of the literal parmaeter
                    return typeFactory.createSqlType(SqlTypeName.Time, precision);
                }

                protected void checkNumberOfArg(SqlCall call) {
                    if (call.getOperands().length > 1) {
                        throw Util.newInternal("todo: function takes 0 or 1 parameters");
                    }
                }

                public void test(SqlTester tester) {
                    //todo: tester.check("","");
                }
            };
    public final SqlFunction localTimestampFunc =
            new SqlFunction("LOCALTIMESTAMP", SqlKind.Function, null, null,
                    SqlOperatorTable.typePositiveIntegerLiteral,
                    SqlFunction.SqlFuncTypeName.TimeDate) {
                protected SaffronType inferType(SqlValidator validator,
                        SqlValidator.Scope scope, SqlCall call) {
                    SqlLiteral literal = (SqlLiteral) call.operands[0];
                    int precision = literal.intValue();
                    if (precision < 0) {
                        throw SaffronResource.instance().newArgumentMustBePositiveInteger(call.operator.name);
                    }
                    return validator.typeFactory.createSqlType(
                            SqlTypeName.Timestamp, precision);
                }

                public SaffronType getType(SaffronTypeFactory typeFactory,
                        SaffronType[] argTypes) {
                    int precision = 0; // todo: access the value of the literal parmaeter
                    return typeFactory.createSqlType(
                            SqlTypeName.Time, precision);
                }

                protected void checkNumberOfArg(SqlCall call) {
                    if (call.getOperands().length > 1) {
                        throw Util.newInternal("todo: function takes 0 or 1 parameters");
                    }
                }

                public void test(SqlTester tester) {
                    //todo tester.check("","");
                }
            };
    public final SqlFunction currentTimeFunc =
            new SqlFunction("CURRENT_TIME", SqlKind.Function, null, null,
                    SqlOperatorTable.typePositiveIntegerLiteral,
                    SqlFunction.SqlFuncTypeName.TimeDate) {
                protected SaffronType inferType(SqlValidator validator,
                        SqlValidator.Scope scope, SqlCall call) {
                    SqlLiteral literal = (SqlLiteral) call.operands[0];
                    int precision = literal.intValue();
                    if (precision < 0) {
                        throw SaffronResource.instance().newArgumentMustBePositiveInteger(call.operator.name);
                    }
                    return validator.typeFactory.createSqlType(SqlTypeName.Time, precision);
                }

                public SaffronType getType(SaffronTypeFactory typeFactory,
                        SaffronType[] argTypes) {
                    int precision = 0; // todo: access the value of the literal parmaeter
                    return typeFactory.createSqlType(SqlTypeName.Time, precision);
                }

                protected void checkNumberOfArg(SqlCall call) {
                    if (call.getOperands().length > 1) {
                        throw Util.newInternal("todo: function takes 0 or 1 parameters");
                    }
                }

                public void test(SqlTester tester) {
                    //todo tester.check("","");
                }
            };
    public final SqlFunction currentTimestampFunc =
            new SqlFunction("CURRENT_TIMESTAMP", SqlKind.Function, null, null,
                    SqlOperatorTable.typePositiveIntegerLiteral,
                    SqlFunction.SqlFuncTypeName.TimeDate) {
                protected SaffronType inferType(SqlValidator validator,
                        SqlValidator.Scope scope, SqlCall call) {
                    SqlLiteral literal = (SqlLiteral) call.operands[0];
                    int precision = literal.intValue();
                    if (precision < 0) {
                        throw SaffronResource.instance().newArgumentMustBePositiveInteger(call.operator.name);
                    }
                    return validator.typeFactory.createSqlType(SqlTypeName.Timestamp, precision);
                }

                public SaffronType getType(SaffronTypeFactory typeFactory,
                        SaffronType[] argTypes) {
                    int precision = 0; // todo: access the value of the literal parmaeter
                    return typeFactory.createSqlType(SqlTypeName.Timestamp,
                            precision);
                }

                protected void checkNumberOfArg(SqlCall call) {
                    if (call.getOperands().length > 1) {
                        throw Util.newInternal("todo: function takes 0 or 1 parameters");
                    }
                }

                public void test(SqlTester tester) {
                    //todo tester.check("","");
                }
            };
    public final SqlFunction currentDateFunc =
            new SqlFunction("CURRENT_DATE", SqlKind.Function, null, null,
                    null, SqlFunction.SqlFuncTypeName.TimeDate) {
                protected SaffronType inferType(SqlValidator validator,
                        SqlValidator.Scope scope, SqlCall call) {
                    return validator.typeFactory.createSqlType(SqlTypeName.Date);
                }

                public SaffronType getType(SaffronTypeFactory typeFactory,
                        SaffronType[] argTypes) {
                    int precision = 0; // todo: access the value of the literal parmaeter
                    return typeFactory.createSqlType(SqlTypeName.Time, precision);
                }

                protected void checkArgTypes(SqlCall call, SqlValidator validator, SqlValidator.Scope scope) {
                    if (call.operands.length != 0) {
                        throw call.newValidationSignatureError(validator, scope);
                    }
                }

                protected void checkNumberOfArg(SqlCall call) {
                    if (call.getOperands().length > 0) {
                        throw Util.newInternal("todo: function takes no parameters");
                    }
                }
                // no argTypeInference, so must override these methods.  Probably need a niladic version of that.

                public List getPossibleNumOfOperands() {
                    List ret = new ArrayList(1);
                    ret.add(new Integer(0));
                    return ret;
                }

                public void test(SqlTester tester) {
                    //todo tester.check("","");
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
