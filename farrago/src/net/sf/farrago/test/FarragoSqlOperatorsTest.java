/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
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
package net.sf.farrago.test;

import junit.framework.Test;
import junit.framework.TestSuite;
import net.sf.farrago.test.regression.FarragoCalcSystemTest;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.SqlOperatorTable;
import org.eigenbase.sql.SqlSyntax;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.test.AbstractSqlTester;
import org.eigenbase.sql.test.SqlOperatorIterator;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.Util;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.HashMap;
import java.util.regex.Pattern;


/**
 * FarragoSqlOperatorsTest contains an implementation of {@link AbstractSqlTester}.
 * It uses the visitor pattern to vist all SqlOperators for unit test purposes.
 *
 * @author Wael Chatila
 * @since May 25, 2004
 * @version $Id$
 */
public class FarragoSqlOperatorsTest extends FarragoTestCase
{
    private static final SqlStdOperatorTable opTab = SqlOperatorTable.std();
    private static final boolean bug260fixed = false;

    //~ Instance fields -------------------------------------------------------

    private FarragoCalcSystemTest.VirtualMachine vm;
    private SqlOperator operator;
    private HashMap operatorTestCases;

    //~ Constructors ----------------------------------------------------------

    public FarragoSqlOperatorsTest(
        FarragoCalcSystemTest.VirtualMachine vm,
        SqlOperator operator,
        String testName)
        throws Exception
    {
        super(testName);
        this.vm = vm;
        this.operator = operator;
        this.operatorTestCases = new HashMap();
        loadTests();
    }

    //~ Methods ---------------------------------------------------------------

    public static Test suite()
        throws Exception
    {
        TestSuite suite = new TestSuite();
        addTests(suite, FarragoCalcSystemTest.VirtualMachine.Auto);
        addTests(suite, FarragoCalcSystemTest.VirtualMachine.Fennel);
        addTests(suite, FarragoCalcSystemTest.VirtualMachine.Java);

        return wrappedSuite(suite);
    }

    private static void addTests(TestSuite suite,
        FarragoCalcSystemTest.VirtualMachine vm)
        throws Exception
    {
        Iterator operatorsIt = new SqlOperatorIterator();
        while (operatorsIt.hasNext()) {
            SqlOperator op = (SqlOperator) operatorsIt.next();
            String testName = "SQL-TESTER-" + op.name + "-";
            if (!vm.canImplement(op)) {
                continue;
            }
            if (!bug260fixed) {
                if (op == opTab.orOperator ||
                    op == opTab.andOperator ||
                    op == opTab.isFalseOperator ||
                    op == opTab.litChainOperator ||
                    op == opTab.multiplyOperator ||
                    op == opTab.localTimeFunc ||
                    op == opTab.localTimestampFunc) {
                    continue;
                }
            }
            suite.addTest(
                new FarragoSqlOperatorsTest(vm,
                    op, testName + vm.name));
        }
    }

    protected void setUp()
        throws Exception
    {
        super.setUp();
        stmt.execute(vm.getAlterSystemCommand());
    }

    protected void runTest()
        throws Throwable
    {
        if (!operatorTestCases.containsKey(operator)) {
            fail("test not defined for "+operator.name);
        }

        SqlOperatorTestCase testCase =
            (SqlOperatorTestCase) operatorTestCases.get(operator);
        testCase.test(new FarragoSqlTester(operator));
    }

    //~ Inner Classes & Interfaces -----------------------------------------

    private interface SqlOperatorTestCase {
        void test(SqlTester tester);
    }

    /**
     * Implementation of {@link AbstractSqlTester}, leveraging connection setup
     * and result set comparing from the class {@link FarragoTestCase}
     */
    private class FarragoSqlTester extends AbstractSqlTester
    {
        /** The name of the operator which should be the same as its syntax */
        SqlOperator operator;

        FarragoSqlTester(SqlOperator op)
        {
            operator = op;
        }

        public void checkFails(
            String expression,
            String expectedError)
        {
            if (bug260fixed) {
                // todo: implement this
                throw Util.needToImplement(this);
            }
        }

        public void checkType(
            String expression,
            String type)
        {
            if (bug260fixed) {
                // todo: implement this
                throw Util.needToImplement(this);
            }
        }

        public void check(
            String query,
            Object result,
            SqlTypeName resultType)
        {
            try {
                if (operator.getSyntax() != SqlSyntax.Internal) {
                    // check that query really contains a call to the operator we
                    // are looking at
                    String queryCmp = query.toUpperCase();
                    String opNameCmp = operator.name.toUpperCase();
                    if (queryCmp.indexOf(opNameCmp) < 0) {
                        fail("Not exercising operator <" + operator + "> "
                            + "with the query <" + query + ">");
                    }
                }

                resultSet = stmt.executeQuery(query);
                if (result instanceof Pattern) {
                    compareResultSetWithPattern((Pattern) result);
                } else {
                    Set refSet = new HashSet();
                    refSet.add(result);
                    compareResultSet(refSet);
                }
                stmt.close();
                stmt = connection.createStatement();
            } catch (Throwable e) {
                RuntimeException newException =
                    new RuntimeException("Exception occured while testing "
                    + operator + ". " + "Exception msg = "
                    + e.getMessage());
                newException.setStackTrace(e.getStackTrace());
                throw newException;
            }
        }
    }

    private void loadTests() {
        SqlOperatorTestCase emptyTest = new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                /* empty implementation */
            };};
        SqlOperatorTestCase todoTest = emptyTest;
        SqlOperatorTestCase notBetweenTest = new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkBoolean("2 not between 1 and 3", Boolean.FALSE);
                tester.checkBoolean("3 not between 1 and 3", Boolean.FALSE);
                tester.checkBoolean("4 not between 1 and 3", Boolean.TRUE);
            };};

        SqlOperatorTestCase betweenTest = new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkBoolean("2 between 1 and 3", Boolean.TRUE);
                tester.checkBoolean("2 between 3 and 2", Boolean.FALSE);
                tester.checkBoolean("2 between symmetric 3 and 2", Boolean.TRUE);
                tester.checkBoolean("3 between 1 and 3", Boolean.TRUE);
                tester.checkBoolean("4 between 1 and 3", Boolean.FALSE);
                tester.checkBoolean("1 between 4 and -3", Boolean.FALSE);
                tester.checkBoolean("1 between -1 and -3", Boolean.FALSE);
                tester.checkBoolean("1 between -1 and 3", Boolean.TRUE);
                tester.checkBoolean("1 between 1 and 1", Boolean.TRUE);
                tester.checkBoolean("x'' between x'' and x''", Boolean.TRUE);
                tester.checkNull("cast(null as integer) between -1 and 2");
                tester.checkNull("1 between -1 and cast(null as integer)");
                tester.checkNull(
                    "1 between cast(null as integer) and cast(null as integer)");
                tester.checkNull("1 between cast(null as integer) and 1");

            };};

        /**
         * Regular expression for a SQL TIME(0) value.
         */
        final Pattern timePattern = Pattern.compile(
            "[0-9][0-9]:[0-9][0-9]:[0-9][0-9]");

        /**
         * Regular expression for a SQL TIMESTAMP(3) value.
         */
        final Pattern timestampPattern = Pattern.compile(
            "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] " +
            "[0-9][0-9]:[0-9][0-9]:[0-9][0-9]\\.[0-9]+");

        /**
         * Regular expression for a SQL DATE value.
         */
        final Pattern datePattern = Pattern.compile(
            "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]");

        //--------------------------------

        operatorTestCases.put(opTab.andOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkBoolean("true and false", Boolean.FALSE);
                tester.checkBoolean("true and true", Boolean.TRUE);
                tester.checkBoolean("cast(null as boolean) and false",
                    Boolean.FALSE);
                tester.checkBoolean("false and cast(null as boolean)",
                    Boolean.FALSE);
                tester.checkNull("cast(null as boolean) and true");
            };});

        operatorTestCases.put(opTab.asOperator, emptyTest);

        operatorTestCases.put(opTab.concatOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkString(" 'a'||'b' ", "ab");

                //not yet implemented
                //                    tester.checkString(" x'f'||x'f' ", "X'FF")
                //                    tester.checkString(" b'1'||b'0' ", "B'10'");
                //                    tester.checkString(" b'1'||b'' ", "B'1'");
                //                    tester.checkNull("x'ff' || cast(null as varbinary)");

            };});

        operatorTestCases.put(opTab.divideOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester)
            {
                tester.checkScalarExact("10 / 5", "2");
                tester.checkScalarApprox("10.0 / 5", "2.0");
                tester.checkNull("1e1 / cast(null as float)");
            };});

        operatorTestCases.put(opTab.dotOperator, emptyTest);

        operatorTestCases.put(opTab.equalsOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkBoolean("1=1", Boolean.TRUE);
                tester.checkBoolean("'a'='b'", Boolean.FALSE);
                tester.checkNull("cast(null as boolean)=cast(null as boolean)");
                tester.checkNull("cast(null as integer)=1");
            };});

        operatorTestCases.put(opTab.greaterThanOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkBoolean("1>2", Boolean.FALSE);
                tester.checkBoolean("1>1", Boolean.FALSE);
                tester.checkBoolean("2>1", Boolean.TRUE);
                tester.checkNull("3.0>cast(null as double)");
            };});

        operatorTestCases.put(opTab.isDistinctFromOperator, emptyTest);

        operatorTestCases.put(opTab.greaterThanOrEqualOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester)
            {
                tester.checkBoolean("1>=2", Boolean.FALSE);
                tester.checkBoolean("1>=1", Boolean.TRUE);
                tester.checkBoolean("2>=1", Boolean.TRUE);
                tester.checkNull("cast(null as real)>=999");
            };});

        operatorTestCases.put(opTab.inOperator, emptyTest);

        operatorTestCases.put(opTab.overlapsOperator, todoTest);

        operatorTestCases.put(opTab.lessThanOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkBoolean("1<2", Boolean.TRUE);
                tester.checkBoolean("1<1", Boolean.FALSE);
                tester.checkBoolean("2<1", Boolean.FALSE);
                tester.checkNull("123<cast(null as bigint)");
            };});

        operatorTestCases.put(opTab.lessThanOrEqualOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkBoolean("1<=2", Boolean.TRUE);
                tester.checkBoolean("1<=1", Boolean.TRUE);
                tester.checkBoolean("2<=1", Boolean.FALSE);
                tester.checkNull("cast(null as integer)<=3");
            };});

        operatorTestCases.put(opTab.minusOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkScalarExact("2-1", "1");
                tester.checkScalarApprox("2.0-1", "1.0");
                tester.checkScalarExact("1-2", "-1");
                tester.checkNull("1e1-cast(null as double)");
            };});

        operatorTestCases.put(opTab.multiplyOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkScalarExact("2*3", "6");
                tester.checkScalarExact("2*-3", "-6");
                tester.checkScalarExact("2*0", "0");
                tester.checkScalarApprox("2.0*3", "6.0");
                tester.checkNull("2e-3*cast(null as integer)");
            };});

        operatorTestCases.put(opTab.notEqualsOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkBoolean("1<>1", Boolean.FALSE);
                tester.checkBoolean("'a'<>'A'", Boolean.TRUE);
                tester.checkNull("'a'<>cast(null as varchar)");
            };});

        operatorTestCases.put(opTab.orOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkBoolean("true or false", Boolean.TRUE);
                tester.checkBoolean("false or false", Boolean.FALSE);
                tester.checkBoolean("true or cast(null as boolean)",
                    Boolean.TRUE);
                tester.checkNull("false or cast(null as boolean)");
            };});

        operatorTestCases.put(opTab.plusOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkScalarExact("1+2", "3");
                tester.checkScalarApprox("1+2.0", "3.0");
                tester.checkNull("cast(null as tinyint)+1");
                tester.checkNull("1e-2+cast(null as double)");
            };});

        operatorTestCases.put(opTab.descendingOperator, emptyTest);

        operatorTestCases.put(opTab.isNotNullOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkBoolean("true is not null", Boolean.TRUE);
                tester.checkBoolean("cast(null as boolean) is not null",
                    Boolean.FALSE);
            };});

        operatorTestCases.put(opTab.isNullOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkBoolean("true is null", Boolean.FALSE);
                tester.checkBoolean("cast(null as boolean) is null",
                    Boolean.TRUE);
            };});

        operatorTestCases.put(opTab.isNotTrueOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkBoolean("true is not true", Boolean.FALSE);
                tester.checkBoolean("false is not true", Boolean.TRUE);
                tester.checkBoolean("cast(null as boolean) is not true",
                    Boolean.TRUE);
            };});

        operatorTestCases.put(opTab.isTrueOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkBoolean("true is true", Boolean.TRUE);
                tester.checkBoolean("false is true", Boolean.FALSE);
                tester.checkBoolean("cast(null as boolean) is true",
                    Boolean.FALSE);
            };});

        operatorTestCases.put(opTab.isNotFalseOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkBoolean("false is not false", Boolean.FALSE);
                tester.checkBoolean("true is not false", Boolean.TRUE);
                tester.checkBoolean("cast(null as boolean) is not false",
                    Boolean.TRUE);
            };});

        operatorTestCases.put(opTab.isFalseOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkBoolean("false is false", Boolean.TRUE);
                tester.checkBoolean("true is false", Boolean.FALSE);
                tester.checkBoolean("cast(null as boolean) is false",
                    Boolean.FALSE);
            };});

        operatorTestCases.put(opTab.isNotUnknownOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkBoolean("false is not unknown", Boolean.TRUE);
                tester.checkBoolean("true is not unknown", Boolean.TRUE);
                tester.checkBoolean("cast(null as boolean) is not unknown",
                    Boolean.FALSE);
                tester.checkBoolean("unknown is not unknown", Boolean.FALSE);
            };});

        operatorTestCases.put(opTab.isUnknownOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkBoolean("false is unknown", Boolean.FALSE);
                tester.checkBoolean("true is unknown", Boolean.FALSE);
                tester.checkBoolean("cast(null as boolean) is unknown",
                    Boolean.TRUE);
                tester.checkBoolean("unknown is unknown", Boolean.TRUE);
            };});

        operatorTestCases.put(opTab.existsOperator, emptyTest);

        operatorTestCases.put(opTab.notOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkBoolean("not true", Boolean.FALSE);
                tester.checkBoolean("not false", Boolean.TRUE);
                tester.checkBoolean("not unknown", null);
                tester.checkNull("not cast(null as boolean)");
            };});

        operatorTestCases.put(opTab.prefixMinusOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkScalarExact("-1", "-1");
                tester.checkScalarApprox("-1.0", "-1.0");
                tester.checkNull("-cast(null as integer)");
            };});

        operatorTestCases.put(opTab.prefixPlusOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkScalarExact("+1", "1");
                tester.checkScalarApprox("+1.0", "1.0");
                tester.checkNull("+cast(null as integer)");
            };});

        operatorTestCases.put(opTab.explicitTableOperator, emptyTest);

        operatorTestCases.put(opTab.rowConstructor, emptyTest);

        operatorTestCases.put(opTab.valuesOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.check("select 'abc' from values(true)", "abc",
                    SqlTypeName.Varchar);
            };});

        operatorTestCases.put(opTab.litChainOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkString("'buttered'\n' toast'", "buttered toast");
                tester.checkString("'corned'\n' beef'\n' on'\n' rye'",
                    "corned beef on rye");
                tester.checkString("_latin1'Spaghetti'\n' all''Amatriciana'",
                    "Spaghetti all'Amatriciana");
                tester.checkBoolean("B'0101'\n'0011' = B'01010011'",
                    Boolean.TRUE);
                tester.checkBoolean("x'1234'\n'abcd' = x'1234abcd'",
                    Boolean.TRUE);
            };});

        operatorTestCases.put(opTab.betweenOperator, betweenTest);

        operatorTestCases.put(opTab.symmetricBetweenOperator, betweenTest);

        operatorTestCases.put(opTab.notBetweenOperator, notBetweenTest);

        operatorTestCases.put(opTab.symmetricNotBetweenOperator, notBetweenTest);

        operatorTestCases.put(opTab.notLikeOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkBoolean("'abc' not like '_b_'", Boolean.FALSE);
            };});

        operatorTestCases.put(opTab.likeOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
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
            };});

        operatorTestCases.put(opTab.notSimilarOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkBoolean("'ab' not similar to 'a_'", Boolean.FALSE);
            };});

        operatorTestCases.put(opTab.similarOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
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
            };});

        operatorTestCases.put(opTab.escapeOperator, emptyTest);

        operatorTestCases.put(opTab.selectOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.check("select * from values(1)", "1", SqlTypeName.Integer);
            };});

        operatorTestCases.put(opTab.caseOperator, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkScalarExact("case when 'a'='a' then 1 end", "1");
                tester.checkString("case 2 when 1 then 'a' when 2 then 'b' end",
                    "b");
                tester.checkScalarExact("case 'a' when 'a' then 1 end", "1");
                tester.checkNull("case 'a' when 'b' then 1 end");
                tester.checkScalarExact("case when 'a'=cast(null as varchar) then 1 else 2 end",
                    "2");
            };});

        operatorTestCases.put(opTab.joinOperator, emptyTest);

        operatorTestCases.put(opTab.insertOperator, emptyTest);

        operatorTestCases.put(opTab.deleteOperator, emptyTest);

        operatorTestCases.put(opTab.updateOperator, emptyTest);

        operatorTestCases.put(opTab.explainOperator, emptyTest);

        operatorTestCases.put(opTab.orderByOperator, emptyTest);

        operatorTestCases.put(opTab.substringFunc, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkString("substring('abc' from 1 for 2)", "ab");
                tester.checkString("substring('abc' from 2)", "bc");

                //substring reg exp not yet supported
                //                    tester.checkString("substring('foobar' from '%#\"o_b#\"%' for '#')", "oob");
                tester.checkNull("substring(cast(null as varchar),1,2)");
            };});

        operatorTestCases.put(opTab.convertFunc, todoTest);

        operatorTestCases.put(opTab.translateFunc, todoTest);

        operatorTestCases.put(opTab.overlayFunc, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
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
            };});

        operatorTestCases.put(opTab.trimFunc, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkString("trim('a' from 'aAa')", "A");
                tester.checkString("trim(both 'a' from 'aAa')", "A");
                tester.checkString("trim(leading 'a' from 'aAa')", "Aa");
                tester.checkString("trim(trailing 'a' from 'aAa')", "aA");
                tester.checkNull("trim(cast(null as varchar) from 'a')");
                tester.checkNull("trim('a' from cast(null as varchar))");
            };});

        operatorTestCases.put(opTab.positionFunc, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkScalarExact("position('b' in 'abc')", "2");
                tester.checkScalarExact("position('' in 'abc')", "1");

                //bit not yet implemented
                //                    tester.checkScalarExact("position(b'10' in b'0010')", "3");
                tester.checkNull("position(cast(null as varchar) in '0010')");
                tester.checkNull("position('a' in cast(null as varchar))");
            };});

        operatorTestCases.put(opTab.charLengthFunc, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkScalarExact("char_length('abc')", "3");
                tester.checkNull("char_length(cast(null as varchar))");
            };});

        operatorTestCases.put(opTab.characterLengthFunc, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkScalarExact("CHARACTER_LENGTH('abc')", "3");
                tester.checkNull("CHARACTER_LENGTH(cast(null as varchar))");
            };});

        operatorTestCases.put(opTab.upperFunc, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkString("upper('a')", "A");
                tester.checkString("upper('A')", "A");
                tester.checkString("upper('1')", "1");
                tester.checkNull("upper(cast(null as varchar))");
            };});

        operatorTestCases.put(opTab.lowerFunc, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkString("lower('A')", "a");
                tester.checkString("lower('a')", "a");
                tester.checkString("lower('1')", "1");
                tester.checkNull("lower(cast(null as varchar))");
            };});

        operatorTestCases.put(opTab.initcapFunc, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                //not yet supported
                //                    tester.checkString("initcap('aA')", "'Aa'");
                //                    tester.checkString("initcap('Aa')", "'Aa'");
                //                    tester.checkString("initcap('1a')", "'1a'");
                //                    tester.checkNull("initcap(cast(null as varchar))");
            };});

        operatorTestCases.put(opTab.powFunc, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkScalarApprox("pow(2,-2)", "0.25");
                tester.checkNull("pow(cast(null as integer),2)");
                tester.checkNull("pow(2,cast(null as double))");
            };});

        operatorTestCases.put(opTab.modFunc, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkScalarExact("mod(4,2)", "0");
                tester.checkNull("mod(cast(null as integer),2)");
                tester.checkNull("mod(4,cast(null as tinyint))");
            };});

        operatorTestCases.put(opTab.lnFunc, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                //todo not very platform independant
                tester.checkScalarApprox("ln(2.71828)", "0.999999327347282");
                tester.checkNull("ln(cast(null as tinyint))");
            };});

        operatorTestCases.put(opTab.logFunc, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkScalarApprox("log(10)", "1.0");
                tester.checkNull("log(cast(null as real))");
            };});

        operatorTestCases.put(opTab.absFunc, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkScalarExact("abs(-1)", "1");
                tester.checkNull("abs(cast(null as double))");
            };});

        operatorTestCases.put(opTab.nullIfFunc, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkNull("nullif(1,1)");
                tester.checkString("nullif('a','b')", "a");

                //todo renable when type checking for case is fixe
                //                    tester.checkString("nullif('a',cast(null as varchar))", "a");
                //                    tester.checkNull("nullif(cast(null as varchar),'a')");
            };});

        operatorTestCases.put(opTab.coalesceFunc, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkString("coalesce('a','b')", "a");
                tester.checkScalarExact("coalesce(null,null,3)", "3");
            };});

        operatorTestCases.put(opTab.userFunc, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkScalar("USER", null, "VARCHAR(30) NOT NULL");
            };});

        operatorTestCases.put(opTab.currentUserFunc, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkScalar("CURRENT_USER", null, "VARCHAR(30) NOT NULL");
            };});

        operatorTestCases.put(opTab.sessionUserFunc, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkScalar("SESSION_USER", null, "VARCHAR(30) NOT NULL");
            };});

        operatorTestCases.put(opTab.systemUserFunc, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                String user = System.getProperty("user.name"); // e.g. "jhyde"
                tester.checkScalar("SYSTEM_USER", user, "VARCHAR(30) NOT NULL");
            };});

        operatorTestCases.put(opTab.currentPathFunc, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkScalar("CURRENT_PATH", "", "VARCHAR(30) NOT NULL");
            };});

        operatorTestCases.put(opTab.currentRoleFunc, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                // We don't have roles yet, so the CURRENT_ROLE function returns
                // the empty string.
                tester.checkScalar("CURRENT_ROLE", "", "VARCHAR(30) NOT NULL");
            };});

        operatorTestCases.put(opTab.localTimeFunc, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkScalar("LOCALTIME", timePattern, "TIME(0) NOT NULL");
                //TODO: tester.checkFails("LOCALTIME()", "?", SqlTypeName.Time);
                tester.checkScalar("LOCALTIME(1)", timePattern,
                    "TIME(1) NOT NULL");
            };});

        operatorTestCases.put(opTab.localTimestampFunc, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkScalar("LOCALTIMESTAMP", timestampPattern,
                    "TIMESTAMP(0) NOT NULL");
                tester.checkFails("LOCALTIMESTAMP()", "?");
                tester.checkScalar("LOCALTIMESTAMP(1)", timestampPattern,
                    "TIMESTAMP(1) NOT NULL");
            };});

        operatorTestCases.put(opTab.currentTimeFunc, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkScalar("CURRENT_TIME", timePattern,
                    "TIME(0) NOT NULL");
                tester.checkFails("CURRENT_TIME()", "?");
                tester.checkScalar("CURRENT_TIME(1)", timePattern,
                    "TIME(1) NOT NULL");
            };});

        operatorTestCases.put(opTab.currentTimestampFunc, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkScalar("CURRENT_TIMESTAMP", timestampPattern,
                    "TIMESTAMP(0) NOT NULL");
                tester.checkFails("CURRENT_TIMESTAMP()", "?");
                tester.checkScalar("CURRENT_TIMESTAMP(1)", timestampPattern,
                    "TIMESTAMP(1) NOT NULL");
            };});

        operatorTestCases.put(opTab.currentDateFunc, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkScalar("CURRENT_DATE", datePattern, "DATE NOT NULL");
            };});

        operatorTestCases.put(opTab.castFunc, new SqlOperatorTestCase() {
            public void test(SqlTester tester) {
                tester.checkScalarExact("cast(1.0 as integer)", "1");
                tester.checkScalarApprox("cast(1 as double)", "1.0");
                tester.checkScalarApprox("cast(1.0 as double)", "1.0");
                tester.checkNull("cast(null as double)");
                tester.checkNull("cast(null as date)");
            };});
    }
}
