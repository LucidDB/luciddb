/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
package net.sf.farrago.test;

import java.io.*;

import junit.framework.*;

import net.sf.farrago.query.*;
import net.sf.farrago.session.*;

import openjava.ptree.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.oj.rex.*;
import org.eigenbase.rel.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql2rel.*;


/**
 * FarragoRexToOJTranslatorTest contains unit tests for the translation code in
 * {@link net.sf.farrago.ojrex}. Each test case takes a single SQL row
 * expression string as input, performs code generation, and then diffs the
 * generated Java code snippet against an expected .ref file under directory
 * farrago/testlog/FarragoRexToOJTranslatorTest.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoRexToOJTranslatorTest
    extends FarragoSqlToRelTestBase
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoRexToOJTranslatorTest object.
     *
     * @param testName .
     *
     * @throws Exception .
     */
    public FarragoRexToOJTranslatorTest(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ----------------------------------------------------------------

    // implement TestCase
    public static Test suite()
    {
        return wrappedSuite(FarragoRexToOJTranslatorTest.class);
    }

    // override FarragoTestCase
    protected boolean shouldDiff()
    {
        // this test should always work regardless of Fennel availability
        return true;
    }

    /**
     * Tests translation of a single row expression.
     *
     * @param rowExpression the text of the row expression to test (this is used
     * as the single select item in a constructed EXPLAIN PLAN statement)
     * @param tableExpression the table to use in the FROM clause (don't use
     * anything fancy here like a nested query because the optimizer used for
     * this test has its hands tied)
     */
    private void checkTranslation(
        String rowExpression,
        String tableExpression)
        throws Exception
    {
        String explainQuery =
            "EXPLAIN PLAN FOR SELECT " + rowExpression + " FROM "
            + tableExpression;

        checkQuery(explainQuery);
    }

    protected void checkAbstract(
        FarragoPreparingStmt stmt,
        RelNode topRel)
        throws Exception
    {
        assert (topRel instanceof IterCalcRel) : topRel.getClass().getName();
        IterCalcRel calcRel = (IterCalcRel) topRel;

        // grab the RexNode corresponding to our select item
        final RexProgram program = calcRel.getProgram();
        final RexLocalRef ref = program.getProjectList().get(0);
        RexNode rexNode = program.getExprList().get(ref.getIndex());

        // create objects needed for codegen
        SqlToRelConverter sqlToRelConverter = stmt.getSqlToRelConverter();
        FarragoRelImplementor relImplementor =
            new FarragoRelImplementor(
                stmt,
                sqlToRelConverter.getRexBuilder());

        // perform the codegen
        StatementList stmtList = new StatementList();
        MemberDeclarationList memberList = new MemberDeclarationList();
        final RexToOJTranslator translator =
            relImplementor.newStmtTranslator(
                calcRel,
                stmtList,
                memberList);
        Expression translatedExp;
        try {
            translator.pushProgram(program);
            translatedExp = translator.translateRexNode(rexNode);
        } finally {
            translator.popProgram(program);
        }

        // dump the generated code
        Writer writer = openTestLog();
        PrintWriter printWriter = new PrintWriter(writer);
        if (!memberList.isEmpty()) {
            printWriter.println(memberList);
        }
        if (!stmtList.isEmpty()) {
            printWriter.println(stmtList);
        }
        printWriter.println("return " + translatedExp + ";");
        printWriter.close();

        // and diff it against what we expect
        diffTestLog();
    }

    protected void initPlanner(FarragoPreparingStmt stmt)
    {
        // TODO jvs 9-Apr-2006:  Eliminate the init parameter
        // to newPlanner and construct a HepPlanner here.

        // NOTE jvs 22-June-2004:  We use a very stripped-down planner
        // so that the optimizer doesn't decide to rewrite our
        // carefully constructed expressions.  This also guarantees
        // that the Java calculator is used without having to
        // mess with system parameters.
        FarragoSessionPlanner planner =
            stmt.getSession().getPersonality().newPlanner(stmt, false);
        planner.addRule(IterRules.IterCalcRule.instance);
        FennelToIteratorConverter.register(planner);

        // Constant reduction hides what we're trying to test for.
        planner.setRuleDescExclusionFilter(
            FarragoReduceExpressionsRule.EXCLUSION_PATTERN);
        stmt.setPlanner(planner);
    }

    /**
     * Tests translation of a single row expression, using the SALES.EMPS table
     * for context.
     *
     * @param rowExpression the text of the row expression to test (this is used
     * as the single select item in a constructed EXPLAIN PLAN statement)
     */
    private void checkTranslation(String rowExpression)
        throws Exception
    {
        checkTranslation(rowExpression, "SALES.EMPS");
    }

    public void testPrimitiveEquals()
        throws Exception
    {
        // NOTE:  choose one nullable and one not null
        checkTranslation("empno = age");
    }

    public void testPrimitiveEqualsNotNull()
        throws Exception
    {
        // NOTE:  choose both not null
        checkTranslation("empno = empid");
    }

    public void testPrimitiveLess()
        throws Exception
    {
        // NOTE:  choose one nullable and one not null
        checkTranslation("empno < age");
    }

    public void testPrimitiveGreater()
        throws Exception
    {
        // NOTE:  choose one nullable and one not null
        checkTranslation("empno > age");
    }

    public void testPrimitivePlus()
        throws Exception
    {
        // NOTE:  choose one nullable and one not null
        checkTranslation("empno + age");
    }

    public void testPrimitiveMinus()
        throws Exception
    {
        // NOTE:  choose one nullable and one not null
        checkTranslation("empno - age");
    }

    public void testPrimitiveTimes()
        throws Exception
    {
        // NOTE:  choose one nullable and one not null
        checkTranslation("empno * age");
    }

    public void testPrimitiveDivide()
        throws Exception
    {
        // NOTE:  choose one nullable and one not null
        checkTranslation("empno / age");
    }

    public void testPrimitivePrefixMinus()
        throws Exception
    {
        // NOTE:  choose nullable
        checkTranslation("-age");
    }

    public void testPrimitiveGreaterBoolean()
        throws Exception
    {
        // NOTE: choose one null, one not null
        checkTranslation("manager > slacker");
    }

    public void testPrefixMinusCastNullTinyint()
        throws Exception
    {
        checkTranslation("-cast(null as tinyint)");
    }

    public void testPlusCastNullSmallint()
        throws Exception
    {
        checkTranslation("cast(null as tinyint) + cast (null as smallint)");
    }

    public void testVarcharEquals()
        throws Exception
    {
        // NOTE:  choose one nullable and one not null
        checkTranslation("name = city");
    }

    public void testVarcharLess()
        throws Exception
    {
        // NOTE:  choose one nullable and one not null
        checkTranslation("name < city");
    }

    public void testBooleanNot()
        throws Exception
    {
        // NOTE: choose nullable
        checkTranslation("not slacker");
    }

    public void testBooleanOr()
        throws Exception
    {
        // NOTE:  choose one nullable and one not null
        checkTranslation("slacker or manager");
    }

    public void testBooleanOrNullable()
        throws Exception
    {
        // NOTE:  choose both nullable
        checkTranslation("(empno < age) or (name = city)");
    }

    public void testBooleanAnd()
        throws Exception
    {
        // NOTE:  choose one nullable and one not null
        checkTranslation("slacker and manager");
    }

    public void testBooleanConjunction()
        throws Exception
    {
        // NOTE:  choose both nullable
        checkTranslation("(empno < age) and (name = city)");
    }

    public void testBooleanConjunctionNotNull()
        throws Exception
    {
        // NOTE:  choose all not null
        checkTranslation("(empno = empid) and (empno = deptno)");
    }

    public void testNullableIsTrue()
        throws Exception
    {
        checkTranslation("slacker is true");
    }

    public void testNullableIsFalse()
        throws Exception
    {
        checkTranslation("slacker is false");
    }

    public void testNotNullIsTrue()
        throws Exception
    {
        checkTranslation("manager is true");
    }

    public void testNotNullIsFalse()
        throws Exception
    {
        checkTranslation("manager is false");
    }

    public void testNullableIsNull()
        throws Exception
    {
        checkTranslation("age is null");
    }

    public void testNullableIsNotNull()
        throws Exception
    {
        checkTranslation("age is not null");
    }

    public void testNotNullIsNull()
        throws Exception
    {
        checkTranslation("empno is null");
    }

    public void testNotNullIsNotNull()
        throws Exception
    {
        checkTranslation("empno is not null");
    }

    // FIXME

    /*
    public void testDynamicParam() throws Exception { checkTranslation("empno +
     ?"); }
     */
    public void testUser()
        throws Exception
    {
        checkTranslation("user");
    }

    public void testCurrentUser()
        throws Exception
    {
        checkTranslation("current_user");
    }

    public void testSessionUser()
        throws Exception
    {
        checkTranslation("session_user");
    }

    public void testSystemUser()
        throws Exception
    {
        checkTranslation("system_user");
    }

    public void testCurrentDate()
        throws Exception
    {
        checkTranslation("current_date");
    }

    public void testCurrentTime()
        throws Exception
    {
        checkTranslation("current_time");
    }

    public void testCurrentTimestamp()
        throws Exception
    {
        checkTranslation("current_timestamp");
    }

    public void testCurrentPath()
        throws Exception
    {
        checkTranslation("current_path");
    }

    public void testJavaUdfInvocation()
        throws Exception
    {
        checkTranslation("sales.decrypt_public_key(public_key)");
    }

    public void testSqlUdfInvocation()
        throws Exception
    {
        checkTranslation("sales.maybe_female(gender)");
    }

    // FIXME

    /*
    public void testCastNullToPrimitive() throws Exception { // FIXME:  should
     take cast(null as int) checkTranslation("cast(null as integer)"); }
     */

    // FIXME

    /*
    public void testCastNullToVarchar() throws Exception {
     checkTranslation("cast(null as varchar(10))"); }
     */
    public void testCastToVarcharImplicitTruncate()
        throws Exception
    {
        checkTranslation(
            "cast('supercalifragilistiexpialodocious' as varchar(10))");
    }

    public void testCastToVarchar()
        throws Exception
    {
        checkTranslation("cast('boo' as varchar(10))");
    }

    // TODO (depends on dtbug 79)

    /*
    public void testCastToCharImplicitPad() throws Exception { checkTranslation(
     "cast('boo' as char(10))"); }
     */

    // TODO (depends on dtbug 79)

    /*
    public void testCastToCharExact() throws Exception { checkTranslation(
     "cast('0123456789' as char(10))"); }
     */

    // TODO (depends on dtbug 79)

    /*
    public void testCastToBinaryImplicitPad() throws Exception {
     checkTranslation(     "cast(x'58797A' as binary(10))"); }
     */
    public void testCastToVarbinaryImplicitTruncate()
        throws Exception
    {
        checkTranslation("cast(x'00112233445566778899AABB' as varbinary(10))");
    }

    public void testCastIntToVarchar()
        throws Exception
    {
        checkTranslation("cast(cast(null as tinyint) as varchar(30))");
    }

    // TODO jvs 22-June-2004:  figure out a way to test codegen for
    // assignment of nullable value to NOT NULL field

    //
    // start Case test cases.
    //
    //
    public void testCaseNotNullableCondWithElse()
        throws Exception
    {
        checkTranslation(
            "case manager when true then 'Yes' when false then 'No' else 'Other' end");
    }

    public void testCaseNotNullableCondWithoutElse()
        throws Exception
    {
        checkTranslation("case deptno when 10 then 'Yes' end");
    }

    public void testCaseNullableCondWithElse()
        throws Exception
    {
        checkTranslation(
            "case age when 50 then 'fifty' when 25 then 'twenty-five' end");
    }

    public void testCaseNullableCondWithoutElse()
        throws Exception
    {
        checkTranslation("case gender when 'M' then 'Yes' end");
    }

    public void testCaseNotNullableCondWithElsePrimitive()
        throws Exception
    {
        checkTranslation("case empno when 120 then 1 else 2 end");
    }

    public void testCaseNotNullableCondWithoutElsePrimitive()
        throws Exception
    {
        checkTranslation(
            "case name when 'Fred' then 1 when 'Eric' then 2  when 'Wilma' then 3 when 'John' then 4 end");
    }

    public void testCaseNullableCondWithElsePrimitive()
        throws Exception
    {
        checkTranslation(
            "case deptno when 10 then 1 when 20 then 2 when 40 then 3 else 4 end");
    }

    public void testCaseNullableCondWithoutElsePrimitive()
        throws Exception
    {
        checkTranslation("case slacker when true then 1 end");
    }

    public void testSubstringNullableLength()
        throws Exception
    {
        checkTranslation("substring(city,  2, age/10)");
    }

    public void testSubstringNullablePosition()
        throws Exception
    {
        checkTranslation("substring(city,  age/20, empid)");
    }

    public void testSubstringNoLength()
        throws Exception
    {
        checkTranslation("substring(city, 3)");
    }

    public void testSubstringPositionLessThanZero()
        throws Exception
    {
        checkTranslation("substring(city, -1, 4)");
    }

    public void testSubstringPositionZero()
        throws Exception
    {
        checkTranslation("substring(city, 0, 4)");
    }

    public void testSubstringNegativeLength()
        throws Exception
    {
        checkTranslation("substring(city, 1, empid - 2)");
    }

    public void testSubstringNothingNullable()
        throws Exception
    {
        checkTranslation("substring(name, 2, empid)");
    }

    public void testConcatNoNullable()
        throws Exception
    {
        checkTranslation("name||name");
    }

    public void testConcatWithOneNullable()
        throws Exception
    {
        checkTranslation("city||name");
    }

    public void testConcatBothNullable()
        throws Exception
    {
        checkTranslation("city||city");
    }

    public void testOverlayNoLength()
        throws Exception
    {
        checkTranslation("overlay(city placing 'MIDDLE' from 2)");
    }

    public void testOverlayNullable()
        throws Exception
    {
        checkTranslation("overlay(city placing 'MIDDLE' from 2 for 3)");
    }

    public void testOverlayNoNullable()
        throws Exception
    {
        checkTranslation("overlay(name placing 'MIDDLE' from 2 for 3)");
    }

    public void testOverlayThreeNullable()
        throws Exception
    {
        checkTranslation("overlay(city placing name from age for age)");
    }

    public void testOverlayAllNullable()
        throws Exception
    {
        checkTranslation("overlay(city placing gender from age for age)");
    }

    public void testPower()
        throws Exception
    {
        checkTranslation("power(2, empid)");
    }

    public void testMod()
        throws Exception
    {
        checkTranslation("mod(age, 3)");
    }

    public void testTrimBoth()
        throws Exception
    {
        checkTranslation("trim(both 'S' from city)");
    }

    public void testTrimLeading()
        throws Exception
    {
        checkTranslation("trim(leading 'W' from name)");
    }

    public void testTrimTrailing()
        throws Exception
    {
        checkTranslation("trim(trailing 'c' from name)");
    }

    public void testUpper()
        throws Exception
    {
        checkTranslation("upper(city)");
    }

    public void testLower()
        throws Exception
    {
        checkTranslation("Lower(city)");
    }

    public void testInitcap()
        throws Exception
    {
        checkTranslation("initcap(city)");
    }

    public void testCharLength()
        throws Exception
    {
        checkTranslation("char_length(city)");
    }

    public void testCharacterLength()
        throws Exception
    {
        checkTranslation("character_length(city)");
    }

    public void testPosition()
        throws Exception
    {
        checkTranslation("position('Fran' in city)");
    }

    public void testLikeLiteral()
        throws Exception
    {
        checkTranslation("City like 'San%'");
    }

    public void testLikeRuntime()
        throws Exception
    {
        checkTranslation("City like Name");
    }

    public void testLikeLiteralWithEscape()
        throws Exception
    {
        checkTranslation("City like 'San%' escape 'n'");
    }

    public void testLikeRuntimeWithEscape()
        throws Exception
    {
        checkTranslation("City like Name escape 'n'");
    }

    public void testSimilarLiteral()
        throws Exception
    {
        checkTranslation("City similar to '[S][[:ALPHA:]]n%'");
    }

    public void testSimilarRuntime()
        throws Exception
    {
        checkTranslation("City similar to Name");
    }

    public void testSimilarLiteralWithEscape()
        throws Exception
    {
        checkTranslation("City similar to 'San%' escape 'n'");
    }

    public void testSimilarRuntimeWithEscape()
        throws Exception
    {
        checkTranslation("City similar to Name escape 'n'");
    }
}

// End FarragoRexToOJTranslatorTest.java
