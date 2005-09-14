/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package net.sf.farrago.test;


import java.io.*;

import junit.framework.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.db.*;
import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.query.*;
import net.sf.farrago.session.*;
import net.sf.farrago.util.*;

import openjava.ptree.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.oj.stmt.*;
import org.eigenbase.rel.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql2rel.*;


/**
 * FarragoRexToOJTranslatorTest contains unit tests for the translation code in
 * {@link net.sf.farrago.ojrex}.  Each test case takes a single SQL row
 * expression string as input, performs code generation, and then diffs the
 * generated Java code snippet against an expected .ref file under directory
 * farrago/testlog/FarragoRexToOJTranslatorTest.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoRexToOJTranslatorTest extends FarragoTestCase
{
    //~ Constructors ----------------------------------------------------------

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

    //~ Methods ---------------------------------------------------------------

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
     * @param rowExpression the text of the row expression to test
     * (this is used as the single select item in a constructed
     * EXPLAIN PLAN statement)
     *
     * @param tableExpression the table to use in the FROM clause
     * (don't use anything fancy here like a nested query because the
     * optimizer used for this test has its hands tied)
     */
    private void testTranslation(
        String rowExpression,
        String tableExpression)
        throws Exception
    {
        String explainQuery =
            "EXPLAIN PLAN FOR SELECT " + rowExpression + " FROM "
            + tableExpression;

        // hijack necessary internals
        FarragoJdbcEngineConnection farragoConnection =
            (FarragoJdbcEngineConnection) connection;
        FarragoDbSession session =
            (FarragoDbSession) farragoConnection.getSession();

        // guarantee release of any resources we allocate on the way
        FarragoCompoundAllocation allocations =
            new FarragoCompoundAllocation();
        FarragoReposTxnContext reposTxn = new FarragoReposTxnContext(repos);
        try {
            reposTxn.beginReadTxn();

            // create a private code cache: don't pollute the real
            // database code cache
            FarragoObjectCache objCache =
                new FarragoObjectCache(allocations, 0);

            // FarragoPreparingStmt does most of the work for us
            FarragoSessionStmtValidator stmtValidator =
                new FarragoStmtValidator(repos,
                    session.getDatabase().getFennelDbHandle(), session,
                    objCache, objCache,
                    session.getSessionIndexMap());
            allocations.addAllocation(stmtValidator);
            FarragoPreparingStmt stmt =
                new FarragoPreparingStmt(stmtValidator);

            initPlanner(stmt);

            // parse the EXPLAIN PLAN statement
            SqlParser sqlParser = new SqlParser(explainQuery);
            SqlNode sqlNode = sqlParser.parseStmt();

            // prepare it
            PreparedExplanation explanation =
                (PreparedExplanation) stmt.prepareSql(
                    sqlNode,
                    session.getPersonality().getRuntimeContextClass(stmt),
                    stmt.getSqlValidator(),
                    true);

            // dig out the top-level relational expression, which
            // we just KNOW will be an IterCalcRel
            RelNode topRel = explanation.getRel();
            assert (topRel instanceof IterCalcRel) : topRel.getClass().getName();
            IterCalcRel calcRel = (IterCalcRel) topRel;

            // grab the RexNode corresponding to our select item
            RexNode rexNode = calcRel.getChildExps()[0];

            // create objects needed for codegen
            SqlToRelConverter sqlToRelConverter = stmt.getSqlToRelConverter();
            FarragoRelImplementor relImplementor =
                new FarragoRelImplementor(stmt,
                    sqlToRelConverter.getRexBuilder());

            // perform the codegen
            StatementList stmtList = new StatementList();
            MemberDeclarationList memberList = new MemberDeclarationList();
            Expression translatedExp =
                relImplementor.translateViaStatements(calcRel, rexNode,
                    stmtList, memberList);

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
        } finally {
            allocations.closeAllocation();
            reposTxn.commit();
        }
    }

    private void initPlanner(FarragoPreparingStmt stmt)
    {
        // NOTE jvs 22-June-2004:  We use a very stripped-down planner
        // so that the optimizer doesn't decide to rewrite our
        // carefully constructed expressions.  This also guarantees
        // that the Java calculator is used without having to
        // mess with system parameters.
        FarragoSessionPlanner planner =
            stmt.getSession().getPersonality().newPlanner(stmt,false);
        planner.addRule(IterRules.IterCalcRule.instance);
        FennelToIteratorConverter.register(planner);
        stmt.setPlanner(planner);
    }

    /**
     * Tests translation of a single row expression, using the
     * SALES.EMPS table for context.
     *
     * @param rowExpression the text of the row expression to test
     * (this is used as the single select item in a constructed
     * EXPLAIN PLAN statement)
     */
    private void testTranslation(String rowExpression)
        throws Exception
    {
        testTranslation(rowExpression, "SALES.EMPS");
    }

    public void testPrimitiveEquals()
        throws Exception
    {
        // NOTE:  choose one nullable and one not null
        testTranslation("empno = age");
    }

    public void testPrimitiveEqualsNotNull()
        throws Exception
    {
        // NOTE:  choose both not null
        testTranslation("empno = empid");
    }

    public void testPrimitiveLess()
        throws Exception
    {
        // NOTE:  choose one nullable and one not null
        testTranslation("empno < age");
    }

    public void testPrimitiveGreater()
        throws Exception
    {
        // NOTE:  choose one nullable and one not null
        testTranslation("empno > age");
    }

    public void testPrimitivePlus()
        throws Exception
    {
        // NOTE:  choose one nullable and one not null
        testTranslation("empno + age");
    }

    public void testPrimitiveMinus()
        throws Exception
    {
        // NOTE:  choose one nullable and one not null
        testTranslation("empno - age");
    }

    public void testPrimitiveTimes()
        throws Exception
    {
        // NOTE:  choose one nullable and one not null
        testTranslation("empno * age");
    }

    public void testPrimitiveDivide()
        throws Exception
    {
        // NOTE:  choose one nullable and one not null
        testTranslation("empno / age");
    }

    public void testPrimitivePrefixMinus()
        throws Exception
    {
        // NOTE:  choose nullable
        testTranslation("-age");
    }

    public void testPrimitiveGreaterBoolean()
        throws Exception
    {
        // NOTE: choose one null, one not null
        testTranslation("manager > slacker");
    }

    public void testPrefixMinusCastNullTinyint()
        throws Exception
    {
        testTranslation("-cast(null as tinyint)");
    }

    public void testPlusCastNullSmallint()
        throws Exception
    {
        testTranslation("cast(null as tinyint) + cast (null as smallint)");
    }

    public void testVarcharEquals()
        throws Exception
    {
        // NOTE:  choose one nullable and one not null
        testTranslation("name = city");
    }

    public void testVarcharLess()
        throws Exception
    {
        // NOTE:  choose one nullable and one not null
        testTranslation("name < city");
    }

    public void testBooleanNot()
        throws Exception
    {
        // NOTE: choose nullable
        testTranslation("not slacker");
    }

    public void testBooleanOr()
        throws Exception
    {
        // NOTE:  choose one nullable and one not null
        testTranslation("slacker or manager");
    }

    public void testBooleanOrNullable()
        throws Exception
    {
        // NOTE:  choose both nullable
        testTranslation("(empno < age) or (name = city)");
    }

    public void testBooleanAnd()
        throws Exception
    {
        // NOTE:  choose one nullable and one not null
        testTranslation("slacker and manager");
    }

    public void testBooleanConjunction()
        throws Exception
    {
        // NOTE:  choose both nullable
        testTranslation("(empno < age) and (name = city)");
    }

    public void testBooleanConjunctionNotNull()
        throws Exception
    {
        // NOTE:  choose all not null
        testTranslation("(empno = empid) and (empno = deptno)");
    }

    public void testNullableIsTrue()
        throws Exception
    {
        testTranslation("slacker is true");
    }

    public void testNullableIsFalse()
        throws Exception
    {
        testTranslation("slacker is false");
    }

    public void testNotNullIsTrue()
        throws Exception
    {
        testTranslation("manager is true");
    }

    public void testNotNullIsFalse()
        throws Exception
    {
        testTranslation("manager is false");
    }

    public void testNullableIsNull()
        throws Exception
    {
        testTranslation("age is null");
    }

    public void testNullableIsNotNull()
        throws Exception
    {
        testTranslation("age is not null");
    }

    public void testNotNullIsNull()
        throws Exception
    {
        testTranslation("empno is null");
    }

    public void testNotNullIsNotNull()
        throws Exception
    {
        testTranslation("empno is not null");
    }

    // FIXME

    /*
    public void testDynamicParam()
        throws Exception
    {
        testTranslation("empno + ?");
    }
    */
    public void testUser()
        throws Exception
    {
        testTranslation("user");
    }

    public void testCurrentUser()
        throws Exception
    {
        testTranslation("current_user");
    }

    public void testSessionUser()
        throws Exception
    {
        testTranslation("session_user");
    }

    public void testSystemUser()
        throws Exception
    {
        testTranslation("system_user");
    }

    public void testCurrentDate()
        throws Exception
    {
        testTranslation("current_date");
    }

    public void testCurrentTime()
        throws Exception
    {
        testTranslation("current_time");
    }

    public void testCurrentTimestamp()
        throws Exception
    {
        testTranslation("current_timestamp");
    }

    public void testCurrentPath()
        throws Exception
    {
        testTranslation("current_path");
    }

    public void testJavaUdfInvocation()
        throws Exception
    {
        testTranslation("sales.decrypt_public_key(public_key)");
    }

    public void testSqlUdfInvocation()
        throws Exception
    {
        testTranslation("sales.maybe_female(gender)");
    }

    // FIXME

    /*
    public void testCastNullToPrimitive()
        throws Exception
    {
        // FIXME:  should take cast(null as int)
        testTranslation("cast(null as integer)");
    }
    */

    // FIXME

    /*
    public void testCastNullToVarchar()
        throws Exception
    {
        testTranslation("cast(null as varchar(10))");
    }
    */
    public void testCastToVarcharImplicitTruncate()
        throws Exception
    {
        testTranslation(
            "cast('supercalifragilistiexpialodocious' as varchar(10))");
    }

    public void testCastToVarchar()
        throws Exception
    {
        testTranslation("cast('boo' as varchar(10))");
    }

    // TODO (depends on dtbug 79)

    /*
    public void testCastToCharImplicitPad()
        throws Exception
    {
        testTranslation(
            "cast('boo' as char(10))");
    }
    */

    // TODO (depends on dtbug 79)

    /*
    public void testCastToCharExact()
        throws Exception
    {
        testTranslation(
            "cast('0123456789' as char(10))");
    }
    */

    // TODO (depends on dtbug 79)

    /*
    public void testCastToBinaryImplicitPad()
        throws Exception
    {
        testTranslation(
            "cast(x'58797A' as binary(10))");
    }
    */
    public void testCastToVarbinaryImplicitTruncate()
        throws Exception
    {
        testTranslation("cast(x'00112233445566778899AABB' as varbinary(10))");
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
        testTranslation("case manager when true then 'Yes' when false then 'No' else 'Other' end");
    }

    public void testCaseNotNullableCondWithoutElse()
        throws Exception
    {
        testTranslation("case deptno when 10 then 'Yes' end");
    }

    public void testCaseNullableCondWithElse()
        throws Exception
    {
        testTranslation("case age when 50 then 'fifty' when 25 then 'twenty-five' end");
    }
    public void testCaseNullableCondWithoutElse()
        throws Exception
    {
        testTranslation("case gender when 'M' then 'Yes' end");
    }

    public void testCaseNotNullableCondWithElsePrimitive()
        throws Exception
    {
        testTranslation("case empno when 120 then 1 else 2 end");
    }

    public void testCaseNotNullableCondWithoutElsePrimitive()
        throws Exception
    {
        testTranslation("case name when 'Fred' then 1 when 'Eric' then 2  when 'Wilma' then 3 when 'John' then 4 end");
    }

    public void testCaseNullableCondWithElsePrimitive()
        throws Exception
    {
        testTranslation("case deptno when 10 then 1 when 20 then 2 when 40 then 3 else 4 end");
    }
    public void testCaseNullableCondWithoutElsePrimitive()
        throws Exception
    {
        testTranslation("case slacker when true then 1 end");
    }

    public void testSubstringNullableLength()
        throws Exception
    {
        testTranslation("substring(city,  2, age/10)");
    }

    public void testSubstringNullablePosition()
        throws Exception
    {
        testTranslation("substring(city,  age/20, empid)");
    }

    public void testSubstringNoLength()
        throws Exception
    {
        testTranslation("substring(city, 3)");
    }

    public void testSubstringPositionLessThanZero()
        throws Exception
    {
        testTranslation("substring(city, -1, 4)");
    }

    public void testSubstringPositionZero()
        throws Exception
    {
        testTranslation("substring(city, 0, 4)");
    }

    public void testSubstringNegativeLength()
        throws Exception
    {
        testTranslation("substring(city, 1, empid - 2)");
    }

    public void testSubstringNothingNullable()
        throws Exception
    {
        testTranslation("substring(name, 2, empid)");
    }

    public void testConcatNoNullable()
        throws Exception
    {
        testTranslation("name||name");
    }

    public void testConcatWithOneNullable()
        throws Exception
    {
        testTranslation("city||name");
    }

    public void testConcatBothNullable()
        throws Exception
    {
        testTranslation("city||city");
    }

    public void testOverlayNoLength()
        throws Exception
    {
        testTranslation("overlay(city placing 'MIDDLE' from 2)");
    }

    public void testOverlayNullable()
        throws Exception
    {
        testTranslation("overlay(city placing 'MIDDLE' from 2 for 3)");
    }

    public void testOverlayNoNullable()
        throws Exception
    {
        testTranslation("overlay(name placing 'MIDDLE' from 2 for 3)");
    }

    public void testOverlayThreeNullable()
        throws Exception
    {
        testTranslation("overlay(city placing name from age for age)");
    }

    public void testOverlayAllNullable()
        throws Exception
    {
        testTranslation("overlay(city placing gender from age for age)");
    }

    public void testPower()
        throws Exception
    {
        testTranslation("pow(2, empid)");
    }

    public void testMod()
        throws Exception
    {
        testTranslation("mod(age, 3)");
    }

    public void testTrimBoth()
        throws Exception
    {
        testTranslation("trim(both 'S' from city)");
    }

    public void testTrimLeading()
        throws Exception
    {
        testTranslation("trim(leading 'W' from name)");
    }

    public void testTrimTrailing()
        throws Exception
    {
        testTranslation("trim(trailing 'c' from name)");
    }

    public void testUpper()
        throws Exception
    {
        testTranslation("upper(city)");
    }

    public void testLower()
        throws Exception
    {
        testTranslation("Lower(city)");
    }

    public void testInitcap()
        throws Exception
    {
        testTranslation("initcap(city)");
    }

    public void testCharLength()
        throws Exception
    {
        testTranslation("char_length(city)");
    }

    public void testCharacterLength()
        throws Exception
    {
        testTranslation("character_length(city)");
    }

    public void testPosition()
        throws Exception
    {
        testTranslation("position('Fran' in city)");
    }

    public void testLikeLiteral()
        throws Exception
    {
        testTranslation("City like 'San%'");
    }

    public void testLikeRuntime()
        throws Exception
    {
        testTranslation("City like Name");
    }

    public void testLikeLiteralWithEscape()
        throws Exception
    {
        testTranslation("City like 'San%' escape 'n'");
    }

    public void testLikeRuntimeWithEscape()
        throws Exception
    {
        testTranslation("City like Name escape 'n'");
    }

    public void testSimilarLiteral()
        throws Exception
    {
        testTranslation("City similar to '[S][[:ALPHA:]]n%'");
    }

    public void testSimilarRuntime()
        throws Exception
    {
        testTranslation("City similar to Name");
    }

    public void testSimilarLiteralWithEscape()
        throws Exception
    {
        testTranslation("City similar to 'San%' escape 'n'");
    }

    public void testSimilarRuntimeWithEscape()
        throws Exception
    {
        testTranslation("City similar to Name escape 'n'");
    }


}

// End FarragoRexToOJTranslatorTest.java
