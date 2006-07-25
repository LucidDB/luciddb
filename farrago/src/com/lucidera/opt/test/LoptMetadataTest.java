/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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
package com.lucidera.opt.test;

import com.lucidera.lcs.*;

import java.math.*;

import java.util.*;

import junit.framework.*;

import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.query.*;
import net.sf.farrago.session.*;
import net.sf.farrago.test.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.relopt.hep.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sarg.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.stat.*;
import org.eigenbase.util.*;


/**
 * LoptMetadataTest tests the relational expression metadata queries relied on
 * by the LucidDB optimizer.
 *
 * <p>NOTE jvs 11-Apr-2006: We don't actually diff the plans used as input to
 * the metadata queries, because that's not what we're testing here. To see what
 * you're getting while developing new tests (or when old ones break), set trace
 * net.sf.farrago.query.plandump.level=FINE and check the trace output; you'll
 * also be able to see rowcounts and cumulative costs for all expressions.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LoptMetadataTest
    extends FarragoSqlToRelTestBase
{

    //~ Static fields/initializers ---------------------------------------------

    private static boolean doneStaticSetup;

    private static final double EPSILON = 1.0e-5;

    // REVIEW jvs 19-Apr-2006:  It's a bit confusing having two
    // tables named EMPS!
    private static final long SALES_EMPS_ROWCOUNT = 100;

    private static final long COLSTORE_EMPS_ROWCOUNT = 99500;

    private static final long COLSTORE_DEPTS_ROWCOUNT = 150;

    // guess for non-sargable equals
    private static final double DEFAULT_EQUAL_SELECTIVITY = 0.15;

    // guess for any sargable predicate when stats are missing
    private static final double DEFAULT_SARGABLE_SELECTIVITY = 0.1;

    private static final double DEFAULT_ROWCOUNT = 100.0;

    //~ Instance fields --------------------------------------------------------

    private HepProgram program;

    private RelNode rootRel;

    private RelStatSource tableStats;

    private RexBuilder rexBuilder;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new LoptMetadataTest object.
     *
     * @param testName JUnit test name
     *
     * @throws Exception .
     */
    public LoptMetadataTest(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ----------------------------------------------------------------

    // implement TestCase
    public static Test suite()
    {
        return wrappedSuite(LoptMetadataTest.class);
    }

    // implement TestCase
    protected void setUp()
        throws Exception
    {
        super.setUp();
        if (doneStaticSetup) {
            return;
        }
        doneStaticSetup = true;

        // TODO jvs 11-Apr-2006:  provide some common LucidDB unit
        // test infrastructure for doing this

        // Change personality to LucidDB for the duration of this test.
        stmt.executeUpdate(
            "create schema lopt_metadata");
        stmt.executeUpdate(
            "set schema 'lopt_metadata'");
        stmt.executeUpdate(
            "alter session implementation set jar"
            + " sys_boot.sys_boot.luciddb_plugin");

        // Tables with statistics
        FarragoJdbcEngineConnection farragoConnection =
            (FarragoJdbcEngineConnection) connection;
        FarragoSession session = farragoConnection.getSession();

        // simulate a 1% sample with 995 rows
        // split among 100 bars: 10 rows per (99) bars, 5 rows last bar
        // deptno is a low cardinality column (all points sampled):
        //     bars alternate between 2,1,2,1 distinct values
        // name is a high cardinality column (few points are sampled):
        //     bars contain about 9.9 distinct values
        stmt.executeUpdate(
            "create table EMPS (deptno int, name varchar(256), age int)");
        FarragoStatsUtil.setTableRowCount(
            session,
            "",
            "",
            "EMPS",
            COLSTORE_EMPS_ROWCOUNT);
        FarragoStatsUtil.createColumnHistogram(
            session,
            "",
            "",
            "EMPS",
            "DEPTNO",
            150,
            1,
            150,
            1,
            "0123456789");
        FarragoStatsUtil.createColumnHistogram(
            session,
            "",
            "",
            "EMPS",
            "NAME",
            90000,
            1,
            990,
            0,
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ");

        stmt.executeUpdate(
            "create table DEPTS (deptno int, dname varchar(256))");
        FarragoStatsUtil.setTableRowCount(
            session,
            "",
            "",
            "DEPTS",
            COLSTORE_DEPTS_ROWCOUNT);
        FarragoStatsUtil.createColumnHistogram(
            session,
            "",
            "",
            "DEPTS",
            "DEPTNO",
            150,
            100,
            150,
            1,
            "0123456789");
        FarragoStatsUtil.createColumnHistogram(
            session,
            "",
            "",
            "DEPTS",
            "DNAME",
            150,
            100,
            150,
            0,
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ");

        // a table for testing typed values
        stmt.executeUpdate(
            "create table WINES ("
            + "  name varchar(256) primary key,"
            + "  imported boolean,"
            + "  price decimal(5,2),"
            + "  price2 double,"
            + "  code varbinary(256),"
            + "  bottle_date date,"
            + "  bottle_time time,"
            + "  purchase_timestamp timestamp)");
        stmt.executeUpdate(
            "insert into WINES values "
            + "  ('zinfadel', false, 15.00, 15e0, x'45e6ab', "
            + "   DATE'2001-01-01', TIME'23:01:01', TIMESTAMP'2002-01-01 12:01:01'),"
            + "  ('zinfadel2', false, 15.00, 15e0, x'45e6ab', "
            + "   DATE'2001-01-01', TIME'23:01:01', TIMESTAMP'2002-01-01 12:01:01'),"
            + "  ('merlot', true, 22.00, 22e0, x'00', "
            + "   DATE'2002-01-01', TIME'12:01:01', TIMESTAMP'2004-01-01 12:01:01'),"
            + "  ('mystery', null, null, null, null, "
            + "   null, null, null),"
            + "  ('mystery', null, null, null, null, "
            + "   null, null, null)");
        stmt.executeUpdate(
            "analyze table WINES compute statistics for all columns");
    }

    protected void checkAbstract(
        FarragoPreparingStmt stmt,
        RelNode relBefore)
        throws Exception
    {
        RelOptPlanner planner = stmt.getPlanner();
        planner.setRoot(relBefore);

        // NOTE jvs 11-Apr-2006: This is a little iffy, because the
        // superclass is going to yank a lot out from under us when we return,
        // but then we're going to keep using rootRel after that.  Seems
        // to work, but...
        rootRel = planner.findBestExp();
    }

    private void transformQuery(
        HepProgram program,
        String sql)
        throws Exception
    {
        this.program = program;

        String explainQuery = "EXPLAIN PLAN FOR " + sql;

        checkQuery(explainQuery);
    }

    private void transformQuery(
        String sql)
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        transformQuery(
            programBuilder.createProgram(),
            sql);
    }

    protected void initPlanner(FarragoPreparingStmt stmt)
    {
        FarragoSessionPlanner planner = new FarragoTestPlanner(
                program,
                stmt);
        stmt.setPlanner(planner);
    }

    private void checkCost(double costExpected, RelOptCost costActual)
    {
        assertTrue(costActual != null);
        assertEquals(
            costExpected,
            costActual.getRows(),
            EPSILON);
        assertEquals(
            0,
            costActual.getCpu(),
            EPSILON);
        assertEquals(
            0,
            costActual.getIo(),
            EPSILON);
    }

    // queries table and checks the row count
    private void checkRowCount(String table, Double rowCount)
        throws Exception
    {
        transformQuery(
            "select * from " + table);
        checkDouble(
            rowCount,
            RelMetadataQuery.getRowCount(rootRel),
            EPSILON);

        RelNode tableScan = rootRel.getInput(0);
        tableStats = RelMetadataQuery.getStatistics(tableScan);
        rexBuilder = rootRel.getCluster().getRexBuilder();
    }

    private void checkColumn(
        int ordinal,
        RexNode rexPredicate,
        Double selectivity,
        Double cardinality)
    {
        SargIntervalSequence predicate = null;
        if (rexPredicate != null) {
            SargFactory sargFactory = new SargFactory(rexBuilder);
            SargRexAnalyzer analyzer = sargFactory.newRexAnalyzer();
            SargExpr expr = analyzer.analyze(rexPredicate).getExpr();
            predicate = FennelRelUtil.evaluateSargExpr(expr);
        }

        RelStatColumnStatistics columnStats =
            tableStats.getColumnStatistics(ordinal, predicate);
        checkDouble(
            selectivity,
            columnStats.getSelectivity(),
            EPSILON);
        checkDouble(
            cardinality,
            columnStats.getCardinality(),
            EPSILON);
    }

    private void checkDouble(Double expected, Double value, double epsilon)
    {
        if (expected == null) {
            assertNull(value);
        } else {
            assertEquals(expected, value, epsilon);
        }
    }

    // checks a single numeric predicate
    private void checkPredicate(
        int ordinal,
        SqlBinaryOperator operator,
        BigDecimal value,
        Double selectivity,
        Double cardinality)
    {
        RexNode rexPredicate = makeIntNode(ordinal, operator, value);
        checkColumn(ordinal, rexPredicate, selectivity, cardinality);
    }

    private RexNode makeIntNode(
        int ordinal,
        SqlBinaryOperator operator,
        BigDecimal value)
    {
        RelDataTypeField [] fields = rootRel.getRowType().getFields();
        RelDataType type = fields[ordinal].getType();
        RexNode rexPredicate =
            rexBuilder.makeCall(
                operator,
                rexBuilder.makeInputRef(type, ordinal + 1),
                rexBuilder.makeExactLiteral(value));
        return rexPredicate;
    }

    private RexNode makeStringNode(
        int ordinal,
        SqlBinaryOperator operator,
        String str)
    {
        RelDataTypeField [] fields = rootRel.getRowType().getFields();
        RelDataType type = fields[ordinal].getType();
        RexNode rexPredicate =
            rexBuilder.makeCall(
                operator,
                rexBuilder.makeInputRef(type, ordinal + 1),
                rexBuilder.makeCharLiteral(new NlsString(str, null, null)));
        return rexPredicate;
    }

    private RexNode makeOrNode(RexNode a, RexNode b)
    {
        RexNode rexPredicate =
            rexBuilder.makeCall(
                SqlStdOperatorTable.orOperator,
                a,
                b);
        return rexPredicate;
    }

    private RexNode makeAndNode(RexNode a, RexNode b)
    {
        RexNode rexPredicate =
            rexBuilder.makeCall(
                SqlStdOperatorTable.andOperator,
                a,
                b);
        return rexPredicate;
    }

    private RexNode makeSearchNode(
        int ordinal,
        RexLiteral value)
    {
        assert (value != null);

        RelDataTypeField [] fields = rootRel.getRowType().getFields();
        RelDataType type = fields[ordinal].getType();
        RexNode rexPredicate =
            rexBuilder.makeCall(
                SqlStdOperatorTable.equalsOperator,
                rexBuilder.makeInputRef(type, ordinal + 1),
                value);
        return rexPredicate;
    }

    private void searchColumn(
        int ordinal,
        RexLiteral value,
        Double selectivity,
        Double cardinality)
    {
        RexNode rexPredicate = makeSearchNode(ordinal, value);
        checkColumn(ordinal, rexPredicate, selectivity, cardinality);
    }

    public void testCumulativeCostTable()
        throws Exception
    {
        transformQuery(
            "select * from sales.emps");
        RelOptCost cost = RelMetadataQuery.getCumulativeCost(rootRel);

        // Cumulative cost is just table access, since we removed the
        // projection.  This also verifies that we override derivation
        // based on FtrsIndexScanRel.computeSelfCost, which would
        // return SALES_EMPS_ROWCOUNT*10 = 1000 instead.
        checkCost(
            SALES_EMPS_ROWCOUNT,
            cost);
    }

    public void testCumulativeCostFilter()
        throws Exception
    {
        transformQuery(
            "select * from lopt_metadata.emps where age=30");
        RelOptCost cost = RelMetadataQuery.getCumulativeCost(rootRel);

        // Cumulative cost is the filtered table access since the
        // predicate is sargable.
        double tableRowCount = COLSTORE_EMPS_ROWCOUNT;
        double selectedRowCount = tableRowCount * DEFAULT_SARGABLE_SELECTIVITY;
        checkCost(
            Math.sqrt(tableRowCount * selectedRowCount),
            cost);
    }

    public void testCumulativeCostFilteredProjection()
        throws Exception
    {
        transformQuery(
            "select * from (select age from lopt_metadata.emps) where age=30");
        RelOptCost cost = RelMetadataQuery.getCumulativeCost(rootRel);

        // Cumulative cost is same as for testCumulativeCostFilter
        // after expansion of projection
        double tableRowCount = COLSTORE_EMPS_ROWCOUNT;
        double selectedRowCount = tableRowCount * DEFAULT_SARGABLE_SELECTIVITY;
        checkCost(
            Math.sqrt(tableRowCount * selectedRowCount),
            cost);
    }

    public void testCumulativeCostFilteredProjectionSort()
        throws Exception
    {
        transformQuery(
            "select * from (select upper(name) as n from lopt_metadata.emps)"
            + " where n='ZELDA' order by 1");
        RelOptCost cost = RelMetadataQuery.getCumulativeCost(rootRel);

        // Cumulative cost is full table access plus the filtered rowcount for
        // the sort.
        double tableRowCount = COLSTORE_EMPS_ROWCOUNT;
        double sortRowCount = tableRowCount * .005;
        checkCost(
            tableRowCount + sortRowCount,
            cost);
    }

    public void testCumulativeCostUnion()
        throws Exception
    {
        transformQuery(
            "select name from sales.emps"
            + " union all select name from sales.emps");
        RelOptCost cost = RelMetadataQuery.getCumulativeCost(rootRel);

        // Cumulative cost is two table accesses; union is a freebie.
        double expected = 2 * SALES_EMPS_ROWCOUNT;
        checkCost(
            expected,
            cost);
    }

    public void testCumulativeCostSelfJoin()
        throws Exception
    {
        transformQuery(
            "select * from sales.emps e1 inner join sales.emps e2"
            + " on e1.empid=e2.empid");
        RelOptCost cost = RelMetadataQuery.getCumulativeCost(rootRel);

        // Cumulative cost is two table accesses plus join.
        double expected = 0;

        // table access
        expected += 2 * SALES_EMPS_ROWCOUNT;

        // worst-case join (two sides are balanced, so factor is 10)
        expected += 10 * SALES_EMPS_ROWCOUNT;
        checkCost(
            expected,
            cost);
    }

    public void testCumulativeCostSemiJoin()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(new PushFilterRule());
        programBuilder.addRuleInstance(new AddRedundantSemiJoinRule());
        transformQuery(
            programBuilder.createProgram(),
            "select * from sales.emps e1, sales.emps e2"
            + " where e1.empid=e2.empid and e2.name='Zelda'");
        RelOptCost cost = RelMetadataQuery.getCumulativeCost(rootRel);
        double expected = 0;

        // table accesses for e2 as semijoin input and join input, and e1 as
        // join input (none of these are treated as filtered because it's an
        // FTRS table)
        expected += 3 * SALES_EMPS_ROWCOUNT;

        // worst-case join (two sides are balanced due to semijoin filtering,
        // so factor is 10)
        expected += 10 * SALES_EMPS_ROWCOUNT * DEFAULT_EQUAL_SELECTIVITY;

        // semijoin overhead
        expected += SALES_EMPS_ROWCOUNT * DEFAULT_EQUAL_SELECTIVITY;
        checkCost(
            expected,
            cost);
    }

    public void testCumulativeCostAggregate()
        throws Exception
    {
        transformQuery(
            "select sum(empid) from sales.emps");
        RelOptCost cost = RelMetadataQuery.getCumulativeCost(rootRel);

        // Cumulative cost is table access plus aggregation.
        double expected = 0;

        // table access
        expected += SALES_EMPS_ROWCOUNT;

        // agg
        expected += 3 + SALES_EMPS_ROWCOUNT;
        checkCost(
            expected,
            cost);
    }

    // these negative tests are uninteresting, but they shouldn't fail
    public void testEmptyStatistics()
        throws Exception
    {
        FarragoJdbcEngineConnection farragoConnection =
            (FarragoJdbcEngineConnection) connection;
        FarragoSession session = farragoConnection.getSession();

        // no row count, as when table is yet to be analyzed
        stmt.executeUpdate(
            "create table NO_ROWCOUNT (i int)");
        checkRowCount("NO_ROWCOUNT", DEFAULT_ROWCOUNT);

        // no statistics for projections yet
        assertNull(RelMetadataQuery.getStatistics(rootRel));

        // no histogram, not a typical situation, but possible if we
        // dummy up values and only do row counts
        stmt.executeUpdate(
            "create table NO_HISTOGRAM (i int)");
        FarragoStatsUtil.setTableRowCount(session, "", "", "NO_HISTOGRAM", 50);
        checkRowCount("NO_HISTOGRAM", 50.0);
        checkColumn(0, null, null, null);

        // no predicate, should be able to query all data
        checkRowCount("EMPS", (double) COLSTORE_EMPS_ROWCOUNT);
        checkColumn(0, null, 1.0, 150.0);
        checkColumn(1, null, 1.0, 90000.0);

        // TODO: tests when table cardinality is missing
    }

    public void testFilteredStatistics()
        throws Exception
    {
        checkRowCount("EMPS", (double) COLSTORE_EMPS_ROWCOUNT);

        // the deptno distribution is 00,01,02,...
        // the predicate "deptno = 10" matches 1/2 first bar, 1/2 second bar
        checkPredicate(
            0,
            SqlStdOperatorTable.equalsOperator,
            BigDecimal.valueOf(10),
            0.01,
            1.0);

        // the query "deptno < 150" matches 00..13, 1/2 of bar14
        // total card 2+1+2+... (21) + 1 for bar14 (total 22)
        checkPredicate(
            0,
            SqlStdOperatorTable.lessThanOperator,
            BigDecimal.valueOf(150),
            0.145,
            22.0);

        // the query "deptno > 980" matches 1/2 of bar98, bar99
        // the bar99 contains only half the rows (50)
        checkPredicate(
            0,
            SqlStdOperatorTable.greaterThanOperator,
            BigDecimal.valueOf(980),
            0.01,
            2.0);

        // the query "deptno >= 980" matches 1/2 of bar97, 98-99
        // selectivity = 0.5+1+0.5 percent  cardinality = 0.5+2+1
        checkPredicate(
            0,
            SqlStdOperatorTable.greaterThanOrEqualOperator,
            BigDecimal.valueOf(980),
            0.02,
            3.5);

        // the query "deptno < 0" matches nothing
        checkPredicate(
            0,
            SqlStdOperatorTable.lessThanOperator,
            BigDecimal.ZERO,
            0.0,
            0.0);

        // "deptno > 990" matches 1/2 of the last bar
        checkPredicate(
            0,
            SqlStdOperatorTable.greaterThanOperator,
            BigDecimal.valueOf(990),
            0.0025,
            0.5);

        // "deptno = 7 or deptno < 5" supplies one point and one
        // range on bar0; should count as 3/4 bar
        RexNode equalsZero =
            makeIntNode(
                0,
                SqlStdOperatorTable.equalsOperator,
                BigDecimal.valueOf(7));
        RexNode lessThanOne =
            makeIntNode(
                0,
                SqlStdOperatorTable.lessThanOperator,
                BigDecimal.valueOf(5));
        RexNode pointAndRange = makeOrNode(equalsZero, lessThanOne);
        checkColumn(0, pointAndRange, 0.0075, 1.5);

        // "name = ABBY|ABIGAIL|ABOO or name < AC" matches the
        // entire bar AA, and has (3 points, 1 range) on bar AB.
        // The 3 points should contribute 3/900 while the range
        // provides half the remainder = 3/900 + 897/1800
        // Cardinality:
        //     Correction = 90000 / 990 = 90.909
        //     Bar values = (10-full bar) + (10-half bar)
        //                  + half of 3 w/o corrections
        RexNode abby =
            makeStringNode(
                1,
                SqlStdOperatorTable.equalsOperator,
                "ABBY");
        RexNode abigail =
            makeStringNode(
                1,
                SqlStdOperatorTable.equalsOperator,
                "ABIGAIL");
        RexNode aboo =
            makeStringNode(
                1,
                SqlStdOperatorTable.equalsOperator,
                "ABOO");
        RexNode ac =
            makeStringNode(
                1,
                SqlStdOperatorTable.lessThanOperator,
                "ABA");
        RexNode rexPredicate =
            makeOrNode(
                makeOrNode(
                    abby,
                    makeOrNode(aboo, abigail)),
                ac);
        checkColumn(1, rexPredicate, 0.015017, 1365.136363);
    }

    public void testTypedStatistics()
        throws Exception
    {
        checkRowCount("WINES", 5.0);

        RexLiteral value = rexBuilder.makeLiteral(true);
        searchColumn(1, value, 0.2, 1.0);
        BigDecimal price = new BigDecimal("15.00");
        value = rexBuilder.makeExactLiteral(price);
        searchColumn(2, value, 0.4, 1.0);
        value = rexBuilder.makeApproxLiteral(price);
        searchColumn(3, value, 0.4, 1.0);
        byte [] code = { 0 };
        value = rexBuilder.makeBinaryLiteral(code);
        searchColumn(4, value, 0.2, 1.0);

        Calendar cal = Calendar.getInstance();

        // note: this matches a value of each column
        // be careful of 0-indexed month, and timezone
        cal.clear();
        cal.setTimeZone(new SimpleTimeZone(0, "GMT+00:00"));
        cal.set(2002, 0, 1);
        value = rexBuilder.makeDateLiteral(cal);
        searchColumn(5, value, 0.2, 1.0);

        cal.clear();
        cal.setTimeZone(new SimpleTimeZone(0, "GMT+00:00"));
        cal.set(Calendar.HOUR_OF_DAY, 12);
        cal.set(Calendar.MINUTE, 1);
        cal.set(Calendar.SECOND, 1);
        value = rexBuilder.makeTimeLiteral(cal, 3);
        searchColumn(6, value, 0.2, 1.0);

        cal.clear();
        cal.setTimeZone(new SimpleTimeZone(0, "GMT+00:00"));
        cal.set(2002, 0, 1, 12, 1, 1);
        value = rexBuilder.makeTimestampLiteral(cal, 3);
        searchColumn(7, value, 0.4, 1.0);

        // all of first bar + 1/2 of second bar
        value = rexBuilder.constantNull();
        searchColumn(7, value, 0.3, 1.0);

        // TODO: in and not in operators, but does IN work?
    }

    private void checkRowCountJoin(String sql, Double expected)
        throws Exception
    {
        // these test also test the SemiJoinRel case, as it's easier to test
        // the as part of regular joins
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(new PushFilterRule());
        programBuilder.addRuleInstance(new AddRedundantSemiJoinRule());
        transformQuery(
            programBuilder.createProgram(),
            sql);

        Double result = RelMetadataQuery.getRowCount(rootRel);
        assertTrue(result != null);
        assertEquals(expected, result, EPSILON);
    }

    public void testRowCountJoinFtrs()
        throws Exception
    {
        checkRowCountJoin(
            "select * from sales.emps e, sales.depts d "
            + "where e.deptno = d.deptno and d.name = 'foo'",
            SALES_EMPS_ROWCOUNT * DEFAULT_EQUAL_SELECTIVITY);
    }

    public void testRowCountJoinLcs()
        throws Exception
    {
        checkRowCountJoin(
            "select * from emps e, depts d "
            + "where e.deptno = d.deptno and d.dname = 'foo'",
            COLSTORE_EMPS_ROWCOUNT * 1.0 / COLSTORE_DEPTS_ROWCOUNT);
    }

    public void testRowCountJoinNoColStats()
        throws Exception
    {
        FarragoJdbcEngineConnection farragoConnection =
            (FarragoJdbcEngineConnection) connection;
        FarragoSession session = farragoConnection.getSession();

        stmt.executeUpdate("create table t1(a int, b int)");
        stmt.executeUpdate("create table t2(a int, b int)");
        FarragoStatsUtil.setTableRowCount(session, "", "", "T1", 1000);
        FarragoStatsUtil.setTableRowCount(session, "", "", "T2", 20);
        checkRowCountJoin(
            "select * from t1, t2 where t1.a = t2.a and t2.b = 1",
            1000 * DEFAULT_SARGABLE_SELECTIVITY);
    }

    public void testRowCountJoinNonEquiJoin()
        throws Exception
    {
        checkRowCountJoin(
            "select * from emps e, depts d "
            + "where e.deptno > d.deptno",
            COLSTORE_EMPS_ROWCOUNT * COLSTORE_DEPTS_ROWCOUNT * .5);
    }

    public void testSelectivityJoin()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(new PushFilterRule());
        transformQuery(
            programBuilder.createProgram(),
            "select * from "
            + "(select * from emps e, depts d where e.deptno = d.deptno) "
            + "where name = 'foo'");
        Double result = RelMetadataQuery.getSelectivity(rootRel, null);
        assert (result != null);
        assertEquals(DEFAULT_EQUAL_SELECTIVITY, result, EPSILON);
    }

    private void checkDistinctRowCount(
        String sql,
        BitSet groupKey,
        Double expected)
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        transformQuery(
            programBuilder.createProgram(),
            sql);

        Double result =
            RelMetadataQuery.getDistinctRowCount(
                rootRel,
                groupKey,
                null);
        if (expected != null) {
            assertTrue(result != null);
            assertEquals(
                expected,
                result.doubleValue(),
                EPSILON);
        } else {
            assertTrue(expected == null);
        }
    }

    public void testDistinctRowCountTabNoFilter()
        throws Exception
    {
        // count the number of distinct values in "name"
        BitSet groupKey = new BitSet();
        groupKey.set(1);
        double expected =
            RelMdUtil.numDistinctVals((double) 90000,
                (double) COLSTORE_EMPS_ROWCOUNT);
        checkDistinctRowCount(
            "select * from emps",
            groupKey,
            expected);
    }

    public void testDistinctRowCountSargableFilter()
        throws Exception
    {
        BitSet groupKey = new BitSet();
        groupKey.set(0);
        checkDistinctRowCount(
            "select * from emps where deptno = 10",
            groupKey,
            new Double(1));
    }

    public void testDistinctRowCountNonSargableFilter()
        throws Exception
    {
        BitSet groupKey = new BitSet();
        groupKey.set(1);
        double expected =
            RelMdUtil.numDistinctVals(
                90000 * DEFAULT_EQUAL_SELECTIVITY,
                COLSTORE_EMPS_ROWCOUNT * DEFAULT_EQUAL_SELECTIVITY);
        checkDistinctRowCount(
            "select * from emps where upper(name) = 'FOO'",
            groupKey,
            expected);
    }

    public void testDistinctRowCountSargAndNonSargFilters()
        throws Exception
    {
        // note that 2 bits are set, so to compute # distinct values, end
        // up multiplying cardinality of 2 columns
        BitSet groupKey = new BitSet();
        groupKey.set(0);
        groupKey.set(1);
        double expected =
            RelMdUtil.numDistinctVals(
                (double) COLSTORE_EMPS_ROWCOUNT * .145
                * DEFAULT_EQUAL_SELECTIVITY,
                (double) COLSTORE_EMPS_ROWCOUNT * .145
                * DEFAULT_EQUAL_SELECTIVITY);

        // selectivity of deptno < 150 is .145
        checkDistinctRowCount(
            "select * from emps where deptno < 150 and upper(name) = 'FOO'",
            groupKey,
            expected);
    }

    public void testDistinctRowCountNoStatsFilter()
        throws Exception
    {
        BitSet groupKey = new BitSet();
        groupKey.set(2);

        // if no stats are available, null is returned
        checkDistinctRowCount(
            "select * from emps where age = 40",
            groupKey,
            null);
    }

    public void testDistinctRowCountUniqueTab()
        throws Exception
    {
        stmt.executeUpdate(
            "create table tabwithuniquekey(a int not null constraint u unique,"
            + "b int)");
        BitSet groupKey = new BitSet();
        groupKey.set(0);
        groupKey.set(1);
        double expected =
            RelMdUtil.numDistinctVals(
                DEFAULT_ROWCOUNT,
                DEFAULT_ROWCOUNT);
        checkDistinctRowCount(
            "select * from tabwithuniqueKey",
            groupKey,
            expected);
    }

    public void testSelectivityLcsTableNoStats()
        throws Exception
    {
        // NOTE zfong 4/21/06 - there are no other explicit tests for
        // selectivity on LCS tables, as those codepaths are already exercised
        // by the tests above for cost and distinct row count
        stmt.executeUpdate(
            "create table noStats(a int, b int)");
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        transformQuery(
            programBuilder.createProgram(),
            "select * from noStats where a = 1");

        Double result = RelMetadataQuery.getSelectivity(rootRel, null);
        assertTrue(result != null);
        assertEquals(
            DEFAULT_SARGABLE_SELECTIVITY,
            result.doubleValue());
    }

    public void testDistinctRowCountProjectedLcsTable()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(new LcsTableProjectionRule());
        transformQuery(
            programBuilder.createProgram(),
            "select name from emps");

        BitSet groupKey = new BitSet();
        groupKey.set(0);
        Double result =
            RelMetadataQuery.getDistinctRowCount(
                rootRel,
                groupKey,
                null);
        double expected = 90000;
        assertEquals(
            expected,
            result.doubleValue(),
            EPSILON);
    }

    private void testPopulationProjectedLcsTable(
        String sql,
        BitSet groupKey,
        double expected,
        double epsilon)
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(new LcsTableProjectionRule());
        transformQuery(
            programBuilder.createProgram(),
            sql);

        Double result = RelMetadataQuery.getPopulationSize(rootRel, groupKey);

        assertTrue(result != null);
        assertEquals(
            expected,
            result.doubleValue(),
            epsilon);
    }

    public void testPopulationNoProjExprs()
        throws Exception
    {
        // get the population of (deptno, name); note that the bits are
        // the projected ordinals
        BitSet groupKey = new BitSet();
        groupKey.set(1);
        groupKey.set(2);

        double expected =
            RelMdUtil.numDistinctVals(
                90000 * 150.0,
                (double) COLSTORE_EMPS_ROWCOUNT);

        testPopulationProjectedLcsTable(
            "select age, deptno, name from emps",
            groupKey,
            expected,
            EPSILON);
    }

    public void testPopulationProjFuncExpr()
        throws Exception
    {
        BitSet groupKey = new BitSet();
        groupKey.set(0);
        groupKey.set(1);

        // 150 distinct values in deptno
        Double nonProjExpr =
            RelMdUtil.numDistinctVals(
                150.0,
                (double) COLSTORE_EMPS_ROWCOUNT);

        // 90000 distinct values in name
        Double projExpr =
            RelMdUtil.numDistinctVals(
                90000.0,
                (double) COLSTORE_EMPS_ROWCOUNT);
        projExpr =
            RelMdUtil.numDistinctVals(
                projExpr,
                (double) COLSTORE_EMPS_ROWCOUNT);
        double expected =
            RelMdUtil.numDistinctVals(
                nonProjExpr * projExpr,
                (double) COLSTORE_EMPS_ROWCOUNT);

        testPopulationProjectedLcsTable(
            "select deptno, upper(name) from emps",
            groupKey,
            expected,
            EPSILON);
    }

    public void testPopulationProjTimesExpr()
        throws Exception
    {
        BitSet groupKey = new BitSet();
        groupKey.set(0);
        groupKey.set(1);

        // 90000 distinct values in name
        double expected =
            RelMdUtil.numDistinctVals(
                90000.0,
                (double) COLSTORE_EMPS_ROWCOUNT);
        expected =
            RelMdUtil.numDistinctVals(
                expected,
                (double) COLSTORE_EMPS_ROWCOUNT);

        // 150 distinct values in deptno
        double nonProjExpr =
            RelMdUtil.numDistinctVals(
                150.0 * 150.0,
                (double) COLSTORE_EMPS_ROWCOUNT);
        expected *= nonProjExpr;
        expected =
            RelMdUtil.numDistinctVals(
                expected,
                (double) COLSTORE_EMPS_ROWCOUNT);

        testPopulationProjectedLcsTable(
            "select deptno * deptno, name from emps",
            groupKey,
            expected,
            1.0);
    }

    public void testUniqueKeysProjectedLcsTable()
        throws Exception
    {
        stmt.executeUpdate(
            "create table tab("
            + "c0 int not null,"
            + "c1 int not null,"
            + "c2 int not null,"
            + "c3 int not null,"
            + "c4 int,"
            + "constraint primkey primary key(c4),"
            + "constraint uniquekey1 unique(c2, c3),"
            + "constraint uniquekey2 unique(c0, c1))");

        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(new LcsTableProjectionRule());
        transformQuery(
            programBuilder.createProgram(),
            "select c4, c2, c1, c0 from tab");

        // unique keys relative to projection are (0) and (3, 2);
        // uniquekey1 is not included because c3 is not projected
        Set<BitSet> expected = new HashSet<BitSet>();

        BitSet uniqKey = new BitSet();
        uniqKey.set(0);
        expected.add(uniqKey);

        uniqKey = new BitSet();
        uniqKey.set(3);
        uniqKey.set(2);
        expected.add(uniqKey);

        Set<BitSet> result = RelMetadataQuery.getUniqueKeys(rootRel);
        assertTrue(result.equals(expected));
    }
}

// End LoptMetadataTest.java
