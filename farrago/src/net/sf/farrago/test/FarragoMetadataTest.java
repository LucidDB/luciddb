/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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

import java.util.*;

import junit.framework.*;

import net.sf.farrago.query.*;
import net.sf.farrago.session.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.relopt.hep.*;


/**
 * FarragoMetadata tests the relational expression metadata queries that require
 * additional sql statement support in order to test, above and beyond what can
 * be tested in {@link org.eigenbase.test.RelMetadataTest}.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class FarragoMetadataTest
    extends FarragoSqlToRelTestBase
{

    //~ Static fields/initializers ---------------------------------------------

    private static boolean doneStaticSetup;

    private static final double EPSILON = 1.0e-5;

    private static final double TAB_ROWCOUNT = 100.0;

    private static final double DEFAULT_EQUAL_SELECTIVITY = 0.15;

    private static final double DEFAULT_EQUAL_SELECTIVITY_SQUARED =
        DEFAULT_EQUAL_SELECTIVITY * DEFAULT_EQUAL_SELECTIVITY;

    private static final double DEFAULT_COMP_SELECTIVITY = 0.5;

    //~ Instance fields --------------------------------------------------------

    private HepProgram program;

    private RelNode rootRel;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoMetadataTest object.
     *
     * @param testName JUnit test name
     *
     * @throws Exception .
     */
    public FarragoMetadataTest(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ----------------------------------------------------------------

    // implement TestCase
    public static Test suite()
    {
        return wrappedSuite(FarragoMetadataTest.class);
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

        stmt.executeUpdate(
            "create schema farrago_metadata");
        stmt.executeUpdate(
            "set schema 'farrago_metadata'");

        stmt.executeUpdate(
            "create table tab("
            + "c0 int,"
            + "c1 int not null,"
            + "c2 int not null,"
            + "c3 int,"
            + "c4 int,"
            + "constraint primkey primary key(c0),"
            + "constraint unique_notnull unique(c1, c2),"
            + "constraint unique_null unique(c2, c3))");
        stmt.executeUpdate(
            "create index idx on tab(c4)");
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

    protected void initPlanner(FarragoPreparingStmt stmt)
    {
        FarragoSessionPlanner planner =
            new FarragoTestPlanner(
                program,
                stmt) {
                // TODO jvs 11-Apr-2006: eliminate this once we switch to Hep
                // permanently for LucidDB; this is to make sure that
                // LoptMetadataProvider gets used for the duration of this test
                // regardless of the LucidDbSessionFactory.USE_HEP flag setting.

                // implement RelOptPlanner
                public void registerMetadataProviders(
                    ChainedRelMetadataProvider chain)
                {
                    super.registerMetadataProviders(chain);
                }
            };
        stmt.setPlanner(planner);
    }

    private void checkPopulation(
        String sql,
        BitSet groupKey,
        Double expected)
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        transformQuery(
            programBuilder.createProgram(),
            sql);

        Double result = RelMetadataQuery.getPopulationSize(
                rootRel,
                groupKey);
        if (expected != null) {
            assertEquals(
                expected,
                result.doubleValue());
        } else {
            assertEquals(expected, null);
        }
    }

    public void testPopulationTabPrimary()
        throws Exception
    {
        BitSet groupKey = new BitSet();

        // c0 has a primary key on it
        groupKey.set(0);
        groupKey.set(4);
        checkPopulation("select * from tab", groupKey, TAB_ROWCOUNT);
    }

    public void testPopulationTabUniqueNotNull()
        throws Exception
    {
        BitSet groupKey = new BitSet();

        // c1, c2 have a unique constraint on them
        groupKey.set(1);
        groupKey.set(2);
        groupKey.set(3);
        checkPopulation("select * from tab", groupKey, TAB_ROWCOUNT);
    }

    public void testPopulationTabUniqueNull()
        throws Exception
    {
        BitSet groupKey = new BitSet();

        // c2, c3 have a unique constraint on them, but c3 is null, so the
        // result should be null
        groupKey.set(2);
        groupKey.set(3);
        checkPopulation("select * from tab", groupKey, null);
    }

    public void testPopulationFilter()
        throws Exception
    {
        BitSet groupKey = new BitSet();

        // c1, c2 have a unique constraint on them
        groupKey.set(1);
        groupKey.set(2);
        groupKey.set(3);
        double expected =
            RelMdUtil.numDistinctVals(
                TAB_ROWCOUNT,
                TAB_ROWCOUNT * DEFAULT_EQUAL_SELECTIVITY);
        checkPopulation(
            "select * from tab where c4 = 1",
            groupKey,
            expected);
    }

    public void testPopulationSort()
        throws Exception
    {
        BitSet groupKey = new BitSet();

        // c0 has a primary key on it
        groupKey.set(0);
        groupKey.set(4);
        checkPopulation(
            "select * from tab order by c4",
            groupKey,
            TAB_ROWCOUNT);
    }

    public void testPopulationJoin()
        throws Exception
    {
        // this test will test both joins and semijoins
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(new PushFilterRule());
        programBuilder.addRuleInstance(new AddRedundantSemiJoinRule());
        transformQuery(
            programBuilder.createProgram(),
            "select * from tab t1, tab t2 where t1.c4 = t2.c4");
        BitSet groupKey = new BitSet();

        // primary key on c0, and unique constraint on c1, c2; set the mask
        // so c0 originates from t1 and c1, c2 originate from t2
        groupKey.set(0);
        groupKey.set(5 + 1);
        groupKey.set(5 + 2);
        Double result = RelMetadataQuery.getPopulationSize(
                rootRel,
                groupKey);
        double expected =
            RelMdUtil.numDistinctVals(
                TAB_ROWCOUNT * TAB_ROWCOUNT,
                TAB_ROWCOUNT * TAB_ROWCOUNT * DEFAULT_EQUAL_SELECTIVITY);
        expected =
            RelMdUtil.numDistinctVals(
                expected,
                TAB_ROWCOUNT * TAB_ROWCOUNT * DEFAULT_EQUAL_SELECTIVITY);
        assertEquals(
            expected,
            result.doubleValue());
    }

    public void testPopulationUnion()
        throws Exception
    {
        BitSet groupKey = new BitSet();
        groupKey.set(0);
        double expected =
            RelMdUtil.numDistinctVals(
                2 * TAB_ROWCOUNT,
                2 * TAB_ROWCOUNT);
        checkPopulation(
            "select * from (select * from tab union all select * from tab)",
            groupKey,
            expected);
    }

    public void testPopulationAgg()
        throws Exception
    {
        BitSet groupKey = new BitSet();
        groupKey.set(0);
        groupKey.set(1);
        double expected = RelMdUtil.numDistinctVals(TAB_ROWCOUNT, TAB_ROWCOUNT);
        expected = RelMdUtil.numDistinctVals(TAB_ROWCOUNT, expected);
        checkPopulation(
            "select c0, count(*) from tab group by c0",
            groupKey,
            expected);
    }

    private void checkUniqueKeys(
        String sql,
        Set<BitSet> expected)
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        transformQuery(
            programBuilder.createProgram(),
            sql);

        Set<BitSet> result = RelMetadataQuery.getUniqueKeys(rootRel);
        assertTrue(result.equals(expected));
    }

    public void testUniqueKeysTab()
        throws Exception
    {
        Set<BitSet> expected = new HashSet<BitSet>();

        BitSet primKey = new BitSet();
        primKey.set(0);
        expected.add(primKey);

        BitSet uniqKey = new BitSet();
        uniqKey.set(1);
        uniqKey.set(2);
        expected.add(uniqKey);

        // this test case tests project, sort, filter, and table
        checkUniqueKeys(
            "select * from tab where c0 = 1 order by c1",
            expected);
    }

    public void testUniqueKeysAgg()
        throws Exception
    {
        Set<BitSet> expected = new HashSet<BitSet>();

        BitSet groupKey = new BitSet();
        groupKey.set(0);
        groupKey.set(1);
        expected.add(groupKey);

        checkUniqueKeys(
            "select c2, c4, count(*) from tab group by c2, c4",
            expected);
    }

    private void checkUniqueKeysJoin(String sql, Set<BitSet> expected)
        throws Exception
    {
        // tests that call this method will test both joins and semijoins
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(new PushFilterRule());
        programBuilder.addRuleInstance(new AddRedundantSemiJoinRule());
        transformQuery(
            programBuilder.createProgram(),
            sql);

        Set<BitSet> result = RelMetadataQuery.getUniqueKeys(rootRel);
        assertTrue(result.equals(expected));
    }

    public void testUniqueKeysJoinLeft()
        throws Exception
    {
        Set<BitSet> expected = new HashSet<BitSet>();

        // left side has a unique join key, so the right side unique keys
        // should be returned
        BitSet keys = new BitSet();
        keys.set(5 + 0);
        expected.add(keys);

        keys = new BitSet();
        keys.set(5 + 1);
        keys.set(5 + 2);
        expected.add(keys);

        checkUniqueKeysJoin(
            "select * from tab t1, tab t2 where t1.c0 = t2.c3",
            expected);
    }

    public void testUniqueKeysJoinRight()
        throws Exception
    {
        Set<BitSet> expected = new HashSet<BitSet>();

        // right side has a unique join key, so the left side unique keys
        // should be returned
        BitSet keys = new BitSet();
        keys.set(0);
        expected.add(keys);

        keys = new BitSet();
        keys.set(1);
        keys.set(2);
        expected.add(keys);

        checkUniqueKeysJoin(
            "select * from tab t1, tab t2 where t1.c3 = t2.c1 and t1.c4 = t2.c2",
            expected);
    }

    public void testUniqueKeysJoinNotUnique()
        throws Exception
    {
        // no equijoins on unique keys so there should be no unique keys
        Set<BitSet> expected = new HashSet<BitSet>();

        checkUniqueKeysJoin(
            "select * from tab t1, tab t2 where t1.c3 = t2.c3",
            expected);
    }

    private void checkDistinctRowCount(
        RelNode rel,
        BitSet groupKey,
        Double expected)
    {
        Double result =
            RelMetadataQuery.getDistinctRowCount(
                rel,
                groupKey,
                null);
        if (expected == null) {
            assertTrue(result == null);
        } else {
            assertTrue(result != null);
            assertEquals(
                expected,
                result.doubleValue(),
                EPSILON);
        }
    }

    public void testDistinctRowCountFilter()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        transformQuery(
            programBuilder.createProgram(),
            "select * from tab where c1 = 1");
        BitSet groupKey = new BitSet();
        groupKey.set(0);
        double expected =
            RelMdUtil.numDistinctVals(
                TAB_ROWCOUNT * DEFAULT_EQUAL_SELECTIVITY,
                TAB_ROWCOUNT * DEFAULT_EQUAL_SELECTIVITY);
        checkDistinctRowCount(rootRel, groupKey, expected);
    }

    public void testDistinctRowCountSort()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        transformQuery(
            programBuilder.createProgram(),
            "select * from tab where c1 = 1 order by c2");
        BitSet groupKey = new BitSet();
        groupKey.set(0);
        double expected =
            RelMdUtil.numDistinctVals(
                TAB_ROWCOUNT * DEFAULT_EQUAL_SELECTIVITY,
                TAB_ROWCOUNT * DEFAULT_EQUAL_SELECTIVITY);
        checkDistinctRowCount(rootRel, groupKey, expected);
    }

    public void testDistinctRowCountUnion()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        transformQuery(
            programBuilder.createProgram(),
            "select * from (select * from tab union all select * from tab) "
            + "where c1 = 10");
        BitSet groupKey = new BitSet();
        groupKey.set(0);

        // compute the number of distinct values from applying the filter
        double expected =
            RelMdUtil.numDistinctVals(
                TAB_ROWCOUNT * DEFAULT_EQUAL_SELECTIVITY,
                TAB_ROWCOUNT);

        // then compute the number of distinct values for each union
        expected =
            RelMdUtil.numDistinctVals(
                expected * 2,
                TAB_ROWCOUNT * DEFAULT_EQUAL_SELECTIVITY * 2);
        checkDistinctRowCount(rootRel, groupKey, expected);
    }

    public void testDistinctRowCountAgg()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        transformQuery(
            programBuilder.createProgram(),
            "select c0, count(*) from tab where c0 > 0 group by c0 "
            + "having count(*) = 0");
        BitSet groupKey = new BitSet();
        groupKey.set(0);
        groupKey.set(1);

        // number of distinct values from applying the filter
        double expected =
            RelMdUtil.numDistinctVals(
                TAB_ROWCOUNT * DEFAULT_COMP_SELECTIVITY,
                TAB_ROWCOUNT * DEFAULT_COMP_SELECTIVITY);

        // number of distinct values from applying the having clause
        //
        // REVIEW zfong 6/22/06 - I'm not able to get this test to pass
        // without applying the where clause filter twice
        expected =
            RelMdUtil.numDistinctVals(
                expected,
                expected * DEFAULT_EQUAL_SELECTIVITY
                * DEFAULT_COMP_SELECTIVITY);
        checkDistinctRowCount(rootRel, groupKey, expected);
    }

    public void testDistinctRowCountJoin()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(new PushFilterRule());
        programBuilder.addRuleInstance(new AddRedundantSemiJoinRule());
        transformQuery(
            programBuilder.createProgram(),
            "select * from tab t1, tab t2 where t1.c0 = t2.c0 and t2.c0 = 1");
        BitSet groupKey = new BitSet();
        groupKey.set(0);
        groupKey.set(5 + 0);
        Double result =
            RelMetadataQuery.getDistinctRowCount(
                rootRel,
                groupKey,
                null);

        // We need to multiply the selectivity three times to account for:
        // - table level filter on t2
        // - semijoin filter on t1
        // - join filter
        // Because this test does not exercise LucidEra logic that accounts
        // for the double counting of semijoins, that is why the selectivity
        // is multiplied three times

        // number of distinct rows from the join; first arg corresponds to
        // the number of rows from applying the table filter and semijoin;
        // second is the number of rows from applying all three filters
        double expected =
            RelMdUtil.numDistinctVals(
                TAB_ROWCOUNT * TAB_ROWCOUNT * DEFAULT_EQUAL_SELECTIVITY_SQUARED,
                TAB_ROWCOUNT * TAB_ROWCOUNT * DEFAULT_EQUAL_SELECTIVITY_SQUARED
                * DEFAULT_EQUAL_SELECTIVITY);

        // number of distinct rows from the topmost project
        expected =
            RelMdUtil.numDistinctVals(
                expected,
                TAB_ROWCOUNT * TAB_ROWCOUNT * DEFAULT_EQUAL_SELECTIVITY_SQUARED
                * DEFAULT_EQUAL_SELECTIVITY);
        assertTrue(result != null);
        assertEquals(
            expected,
            result.doubleValue(),
            EPSILON);
    }
}

// End FarragoMetadataTest.java
