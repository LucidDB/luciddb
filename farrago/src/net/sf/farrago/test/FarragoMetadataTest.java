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

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.relopt.hep.*;
import org.eigenbase.rex.*;

import net.sf.farrago.query.*;
import net.sf.farrago.session.*;

import junit.framework.*;

import java.util.*;

/**
 * FarragoMetadata tests the relational expression metadata queries
 * that require additional sql statement support in order to test, above
 * and beyond what can be tested in {@link org.eigenbase.test.RelMetadataTest}.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class FarragoMetadataTest extends FarragoSqlToRelTestBase
{
    private HepProgram program;

    private RelNode rootRel;
    
    private static boolean doneStaticSetup;
    
    private static final double EPSILON = 1.0e-5;
    
    private static final double TAB_ROWCOUNT = 100.0;
    
    private static final double DEFAULT_EQUAL_SELECTIVITY = 0.15;
    
    private static final double DEFAULT_EQUAL_SELECTIVITY_SQUARED =
        DEFAULT_EQUAL_SELECTIVITY * DEFAULT_EQUAL_SELECTIVITY;
    
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
            "create table tab(" +
            "c0 int," +
            "c1 int not null," +
            "c2 int not null,"+
            "c3 int," +
            "c4 int," +
            "constraint primkey primary key(c0)," +
            "constraint unique_notnull unique(c1, c2)," +
            "constraint unique_null unique(c2, c3))");
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

        String explainQuery =
            "EXPLAIN PLAN FOR " + sql;

        checkQuery(explainQuery);
    }
    
    protected void initPlanner(FarragoPreparingStmt stmt)
    {
        FarragoSessionPlanner planner = new FarragoTestPlanner(
            program,
            stmt)
            {
                // TODO jvs 11-Apr-2006: eliminate this once we switch to Hep
                // permanently for LucidDB; this is to make sure that
                // LoptMetadataProvider gets used for the duration of this test
                // regardless of the LucidDbSessionFactory.USE_HEP flag
                // setting.
                
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
        String sql, BitSet groupKey, Double expected, boolean callChild)
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        transformQuery(
            programBuilder.createProgram(), sql);
     
        RelNode rel;
        rel = (callChild) ? ((ProjectRel) rootRel).getChild() : rootRel;
        Double result = RelMetadataQuery.getPopulationSize(
            rel, groupKey);
        if (expected != null) {
            assertEquals(expected, result.doubleValue());
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
        checkPopulation("select * from tab", groupKey, TAB_ROWCOUNT, false);
    }
    
    public void testPopulationTabUniqueNotNull()
        throws Exception
    {
        BitSet groupKey = new BitSet();
        // c1, c2 have a unique constraint on them
        groupKey.set(1);
        groupKey.set(2);
        groupKey.set(3);
        checkPopulation("select * from tab", groupKey, TAB_ROWCOUNT, false);
    }
    
    public void testPopulationTabUniqueNull()
        throws Exception
    {
        BitSet groupKey = new BitSet();
        // c2, c3 have a unique constraint on them, but c3 is null, so the
        // result should be null
        groupKey.set(2);
        groupKey.set(3);
        checkPopulation("select * from tab", groupKey, null, false);
    }
    
    public void testPopulationFilter()
        throws Exception
    {
        BitSet groupKey = new BitSet();
        // c1, c2 have a unique constraint on them
        groupKey.set(1);
        groupKey.set(2);
        groupKey.set(3);
        // filters are ignored so the full rowcount should be returned
        checkPopulation(
            "select * from tab where c4 = 1", groupKey, TAB_ROWCOUNT, true);
    }
    
    public void testPopulationSort()
        throws Exception
    {
        BitSet groupKey = new BitSet();
        // c0 has a primary key on it
        groupKey.set(0);
        groupKey.set(4);
        checkPopulation(
            "select * from tab order by c4", groupKey, TAB_ROWCOUNT, false);
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
        groupKey.set(5+1);
        groupKey.set(5+2);
        Double result = RelMetadataQuery.getPopulationSize(
            ((ProjectRel) rootRel).getChild(), groupKey);
        assertEquals(TAB_ROWCOUNT * TAB_ROWCOUNT, result.doubleValue());
    }
    
    public void testPopulationUnion()
        throws Exception
    {
        BitSet groupKey = new BitSet();
        groupKey.set(0);
        checkPopulation(
            "select * from (select * from tab union all select * from tab)",
            groupKey, 2* TAB_ROWCOUNT, true);
    }
    
    private void checkUniqueKeys(
        String sql, Set<BitSet> expected)
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        transformQuery(
            programBuilder.createProgram(), sql);
     
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
    
    private void checkUniqueKeysJoin(String sql, Set<BitSet> expected)
        throws Exception
    {
        // tests that call this method will test both joins and semijoins
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(new PushFilterRule());
        programBuilder.addRuleInstance(new AddRedundantSemiJoinRule());
        transformQuery(programBuilder.createProgram(), sql);
        
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
        keys.set(5+0);
        expected.add(keys);
        
        keys = new BitSet();
        keys.set(5+1);
        keys.set(5+2);
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
        RelNode rel, BitSet groupKey, RexNode predicate, Double expected)
    {
        Double result = RelMetadataQuery.getDistinctRowCount(
            rel, groupKey, predicate);
        if (expected == null) {
            assertTrue(result == null);
        } else {
            assertTrue(result != null);
            assertEquals(expected, result.doubleValue());
        }
    }
    
    public void testDistinctRowCountFilter()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        transformQuery(
            programBuilder.createProgram(),
            "select * from tab where c1 = 1");
        ProjectRel projectRel = (ProjectRel) rootRel;
        FilterRel filterRel = (FilterRel) projectRel.getChild();
        BitSet groupKey = new BitSet();
        groupKey.set(0);
        checkDistinctRowCount(
            filterRel, groupKey, filterRel.getCondition(),
            TAB_ROWCOUNT * DEFAULT_EQUAL_SELECTIVITY);
    }
    
    public void testDistinctRowCountSort()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        transformQuery(
            programBuilder.createProgram(),
            "select * from tab where c1 = 1 order by c2");
        SortRel sortRel = (SortRel) rootRel;
        ProjectRel projectRel = (ProjectRel) sortRel.getChild();
        FilterRel filterRel = (FilterRel) projectRel.getChild();
        BitSet groupKey = new BitSet();
        groupKey.set(0);
        // REVIEW zfong 4/19/06 - the predicate is getting applied
        // twice in the calculation; I'm not sure how to avoid this
        checkDistinctRowCount(
            sortRel, groupKey, filterRel.getCondition(), 
            TAB_ROWCOUNT * DEFAULT_EQUAL_SELECTIVITY_SQUARED);
    }
    
    public void testDistinctRowCountUnion()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        transformQuery(
            programBuilder.createProgram(),
            "select * from (select * from tab union all select * from tab) " +
                "where c1 = 10");
        ProjectRel projectRel = (ProjectRel) rootRel;
        FilterRel filterRel = (FilterRel) projectRel.getChild();
        BitSet groupKey = new BitSet();
        groupKey.set(0);
        // UnionRel is the child of the FilterRel
        checkDistinctRowCount(
            filterRel.getChild(), groupKey, filterRel.getCondition(),
            2 * (TAB_ROWCOUNT * DEFAULT_EQUAL_SELECTIVITY));
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
        groupKey.set(5+0);
        ProjectRel projectRel = (ProjectRel) rootRel;
        Double result = RelMetadataQuery.getDistinctRowCount(
            projectRel.getChild(), groupKey, null);
        
        Double domainSize =
            TAB_ROWCOUNT * TAB_ROWCOUNT *
                DEFAULT_EQUAL_SELECTIVITY * DEFAULT_EQUAL_SELECTIVITY;
        Double numSelected = domainSize * DEFAULT_EQUAL_SELECTIVITY;
        // The calculation below mimics RelMdUtil.numDistinctVals().  
        // We need to multiply the selectivity three times to account for:
        // - table level filter on t2
        // - semijoin filter on t1
        // - join filter
        // Because this test does not exercise LucidEra logic that accounts
        // for the double counting of semijoins, that is why the selectivity
        // is multiplied three times
        Double expected =
            (1.0 - Math.exp(-1 * numSelected / domainSize)) * domainSize;
        assertTrue(result != null);
        assertEquals(expected, result.doubleValue(), EPSILON);
    }
}

// End FarragoMetadataTest.java
