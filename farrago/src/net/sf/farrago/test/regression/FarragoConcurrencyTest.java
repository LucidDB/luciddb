/*
// Farrago is a relational database management system.
// (C) Copyright 2004-2004, Disruptive Tech
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
package net.sf.farrago.test.regression;

import junit.framework.Test;

/**
 * FarragoConcurrencyTest executes a variety of SQL DML and DDL
 * commands via a multi-threaded test harness in an effort to detect
 * errors in concurrent execution.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public class FarragoConcurrencyTest
    extends FarragoConcurrencyTestCase
{
    public FarragoConcurrencyTest(String name)
        throws Exception
    {
        super(name);
    }

    public static Test suite()
    {
        return wrappedSuite(FarragoConcurrencyTest.class);
    }

    /**
     * Test concurrent "explain plan" for a simple select statement.
     */
    public void testConcurrentExplain()
        throws Exception
    {
        FarragoTestCommandGenerator cmdGen = newCommandGenerator();

        String sql = "explain plan for select * from sales.depts";

        // Repeat a few times to improve odds of getting concurrent
        // planning.
        for (int i = 1; i <= 10; i++) {
            cmdGen.addExplainCommand(1, i, sql);
            cmdGen.addExplainCommand(2, i, sql);
        }

        executeTest(cmdGen, true);
    }


    /**
     * Test concurrent "explain plan" for a simple select statement.
     */
    public void testConcurrentExplainNoLockstep()
        throws Exception
    {
        FarragoTestTimedCommandGenerator cmdGen = 
            new FarragoTestTimedCommandGenerator(30);

        String sql = "explain plan for select * from sales.depts";

        cmdGen.addExplainCommand(1, 1, sql);
        cmdGen.addExplainCommand(2, 1, sql);

        executeTest(cmdGen, false);
    }


    /**
     * Test concurrent <code>select * from sales.depts</code> statements.
     */
    public void testConcurrentSelect()
        throws Exception
    {
        FarragoTestCommandGenerator cmdGen = newCommandGenerator();

        String sql = "select * from sales.depts order by deptno";
        String expected = 
            "{ 10, 'Sales' }, { 20, 'Marketing' }, { 30, 'Accounts' }";

        // Repeat a few times to improve odds of getting concurrent
        // execution.
        for(int i = 0; i < 10; i++) {
            int tick = (i * 3) + 2;

            cmdGen.addPrepareCommand(1, tick, sql);
            cmdGen.addFetchAndCompareCommand(1, tick + 1, 5, expected);
            cmdGen.addCloseCommand(1, tick + 2);

            cmdGen.addPrepareCommand(2, tick, sql);
            cmdGen.addFetchAndCompareCommand(2, tick + 1, 5, expected);
            cmdGen.addCloseCommand(2, tick + 2);

            cmdGen.addPrepareCommand(3, tick, sql);
            cmdGen.addFetchAndCompareCommand(3, tick + 1, 5, expected);
            cmdGen.addCloseCommand(3, tick + 2);
        }

        executeTest(cmdGen, true);        
    }


    /**
     * Test concurrent <code>select * from sales.depts</code> statements.
     * Known to fail as of 6/17/2004.
     */
    public void testConcurrentSelectNoLockStep()
        throws Exception
    {
        FarragoTestTimedCommandGenerator cmdGen = 
            new FarragoTestTimedCommandGenerator(30);

        String sql = "select * from sales.depts order by deptno";
        String expected = 
            "{ 10, 'Sales' }, { 20, 'Marketing' }, { 30, 'Accounts' }";

        cmdGen.addPrepareCommand(1, 1, sql);
        cmdGen.addFetchAndCompareCommand(1, 2, 5, expected);
        cmdGen.addCloseCommand(1, 3);

        cmdGen.addPrepareCommand(2, 1, sql);
        cmdGen.addFetchAndCompareCommand(2, 2, 5, expected);
        cmdGen.addCloseCommand(2, 3);

        cmdGen.addPrepareCommand(3, 1, sql);
        cmdGen.addFetchAndCompareCommand(3, 2, 5, expected);
        cmdGen.addCloseCommand(3, 3);

        executeTest(cmdGen, false);
    }


    /**
     * Test concurrent select statements with a join.
     */
    public void testConcurrentJoin()
        throws Exception
    {
        FarragoTestCommandGenerator cmdGen = newCommandGenerator();

        String sql = "select emps.empno, emps.name, emps.gender, depts.* from sales.depts, sales.emps where emps.deptno = depts.deptno";

        String expected = 
            "{ 100, 'Fred',  null, 10, 'Sales' }, " +
            "{ 110, 'Eric',  'M',  20, 'Marketing' }, " +
            "{ 120, 'Wilma', 'F',  20, 'Marketing' }";

        // Repeat a few times to improve odds of getting concurrent
        // execution.
        for(int i = 0; i < 10; i++) {
            int tick = (i * 3) + 2;

            cmdGen.addPrepareCommand(1, tick, sql);
            cmdGen.addFetchAndCompareCommand(1, tick + 1, 5, expected);
            cmdGen.addCloseCommand(1, tick + 2);

            cmdGen.addPrepareCommand(2, tick, sql);
            cmdGen.addFetchAndCompareCommand(2, tick + 1, 5, expected);
            cmdGen.addCloseCommand(2, tick + 2);
        }

        executeTest(cmdGen, true);        
    }


    /**
     * Test concurrent select statements with a join.
     */
    public void testConcurrentJoinNoLockStep()
        throws Exception
    {
        FarragoTestTimedCommandGenerator cmdGen =
            new FarragoTestTimedCommandGenerator(30);

        String sql = "select emps.empno, emps.name, emps.gender, depts.* from sales.depts, sales.emps where emps.deptno = depts.deptno";

        String expected = 
            "{ 100, 'Fred',  null, 10, 'Sales' }, " +
            "{ 110, 'Eric',  'M',  20, 'Marketing' }, " +
            "{ 120, 'Wilma', 'F',  20, 'Marketing' }";

        cmdGen.addPrepareCommand(1, 1, sql);
        cmdGen.addFetchAndCompareCommand(1, 2, 5, expected);
        cmdGen.addCloseCommand(1, 3);
        
        cmdGen.addPrepareCommand(2, 1, sql);
        cmdGen.addFetchAndCompareCommand(2, 2, 5, expected);
        cmdGen.addCloseCommand(2, 3);

        executeTest(cmdGen, false);        
    }


    /**
     * Test conccurent insert statements.
     */
    public void _testConcurrentInsert()
        throws Exception
    {
        // REVIEW: SZ 6/18/2004: Fennel storage currently has no
        // table-level concurrency-control, so this test should fail.

        FarragoTestCommandGenerator cmdGen = newCommandGenerator();

        String createSchema = "create schema concurrency";

        String createTable = "create table concurrency.test (message_id integer not null primary key, message varchar(128) not null)";

        String baseSql = "insert into concurrency.test (message_id, message) values (@MESSAGE_ID@, @MESSAGE@)";

        cmdGen.addDdlCommand(1, 1, createSchema);
        cmdGen.addDdlCommand(1, 2, createTable);

        // Repeat a few times to improve odds of getting concurrent
        // execution.
        for(int i = 0; i < 10; i++) {
            int tick = (i * 2) + 3;

            String tickBaseSql = baseSql.replaceAll("@MESSAGE@", "'clock tick " + tick + "'");

            String sql1 = tickBaseSql.replaceAll("@MESSAGE_ID@",
                                                 String.valueOf(i * 2 + 1));
            String sql2 = tickBaseSql.replaceAll("@MESSAGE_ID@",
                                                 String.valueOf(i * 2 + 2));

            cmdGen.addInsertCommand(1, tick, 5, sql1);
            cmdGen.addCommitCommand(1, tick + 1);

            cmdGen.addInsertCommand(2, tick, 5, sql2);
            cmdGen.addCommitCommand(2, tick + 1);
        }

        executeTest(cmdGen, true);        
    }
}
