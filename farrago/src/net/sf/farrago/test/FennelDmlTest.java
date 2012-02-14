/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package net.sf.farrago.test;

import java.sql.*;

import java.util.*;

import junit.framework.*;


/**
 * FennelDmlTest tests execution of Farrago DML statements over data stored in
 * Fennel (including both temporary and permanent tables).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelDmlTest
    extends FarragoTestCase
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FennelDmlTest object.
     *
     * @param testName .
     *
     * @throws Exception .
     */
    public FennelDmlTest(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ----------------------------------------------------------------

    // implement TestCase
    public static Test suite()
    {
        return wrappedSuite(FennelDmlTest.class);
    }

    /**
     * Tests INSERT ... SELECT
     *
     * @throws Exception .
     */
    public void testInsert()
        throws Exception
    {
        String sql = "insert into temps select * from emps";
        try {
            int rowCount = stmt.executeUpdate(sql);
            assertEquals(4, rowCount);
            resultSet = stmt.executeQuery("select * from temps");
            assertEquals(
                4,
                getResultSetCount());
        } finally {
            connection.rollback();
        }
    }

    /**
     * Tests INSERT ... VALUES ()
     *
     * @throws Exception .
     */
    public void testInsertSingleRow()
        throws Exception
    {
        String sql = "insert into depts values(70,'Obfuscation')";
        try {
            int rowCount = stmt.executeUpdate(sql);
            assertEquals(1, rowCount);
            resultSet = stmt.executeQuery("select name from depts");
            Set<String> refSet = new HashSet<String>();
            refSet.add("Sales");
            refSet.add("Marketing");
            refSet.add("Accounts");
            refSet.add("Obfuscation");
            compareResultSet(refSet);
        } finally {
            connection.rollback();
        }
    }

    /**
     * Tests INSERT ... (column-list) VALUES ()
     *
     * @throws Exception .
     */
    public void testInsertColumnList()
        throws Exception
    {
        String sql =
            "insert into emps(gender,name,empno,empid,deptno,manager) "
            + "values('M','Flubber',130,5,40,false)";
        try {
            int rowCount = stmt.executeUpdate(sql);
            assertEquals(1, rowCount);
            resultSet = stmt.executeQuery("select name from emps");
            Set<String> refSet = new HashSet<String>();
            refSet.add("Fred");
            refSet.add("Eric");
            refSet.add("John");
            refSet.add("Wilma");
            refSet.add("Flubber");
            compareResultSet(refSet);
        } finally {
            connection.rollback();
        }
    }

    /**
     * Tests INSERT ... VALUES (), ()
     *
     * @throws Exception .
     */
    public void testInsertMultiRow()
        throws Exception
    {
        String sql =
            "insert into depts values(70,'Obfuscation'), (80,'Eradication')";
        try {
            int rowCount = stmt.executeUpdate(sql);
            assertEquals(2, rowCount);
            resultSet = stmt.executeQuery("select name from depts");
            Set<String> refSet = new HashSet<String>();
            refSet.add("Sales");
            refSet.add("Marketing");
            refSet.add("Accounts");
            refSet.add("Obfuscation");
            refSet.add("Eradication");
            compareResultSet(refSet);
        } finally {
            connection.rollback();
        }
    }

    /**
     * Tests INSERT with an implied DEFAULT value.
     *
     * @throws Exception .
     */
    public void testInsertImplicitDefault()
        throws Exception
    {
        String sql =
            "insert into temps(empno,name,deptno,gender,city,manager) "
            + "values(130,'Flubber',40,'M','Miami',false)";
        try {
            int rowCount = stmt.executeUpdate(sql);
            assertEquals(1, rowCount);
            resultSet = stmt.executeQuery("select empid from temps");
            Set<String> refSet = new HashSet<String>();
            refSet.add("999");
            compareResultSet(refSet);
        } finally {
            connection.rollback();
        }
    }

    /**
     * Tests INSERT ... SELECT ... WHERE ...
     *
     * @throws Exception .
     */
    public void testInsertWithFilter()
        throws Exception
    {
        String sql = "insert into temps select * from emps where empno = 120";
        try {
            int rowCount = stmt.executeUpdate(sql);
            assertEquals(1, rowCount);
            resultSet = stmt.executeQuery("select name from temps");
            Set<String> refSet = new HashSet<String>();
            refSet.add("Wilma");
            compareResultSet(refSet);
        } finally {
            connection.rollback();
        }
    }

    /**
     * Tests INSERT where source and target are the same table.
     *
     * @throws Exception .
     */
    public void testSelfInsert()
        throws Exception
    {
        String sql =
            "insert into depts select deptno+100,'Antisales' from depts "
            + "where deptno = 10";
        try {
            int rowCount = stmt.executeUpdate(sql);
            assertEquals(1, rowCount);
            resultSet = stmt.executeQuery("select name from depts");
            Set<String> refSet = new HashSet<String>();
            refSet.add("Sales");
            refSet.add("Marketing");
            refSet.add("Accounts");
            refSet.add("Antisales");
            compareResultSet(refSet);
        } finally {
            connection.rollback();
        }
    }

    /**
     * Tests INSERT ... VALUES (?,?,...)
     *
     * @throws Exception .
     */
    public void testInsertPrepared()
        throws Exception
    {
        String sql = "insert into depts values (?,?)";
        try {
            preparedStmt = connection.prepareStatement(sql);

            preparedStmt.setInt(1, 40);
            preparedStmt.setString(2, "Excoriation");
            int rowCount = preparedStmt.executeUpdate();

            preparedStmt.setInt(1, 50);
            preparedStmt.setString(2, "Defenestration");
            rowCount += preparedStmt.executeUpdate();

            assertEquals(2, rowCount);
            resultSet = stmt.executeQuery("select name from depts");
            Set<String> refSet = new HashSet<String>();
            refSet.add("Sales");
            refSet.add("Marketing");
            refSet.add("Accounts");
            refSet.add("Excoriation");
            refSet.add("Defenestration");
            compareResultSet(refSet);
        } finally {
            connection.rollback();
        }
    }

    /**
     * Tests DELETE.
     *
     * @throws Exception .
     */
    public void testDelete()
        throws Exception
    {
        String sql = "delete from depts";
        try {
            int rowCount = stmt.executeUpdate(sql);
            assertEquals(3, rowCount);
            resultSet = stmt.executeQuery("select * from depts");
            assertEquals(
                0,
                getResultSetCount());
        } finally {
            connection.rollback();
        }
    }

    public void testDeleteNothing()
        throws Exception
    {
        String sql = "delete from depts where deptno=40";
        try {
            int rowCount = stmt.executeUpdate(sql);
            assertEquals(0, rowCount);
        } finally {
            connection.rollback();
        }
    }

    /**
     * Tests DELETE ... WHERE ...
     *
     * @throws Exception .
     */
    public void testDeleteWithFilter()
        throws Exception
    {
        String sql = "delete from depts where deptno=20";
        try {
            int rowCount = stmt.executeUpdate(sql);
            assertEquals(1, rowCount);
            resultSet = stmt.executeQuery("select name from depts");
            Set<String> refSet = new HashSet<String>();
            refSet.add("Sales");
            refSet.add("Accounts");
            compareResultSet(refSet);
        } finally {
            connection.rollback();
        }
    }

    /**
     * Tests UPDATE which can be performed without updating any secondary
     * indexes.
     *
     * @throws Exception .
     */
    public void testPrimaryUpdate()
        throws Exception
    {
        String sql = "update emps set age=99 where empid=3";
        try {
            int rowCount = stmt.executeUpdate(sql);
            assertEquals(1, rowCount);
            resultSet = stmt.executeQuery("select age from emps");
            Set<String> refSet = new HashSet<String>();
            refSet.add(null);
            refSet.add("25");
            refSet.add("99");
            refSet.add("50");
            compareResultSet(refSet);
        } finally {
            connection.rollback();
        }
    }

    /**
     * Tests UPDATE to a column covered by a secondary index.
     *
     * @throws Exception .
     */
    public void testSecondaryUpdate()
        throws Exception
    {
        String sql = "update depts set name='Slacking' where deptno=30";
        try {
            int rowCount = stmt.executeUpdate(sql);
            assertEquals(1, rowCount);
            resultSet = stmt.executeQuery("select name from depts order by 1");
            List<String> refList = new ArrayList<String>();
            refList.add("Marketing");
            refList.add("Sales");
            refList.add("Slacking");
            compareResultList(refList);
        } finally {
            connection.rollback();
        }
    }

    /**
     * Tests explicit checkpoint (results are not verifiable so just test that
     * the statement is accepted).
     *
     * @throws Exception .
     */
    public void testCheckpoint()
        throws Exception
    {
        try {
            stmt.execute("checkpoint");
        } finally {
            connection.rollback();
        }
    }

    /**
     * Tests savepoint API.
     *
     * @throws Exception .
     */
    public void testSavepoints()
        throws Exception
    {
        String insertA = "insert into depts values (40,'A')";
        String insertB = "insert into depts values (50,'B')";
        String insertC = "insert into depts values (60,'C')";
        String sql = "select name from depts";

        int rowCount;
        Set<String> refSet = new HashSet<String>();
        refSet.add("Sales");
        refSet.add("Marketing");
        refSet.add("Accounts");

        try {
            rowCount = stmt.executeUpdate(insertA);
            refSet.add("A");
            assertEquals(1, rowCount);
            resultSet = stmt.executeQuery("select name from depts");
            compareResultSet(refSet);
            Savepoint savepointX = connection.setSavepoint("X");

            rowCount = stmt.executeUpdate(insertB);
            refSet.add("B");
            assertEquals(1, rowCount);
            resultSet = stmt.executeQuery("select name from depts");
            compareResultSet(refSet);
            Savepoint savepointY = connection.setSavepoint("Y");

            rowCount = stmt.executeUpdate(insertC);
            refSet.add("C");
            assertEquals(1, rowCount);
            resultSet = stmt.executeQuery("select name from depts");
            compareResultSet(refSet);

            connection.rollback(savepointY);
            refSet.remove("C");
            resultSet = stmt.executeQuery("select name from depts");
            compareResultSet(refSet);

            connection.rollback(savepointX);
            refSet.remove("B");
            resultSet = stmt.executeQuery("select name from depts");
            compareResultSet(refSet);

            rowCount = stmt.executeUpdate(insertB);
            refSet.add("B");
            assertEquals(1, rowCount);
            resultSet = stmt.executeQuery("select name from depts");
            compareResultSet(refSet);
            Savepoint savepointZ = connection.setSavepoint("Z");

            rowCount = stmt.executeUpdate(insertC);
            refSet.add("C");
            assertEquals(1, rowCount);
            resultSet = stmt.executeQuery("select name from depts");
            compareResultSet(refSet);

            connection.rollback(savepointZ);
            refSet.remove("C");
            resultSet = stmt.executeQuery("select name from depts");
            compareResultSet(refSet);

            rowCount = stmt.executeUpdate(insertC);
            refSet.add("C");
            assertEquals(1, rowCount);
            resultSet = stmt.executeQuery("select name from depts");
            compareResultSet(refSet);

            connection.rollback(savepointX);
            refSet.remove("B");
            refSet.remove("C");
            resultSet = stmt.executeQuery("select name from depts");
            compareResultSet(refSet);

            connection.rollback();
            refSet.remove("A");
            resultSet = stmt.executeQuery("select name from depts");
            compareResultSet(refSet);
        } finally {
            connection.rollback();
        }
    }

    // implement TestCase
    protected void setUp()
        throws Exception
    {
        super.setUp();
        stmt.execute("set schema 'sales'");
    }
}

// End FennelDmlTest.java
