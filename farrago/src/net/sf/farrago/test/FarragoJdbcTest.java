/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
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

import junit.framework.*;

import java.sql.*;

import java.util.*;

/**
 * FarragoJdbcTest tests specifics of the Farrago implementation of the JDBC
 * API.  See also unitsql/jdbc/*.sql.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJdbcTest extends FarragoTestCase
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoDJbcTest object.
     *
     * @param testName JUnit test name
     *
     * @throws Exception .
     */
    public FarragoJdbcTest(String testName) throws Exception
    {
        super(testName);
    }

    //~ Methods ---------------------------------------------------------------

    // implement TestCase
    public static Test suite()
    {
        return wrappedSuite(FarragoJdbcTest.class);
    }

    /**
     * Test re-execution of a prepared query.
     *
     * @throws Exception .
     */
    public void testPreparedQuery() throws Exception
    {
        String sql = "select * from sales.emps";
        preparedStmt = connection.prepareStatement(sql);
        for (int i = 0; i < 5; ++i) {
            resultSet = preparedStmt.executeQuery();
            if (catalog.isFennelEnabled()) {
                assertEquals(4,getResultSetCount());
            } else {
                assertEquals(0,getResultSetCount());
            }
            resultSet.close();
            resultSet = null;
        }
    }

    /**
     * Test re-execution of an unprepared query.  There's no black-box way to
     * verify that caching is working, but if it is, this will at least
     * exercise it.
     *
     * @throws Exception .
     */
    public void testCachedQuery() throws Exception
    {
        repeatQuery();
    }

    /**
     * Test re-execution of an unprepared query with statement caching
     * disabled.
     *
     * @throws Exception .
     */
    public void testUncachedQuery() throws Exception
    {
        // disable caching
        stmt.execute("alter system set \"codeCacheMaxBytes\" = 0");
        repeatQuery();

        // re-enable caching
        stmt.execute("alter system set \"codeCacheMaxBytes\" = 10000000");
    }

    private void repeatQuery() throws Exception
    {
        String sql = "select * from sales.emps";
        for (int i = 0; i < 3; ++i) {
            resultSet = stmt.executeQuery(sql);
            if (catalog.isFennelEnabled()) {
                assertEquals(4,getResultSetCount());
            } else {
                assertEquals(0,getResultSetCount());
            }
            resultSet.close();
            resultSet = null;
        }
    }

    /**
     * Test retrieval of ResultSetMetaData without actually executing query.
     */
    public void testPreparedMetaData() throws Exception
    {
        String sql = "select name from sales.emps";
        preparedStmt = connection.prepareStatement(sql);
        ResultSetMetaData metaData = preparedStmt.getMetaData();
        assertEquals(1,metaData.getColumnCount());
        assertTrue(metaData.isSearchable(1));
        assertEquals(
            ResultSetMetaData.columnNoNulls,metaData.isNullable(1));
        assertEquals("NAME",metaData.getColumnName(1));
        assertEquals(128,metaData.getPrecision(1));
        assertEquals(Types.VARCHAR,metaData.getColumnType(1));
        assertEquals("VARCHAR",metaData.getColumnTypeName(1));
    }

    // TODO:  re-execute DDL, DML

    /**
     * Test valid usage of a dynamic parameter and retrieval of associated
     * metadata.
     */
    public void testDynamicParameter() throws Exception
    {
        String sql = "select empid from sales.emps where name=?";
        preparedStmt = connection.prepareStatement(sql);
        ParameterMetaData pmd = preparedStmt.getParameterMetaData();
        assertEquals(
            1,pmd.getParameterCount());
        assertEquals(
            128,pmd.getPrecision(1));
        assertEquals(
            Types.VARCHAR,pmd.getParameterType(1));
        assertEquals(
            "VARCHAR",pmd.getParameterTypeName(1));
            
        preparedStmt.setString(1,"Wilma");
        resultSet = preparedStmt.executeQuery();
        if (catalog.isFennelEnabled()) {
            compareResultSet(Collections.singleton("1"));
        }
        preparedStmt.setString(1,"Eric");
        resultSet = preparedStmt.executeQuery();
        if (catalog.isFennelEnabled()) {
            compareResultSet(Collections.singleton("3"));
        }
        preparedStmt.setString(1,"George");
        resultSet = preparedStmt.executeQuery();
        assertEquals(0,getResultSetCount());
        preparedStmt.setString(1,null);
        resultSet = preparedStmt.executeQuery();
        assertEquals(0,getResultSetCount());
    }

    /**
     * Test metadata for dynamic parameter in an UPDATE statement.
     */
    public void testDynamicParameterInUpdate() throws Exception
    {
        String sql = "update sales.emps set age = ?";
        preparedStmt = connection.prepareStatement(sql);
        ParameterMetaData pmd = preparedStmt.getParameterMetaData();
        assertEquals(
            1,pmd.getParameterCount());
        assertEquals(
            Types.INTEGER,pmd.getParameterType(1));
        assertEquals(
            "INTEGER",pmd.getParameterTypeName(1));
    }

    /**
     * Test invalid usage of a dynamic parameter.
     */
    public void testInvalidDynamicParameter() throws Exception
    {
        String sql = "select ? from sales.emps";
        try {
            preparedStmt = connection.prepareStatement(sql);
        } catch (SQLException ex) {
            // expected
            return;
        }
        Assert.fail("Expected failure due to invalid dynamic param");
    }
    
    /**
     * Test invalid attempt to execute a statement with a dynamic parameter
     * without preparation.
     */
    public void testDynamicParameterExecuteImmediate() throws Exception
    {
        String sql = "select empid from sales.emps where name=?";
        try {
            resultSet = stmt.executeQuery(sql);
        } catch (SQLException ex) {
            // expected
            return;
        }
        Assert.fail(
            "Expected failure due to immediate execution with dynamic param");
    }
}

// End FarragoJdbcTest.java
