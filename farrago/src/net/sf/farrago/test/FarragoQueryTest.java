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

import java.sql.*;
import java.util.*;

import junit.framework.*;


/**
 * FarragoQueryTest tests miscellaneous aspects of Farrago SQL query
 * processing.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoQueryTest extends FarragoTestCase
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoQueryTest object.
     *
     * @param testName JUnit test name
     *
     * @throws Exception .
     */
    public FarragoQueryTest(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ---------------------------------------------------------------

    // implement TestCase
    public static Test suite()
    {
        return wrappedSuite(FarragoQueryTest.class);
    }

    /**
     * Test a query which involves operation on columns
     */
    public void testPrimitiveColumnOperation()
        throws Exception
    {
        String sql =
            "select deptno*1, deptno/1, deptno+0,deptno-0,deptno*deptno,deptno/deptno,deptno"
            + " from sales.emps order by deptno";
        preparedStmt = connection.prepareStatement(sql);
        resultSet = preparedStmt.executeQuery();
        List refList = new ArrayList();
        refList.add("10");
        refList.add("20");
        refList.add("20");
        refList.add("40");
        compareResultList(refList);
    }

    /**
     * Test a query which involves comparison with VARBINARY values.
     */
    public void testVarbinaryComparison()
        throws Exception
    {
        String sql = "select name from sales.emps where public_key=?";
        preparedStmt = connection.prepareStatement(sql);
        final byte [] bytes = { 0x41, 0x62, 0x63 };
        preparedStmt.setBytes(1, bytes);
        resultSet = preparedStmt.executeQuery();
        Set refSet = new HashSet();
        refSet.add("Eric");
        compareResultSet(refSet);
    }

    /**
     * Test a query which involves sorting VARBINARY values.
     */
    public void testOrderByVarbinary()
        throws Exception
    {
        String sql =
            "select name,public_key from sales.emps" + " order by public_key";
        resultSet = stmt.executeQuery(sql);
        List refList = new ArrayList();
        refList.add("Wilma");
        refList.add("Eric");
        refList.add("Fred");
        refList.add("John");
        compareResultList(refList);
    }

    /**
     * Test a query using a different catalog.
     */
    public void testSetCatalog()
        throws Exception
    {
        String sql = "set catalog 'sys_cwm'";
        stmt.execute(sql);
        sql = "select \"name\" from \"Relational\".\"Schema\"";
        resultSet = stmt.executeQuery(sql);
        Set refSet = new HashSet();
        refSet.add("SALES");
        refSet.add("JDBC_METADATA");
        compareResultSet(refSet);

        // restore default catalog
        sql = "set catalog 'localdb'";
        stmt.execute(sql);
    }
}


// End FarragoQueryTest.java
