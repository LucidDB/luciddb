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

import java.sql.SQLException;

import junit.framework.Assert;
import junit.framework.Test;


/**
 * <code>FarragoTypeTest</code> tests type conversion
 *
 * @author kinkoi
 * @since Dec 29, 2003
 * @version $Id$
 **/
public class FarragoTypeTest extends FarragoTestCase
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new <code>FarragoTypeTest</code> object.
     *
     * @param testName JUnit test name
     *
     * @throws Exception .
     */
    public FarragoTypeTest(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ---------------------------------------------------------------

    // implement TestCase
    public static Test suite()
    {
        return wrappedSuite(FarragoTypeTest.class);
    }

    /**
     * Test implicit type converstion
     *
     * @throws Exception .
     */
    public void testIntPlusChar()
        throws Exception
    {
        String sql = "select 1+'hello' from values(1)";
        try {
            stmt = connection.prepareStatement(sql);
        } catch (SQLException e) {
            // Expected
            return;
        }
        Assert.fail("Expected failure due to 1+'hello'");
    }

    public void _testDefaultValue()
        throws Exception
    {
        String sql = "SELECT 999 AS DEFAULT_VALUE FROM VALUES(0)";
        try {
            stmt = connection.prepareStatement(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void _testIntPlusInt()
        throws Exception
    {
        String sql = "select 1+1.0 from values(1)";
        resultSet = stmt.executeQuery(sql);
        if (repos.isFennelEnabled()) {
            assertEquals(
                4,
                getResultSetCount());
        } else {
            assertEquals(
                1,
                getResultSetCount());
        }
    }

    public void _testSubstringFunc()
        throws Exception
    {
        String sql = "select SUBSTR('aa',1,2) from values('aaa')";
        resultSet = stmt.executeQuery(sql);
    }

    // todo: test that substr(varchar(10),?,5) returns varchar(5)
    // because 5 < 10
    // todo: test that substr(varchar(10),?,15) returns varchar(10)
    // because 10<15
    // todo: test that substr(varchar(10),?,variable) returns varchar(10)
    // todo: test taht substr(?,?,date) fails because there's not a conversion
    // to integer (I don't think there is)
    // todo: test that substr(char(10),int,int) returns varchar(10), because
    // there's  a conversion from char to varchar
    // todo: test "select * from values 1, 2 + 3" (should be in parser test)
    public void __testWrongParamSubstrFunc()
        throws Exception
    {
        String sql = "select SUBSTR('fff',1,1) from values('aaa')";
        resultSet = stmt.executeQuery(sql);
    }

    public void __testUpperfunc()
        throws Exception
    {
        String sql = "select UPPER(12312) from values('aaa')";
        resultSet = stmt.executeQuery(sql);
    }

    public void __testCombinedFunc()
        throws Exception
    {
        String sql = "select UPPER(SUBSTR('aa',1,'bbcc')) from values('aaaa')";
        try {
            resultSet = stmt.executeQuery(sql);
        } catch (SQLException e) {
            e.printStackTrace(); //To change body of catch statement use Options | File Templates.
        }
    }

    public void testDummy()
    {
        // Do nothing.
        // Add this test to avoid no test found exception after rename all the unit tests
    }
}


// End FarragoTypeTest.java
