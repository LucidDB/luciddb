/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
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

package net.sf.saffron.test;

import junit.framework.TestCase;

import net.sf.saffron.util.Util;

import java.io.PrintWriter;
import java.io.StringWriter;

import java.sql.*;


/**
 * <code>JdbcTest</code> is a set of unit tests
 *
 * @author jhyde
 * @version $Id$
 *
 * @since Mar 21, 2003
 */
public class JdbcTest extends TestCase
{
    //~ Static fields/initializers --------------------------------------------

    private static final String driverClassName =
        "net.sf.saffron.jdbc.SaffronJdbcDriver";
    private static final String NL = System.getProperty("line.separator");

    //~ Constructors ----------------------------------------------------------

    public JdbcTest(String name)
    {
        super(name);
    }

    //~ Methods ---------------------------------------------------------------

    public void foo()
    {
    }

    public void testConnect() throws SQLException
    {
        Util.discard(getConnection());
    }

    // tests follow
    public void testImplicitRegistration()
        throws ClassNotFoundException,
            InstantiationException,
            IllegalAccessException,
            SQLException
    {
        Util.discard(Class.forName(driverClassName));
    }

    public void _testJoin() throws SQLException
    {
        testQuery(
            "select * from \"emps\" as emp join \"depts\" as dept on emp.\"deptno\" = dept.\"deptno\"",
            "empno='100' name='Fred' deptno='10' gender='M' city='San Francisco' deptno0='10' name0='Sales'" + NL +
            "empno='110' name='Eric' deptno='20' gender='M' city='San Francisco' deptno0='20' name0='Marketing'" + NL +
            "empno='120' name='Wilma' deptno='20' gender='F' city='Los Angeles' deptno0='20' name0='Marketing'");
    }

    public void _testOuterJoinToQuery() throws SQLException
    {
        testQuery(
            "select * from \"depts\" as dept" + NL
            + "left join (select * from \"emps\" where \"gender\" = 'F') as femaleEmp"
            + NL + "on femaleEmp.\"deptno\" = dept.\"deptno\"",
            "deptno='20' name='Marketing' empno='120' name0='Wilma' deptno0='20' gender='F' city='Los Angeles'");
    }

    public void testRegisterDriver()
        throws ClassNotFoundException,
            InstantiationException,
            IllegalAccessException,
            SQLException
    {
        final Class clazz = Class.forName(driverClassName);
        Driver driver = (Driver) clazz.newInstance();
        DriverManager.registerDriver(driver);
    }

    public void _testSimpleQuery() throws SQLException
    {
        testQuery(
                "select * from \"emps\"",
                "empno='100' name='Fred' deptno='10' gender='M' city='San Francisco'" + NL +
                "empno='110' name='Eric' deptno='20' gender='M' city='San Francisco'" + NL +
                "empno='110' name='John' deptno='40' gender='M' city='Vancouver'" + NL +
                "empno='120' name='Wilma' deptno='20' gender='F' city='Los Angeles'");
    }

    private Connection getConnection() throws SQLException
    {
        try {
            Util.discard(Class.forName(driverClassName));
        } catch (ClassNotFoundException e) {
        }
        return DriverManager.getConnection(
            "jdbc:saffron:schema=sales.SalesInMemory");
    }

    private void testQuery(String query,String expected)
        throws SQLException
    {
        final ResultSet resultSet =
            getConnection().createStatement().executeQuery(query);
        String actual = toString(resultSet);
        Util.assertEqualsVerbose(expected,actual);
    }

    private String toString(final ResultSet resultSet)
        throws SQLException
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        int i = 0;
        final ResultSetMetaData metaData = resultSet.getMetaData();
        while (resultSet.next()) {
            if (i++ > 0) {
                pw.println();
            }
            for (int j = 0; j < metaData.getColumnCount(); j++) {
                if (j > 0) {
                    pw.print(' ');
                }
                pw.print(
                    metaData.getColumnName(j + 1) + "='"
                    + resultSet.getObject(j + 1) + "'");
            }
        }
        pw.flush();
        String actual = sw.toString();
        return actual;
    }
}


// End JdbcTest.java
