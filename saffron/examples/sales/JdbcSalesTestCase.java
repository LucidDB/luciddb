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

package sales;

import net.sf.saffron.oj.stmt.*;
import net.sf.saffron.runtime.Row;
import net.sf.saffron.util.DelegatingInvocationHandler;
import net.sf.saffron.util.Util;
import net.sf.saffron.core.SaffronConnection;

import java.io.PrintWriter;
import java.io.StringWriter;

import java.lang.reflect.Proxy;

import java.sql.*;


/**
 * <code>JdbcSalesTestCase</code> runs queries which refer to the 'sales'
 * schema using a {@link Sales} object (which talks to JDBC).
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 26 April, 2002
 */
public class JdbcSalesTestCase extends SalesTestCase
{
    //~ Instance fields -------------------------------------------------------

    java.sql.Connection sqlConnection;

    //~ Constructors ----------------------------------------------------------

    public JdbcSalesTestCase(String s) throws Exception
    {
        super(s);
        this.sqlConnection = getJdbcConnection();
        this.arguments =
            new OJStatement.Argument [] {
                new OJStatement.Argument("salesDb",new Sales(sqlConnection))
            };
    }

    //~ Methods ---------------------------------------------------------------
    public SaffronConnection getConnection() {
        return getSales();
    }

    // ------------------------------------------------------------------------
    // jdbc

    /**
     * Creates a JDBC connection. The caller must close it when finished.
     * Fails the test if cannot create the connection.
     */
    public static Connection getJdbcConnection()
    {
        String connectString = Util.getSalesConnectString();
        try {
            return DriverManager.getConnection(connectString);
        } catch (SQLException e) {
            fail(
                "received exception [" + e + "] while connecting to ["
                + connectString + "]");
            return null;
        }
    }

    public void testConvertResultSetToArray()
    {
        Connection connection = getJdbcConnection();
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet =
                statement.executeQuery("SELECT EMPNO, GENDER FROM EMP");
            Object o =
                runQuery(
                    "(select from resultSet as foo)",
                    new OJStatement.Argument [] {
                        new OJStatement.Argument("resultSet",resultSet)
                    });
            assertTrue(o instanceof Object []);
            Object [] a = (Object []) o;
            assertEquals(4,a.length);
            assertTrue(a[0] instanceof Row);
            Row row = (Row) a[0];
            Object value = row.getObject(1);
            assertTrue(value instanceof Integer);
            assertEquals(100,((Integer) value).intValue());
            value = row.getObject(2);
            assertTrue(value instanceof String);
            assertEquals("M",value);
        } catch (SQLException e) {
            fail("received " + e);
        }
    }

    public void testPushdownFilterToJdbc()
    {
        Object o =
            assertQueryGeneratesSql(
                "select from salesDb.emps as emp where emp.gender.equals(\"M\")",
                "SELECT *\r\nFROM `EMP`\r\nWHERE `EMP`.`gender` = 'M'");
        assertTrue(o instanceof Emp []);
        Emp [] emps = (Emp []) o;
        assertEquals(3,emps.length);
    }

    public void testPushdownProjectToJdbc()
    {
        Object o =
            assertQueryGeneratesSql(
                "select {emp.name.substring(2,3) as namePart, emp.empno} "
                + "from salesDb.emps as emp where emp.gender.equals(\"M\")",
                "SELECT *\r\nFROM `EMP`\r\nWHERE `EMP`.`gender` = 'M'");
        assertTrue(o instanceof Emp []);
        Emp [] emps = (Emp []) o;
        assertEquals(3,emps.length);
    }

    protected void tearDown() throws Exception
    {
        if (sqlConnection != null) {
            sqlConnection.close();
            sqlConnection = null;
        }
        super.tearDown();
    }

    Sales getSales()
    {
        return (Sales) arguments[0].getValue();
    }

    private Object assertQueryGeneratesSql(String query,String sql)
    {
        Sales sales = getSales();
        Connection oldConnection = sales.getConnection();
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        Connection newConnection = createTracingConnection(oldConnection,pw);
        sales.setConnection(newConnection);
        Object o;
        try {
            o = runQuery(query);
            pw.flush();
            String s = sw.toString();
            assertTrue(s,s.indexOf(sql) >= 0);
        } finally {
            sales.setConnection(oldConnection);
        }
        return o;
    }

    private static Connection createTracingConnection(
        Connection oldConnection,
        PrintWriter pw)
    {
        return (Connection) Proxy.newProxyInstance(
            null,
            new Class [] { Connection.class },
            new TracingConnectionHandler(oldConnection,pw));
    }

    private static Statement createTracingStatement(
        Statement statement,
        PrintWriter pw)
    {
        return (Statement) Proxy.newProxyInstance(
            null,
            new Class [] { Statement.class },
            new TracingStatementHandler(statement,pw));
    }

    //~ Inner Classes ---------------------------------------------------------

    private static class TracingConnectionHandler
        extends DelegatingInvocationHandler//implements Connection

    {
        final Connection connection;
        final PrintWriter pw;

        TracingConnectionHandler(Connection connection,PrintWriter pw)
        {
            this.connection = connection;
            this.pw = pw;
        }

        public Statement createStatement() throws SQLException
        {
            return createTracingStatement(connection.createStatement(),pw);
        }

        protected Object getTarget()
        {
            return connection;
        }
    }

    private static class TracingStatementHandler
        extends DelegatingInvocationHandler//implements Statement

    {
        private final PrintWriter pw;
        private final Statement statement;

        TracingStatementHandler(Statement statement,PrintWriter pw)
        {
            this.statement = statement;
            this.pw = pw;
        }

        public ResultSet executeQuery(String sql) throws SQLException
        {
            pw.println("executeQuery('" + sql + "')");
            return statement.executeQuery(sql);
        }

        protected Object getTarget()
        {
            return statement;
        }
    }
}


// End JdbcSalesTestCase.java
