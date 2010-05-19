/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2006 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
package net.sf.farrago.test.jdbc;

import java.sql.*;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.jdbc.*;
import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.test.*;

import org.eigenbase.sql.parser.*;


/**
 * Farrago Engine Driver tests (refactored from FarragoJdbcTest)
 *
 * @author angel
 * @version $Id$
 * @since Jun 4, 2006
 */
public class FarragoEngineDriverTest
    extends FarragoTestCase
{
    //~ Constructors -----------------------------------------------------------

    public FarragoEngineDriverTest(String testname)
        throws Exception
    {
        super(testname);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Tests engine driver URIs.
     */
    public void testURIs()
        throws Exception
    {
        FarragoAbstractJdbcDriver driver =
            FarragoTestCase.newJdbcEngineDriver();

        assert (driver instanceof FarragoUnregisteredJdbcEngineDriver);
        String uri = null;
        assertFalse(
            "driver accepts " + uri,
            driver.acceptsURL(uri));

        uri = "";
        assertFalse(
            "driver accepts " + uri,
            driver.acceptsURL(uri));

        uri = "foo:";
        assertFalse(
            "driver accepts " + uri,
            driver.acceptsURL(uri));

        uri = "foo:bar:";
        assertFalse(
            "driver accepts " + uri,
            driver.acceptsURL(uri));

        uri = "jdbc:";
        assertFalse(
            "driver accepts " + uri,
            driver.acceptsURL(uri));

        uri = "jdbc:foobar:";
        assertFalse(
            "driver accepts " + uri,
            driver.acceptsURL(uri));

        uri = "jdbc:farrago:";
        assertTrue(
            "driver doesn't accept " + uri,
            driver.acceptsURL(uri));

        uri = "jdbc:farrago://localhost";
        assertTrue(
            "driver doesn't accept " + uri,
            driver.acceptsURL(uri));

        uri = "jdbc:farrago:rmi:"; // only client driver should accept RMI
        assertFalse(
            "driver accepts " + uri,
            driver.acceptsURL(uri));

        uri = "jdbc:farrago:rmi://";
        assertFalse(
            "driver accepts " + uri,
            driver.acceptsURL(uri));

        uri = "jdbc:farrago:rmi://localhost";
        assertFalse(
            "driver accepts " + uri,
            driver.acceptsURL(uri));

        uri = "jdbc:farrago:client_rmi"; // internal
        assertTrue(
            "driver doesn't accept " + uri,
            driver.acceptsURL(uri));
    }

    /**
     * Tests engine driver URIs with connection params.
     */
    public void testConnectStrings()
        throws Exception
    {
        final String driverURI = "jdbc:farrago:";

        // create a sample connect string with various complications.
        // note that the parser itself is tested in eigenbase.
        final int maxParams = 6;
        HashMap<String, String> ref = new HashMap<String, String>();
        StringBuilder params = new StringBuilder();
        for (int i = 0; i < maxParams; ++i) {
            String key = "name" + i;
            String val = "value" + i;
            params.append(";");
            if (i == 2) {
                key += "=";
                val += "=False"; // name2==value2=False
            }
            if (i == 3) {
                val += "==True"; // name3=value3==True
            }
            if (i == 4) {
                // abandon without value
                val = ""; // name4=
            }
            params.append(key);
            params.append("=");
            params.append(val);
            ref.put(key, val);
        }

        String uri = driverURI + params.toString();
        tracer.info("loaded: " + uri);

        // test the driver's use of the connect string parser
        FarragoAbstractJdbcDriver driver =
            FarragoTestCase.newJdbcEngineDriver();

        assert (driver instanceof FarragoUnregisteredJdbcEngineDriver);
        Properties parsedProps = new Properties();
        String strippedUri = driver.parseConnectionParams(uri, parsedProps);

        //        tracer.info("stripped: " +strippedUri);
        //        tracer.info("parsed: " +toStringProperties(parsedProps));
        for (int i = 0; i < maxParams; ++i) {
            String key = "name" + i;
            String val = (String) parsedProps.get(key);
            String expval = ref.get(key);
            assertEquals("param " + key + ", ", expval, val);
        }

        // since driver's implementing method is public, be sure it is safe
        String cleanUri = driver.parseConnectionParams(uri, null);
        assertEquals("stripped URIs differ,", strippedUri, cleanUri);
        cleanUri = driver.parseConnectionParams(null, null);
        assertNull("cleanUri not null: " + cleanUri, cleanUri);

        // test an actual connection
        Properties props = newProperties();
        Connection conn = driver.connect(uri, props);
        assertNotNull("null connection", conn);
        assertTrue(
            "FarragoJdbcEngineConnection",
            conn instanceof FarragoJdbcEngineConnection);
        assertEquals(
            "user's props changed,",
            newProperties(),
            props);
        conn.close();

        // test a connection that fails without the params
        Properties empty = new Properties();
        try {
            conn = driver.connect(uri, empty);
            fail("Farrago connect without user credentials");
        } catch (SQLException e) {
            FarragoJdbcTest.assertExceptionMatches(e, ".*Login failed.*");
        }
        String loginUri = uri + ";user=" + FarragoCatalogInit.SA_USER_NAME;
        conn = driver.connect(loginUri, empty);
        assertNotNull("null connection", conn);
        assertTrue(
            "FarragoJdbcEngineConnection",
            conn instanceof FarragoJdbcEngineConnection);
        assertEquals(
            "empty props changed",
            0,
            empty.size());
        conn.close();

        // test that parameter precedence works
        Properties unauth = new Properties();
        unauth.setProperty("user", "unauthorized user");
        unauth.setProperty("password", "invalid password");

        // connect will fail unless loginUri attributes take precedence
        try {
            conn = driver.connect(uri, unauth);
            fail("Farrago connect with bad user credentials");
        } catch (SQLException e) {
            FarragoJdbcTest.assertExceptionMatches(e, ".*Login failed.*");
        }
        conn = driver.connect(loginUri, unauth);
        assertNotNull("null connection", conn);
        conn.close();
    }

    /**
     * Tests that session parameter values make it to sessions_view.
     */
    public void testSessionParams()
        throws Exception
    {
        final String driverURI = "jdbc:farrago:";
        final String sessionName = "FarragoJdbcTest.testSessionParams session";
        final String sessQuery =
            "SELECT * FROM sys_boot.mgmt.sessions_view "
            + " WHERE session_name = '" + sessionName + "'";
        FarragoAbstractJdbcDriver driver =
            FarragoTestCase.newJdbcEngineDriver();

        assert (driver instanceof FarragoUnregisteredJdbcEngineDriver);

        Properties sessionProps = new Properties(newProperties());
        sessionProps.setProperty("sessionName", sessionName);
        sessionProps.setProperty("clientUserName", "fjfarrago");
        sessionProps.setProperty("clientUserFullName", "Franklin J. Farrago");
        sessionProps.setProperty("clientProgramName", "Sample Client App");
        sessionProps.setProperty("clientProcessId", "12345");

        Connection conn = driver.connect(driverURI, sessionProps);
        assertNotNull("null connection", conn);
        ResultSet rset = conn.createStatement().executeQuery(sessQuery);
        boolean bool = rset.next();
        assertTrue("no matching session for \"" + sessionName + "\"", bool);

        String exp = sessionProps.getProperty("clientUserName");
        String str = rset.getString("SYSTEM_USER_NAME");
        assertEquals("client user name", exp, str);

        exp = sessionProps.getProperty("clientUserFullName");
        str = rset.getString("SYSTEM_USER_FULLNAME");
        assertEquals("client user fullname", exp, str);

        exp = sessionProps.getProperty("clientProgramName");
        str = rset.getString("PROGRAM_NAME");
        assertEquals("program name", exp, str);

        exp = sessionProps.getProperty("clientProcessId");
        str = rset.getString("PROCESS_ID");
        assertEquals("process id", exp, str);

        conn.close();
    }

    /**
     * Tests special connection initialization based on connection properties.
     *
     * @throws Exception
     *
     * @see FarragoJdbcEngineConnection#initConnection(Properties)
     */
    public void testConnectionInit()
        throws Exception
    {
        final String driverURI = "jdbc:farrago:";
        final String initialSchema = "sales";
        final String query = "SELECT * FROM emps";
        FarragoAbstractJdbcDriver driver =
            FarragoTestCase.newJdbcEngineDriver();

        Properties props = newProperties();
        Connection conn = driver.connect(driverURI, props);

        // test query that assumes a schema
        Statement stmt = conn.createStatement();
        try {
            stmt.executeQuery(query);
            fail("query should have required a default schema");
        } catch (SQLException e) {
            assertExceptionMatches(e, "No default schema specified.*");
        }
        stmt.close();
        conn.close();

        // test good initial schema; could also be param on URI string
        props.setProperty("schema", initialSchema);
        conn = driver.connect(driverURI, props);
        stmt = conn.createStatement();
        try {
            ResultSet rset = stmt.executeQuery(query);
            assertTrue("expected at last one row", rset.next());
        } catch (SQLException e) {
            assertExceptionMatches(e, "No default schema specified.*");
        }
        stmt.close();
        conn.close();

        // test wrong initial schema; could also be param on URI string
        props.setProperty("schema", "SAILS");
        conn = driver.connect(driverURI, props);
        stmt = conn.createStatement();
        try {
            stmt.executeQuery(query);
            fail("query should have required the SALES schema");
        } catch (SQLException e) {
            assertExceptionMatches(e, ".*Table 'EMPS' not found");
        }
        stmt.close();
        conn.close();

        // test invalid initial schema; could also be param on URI string
        props.setProperty("schema", "unquoted phrase");
        try {
            conn = driver.connect(driverURI, props);
            fail("connection should fail with syntax error");
        } catch (SQLException e) {
            assertTrue(
                "got " + e.getClass().getName()
                + " but expected FarragoSqlException,",
                e instanceof FarragoJdbcUtil.FarragoSqlException);
            FarragoJdbcUtil.FarragoSqlException fse =
                (FarragoJdbcUtil.FarragoSqlException) e;
            Throwable orig = fse.getOriginalThrowable();
            assertNotNull("null original throwable", orig);
            assertTrue(
                "got " + orig.getClass().getName()
                + " but expected SqlParseException,",
                orig instanceof SqlParseException);
        }
    }

    public void testLabelInFarragoConnection()
        throws Exception
    {
        final String driverURI = "jdbc:farrago:";
        FarragoAbstractJdbcDriver driver =
            FarragoTestCase.newJdbcEngineDriver();
        Properties props = newProperties();
        props.setProperty("label", "foo");
        try {
            driver.connect(driverURI, props);
            fail(
                "connection should fail because snapshots aren't supported "
                + "in Farrago");
        } catch (Exception ex) {
            FarragoJdbcTest.assertExceptionMatches(
                ex,
                ".*Personality does not support snapshot reads");
        }
    }

    /**
     * For background on this test, please see
     * http://n2.nabble.com/SqlParseException.getCause%28%29-td1616492.html
     */
    public void testExcnStack()
        throws Exception
    {
        // Do something we know will cause Util.needToImplement
        // to be invoked, since that produces a generic RuntimeException
        // rather than a Farrago-specific excn.  If you are seeing
        // this test fail because you are implementing
        // ALTER TABLE ADD c INT UNIQUE, please find another excn cause
        // to keep this test coverage.
        String sql = "alter table sales.depts add dcode int unique";

        // For the engine driver, we should get full exception stacks,
        // because they don't have to go over the wire.
        try {
            stmt.execute(sql);
        } catch (FarragoJdbcUtil.FarragoSqlException ex) {
            assertNotNull(ex.getOriginalThrowable().getCause());
        }
    }

    /**
     * creates test connection properties.
     */
    private static Properties newProperties()
    {
        Properties props = new Properties();
        props.put("user", FarragoCatalogInit.SA_USER_NAME);
        props.put("password", "");
        return props;
    }

    /**
     * renders Properties values with quotes for easier reading.
     */
    private static String toStringProperties(Properties props)
    {
        StringBuilder buf = new StringBuilder();
        buf.append("{");
        Enumeration<?> enumer = props.propertyNames();
        int cnt = 0;
        while (enumer.hasMoreElements()) {
            if (cnt++ > 0) {
                buf.append(", ");
            }
            Object key = enumer.nextElement();
            String val = (String) props.get(key);
            buf.append(key).append(" => ");
            buf.append("\"").append(val).append("\"");
        }
        buf.append("}");
        return buf.toString();
    }

    /**
     * asserts that exception message matches the specified pattern.
     */
    private static void assertExceptionMatches(Throwable e, String match)
        throws Exception
    {
        String msg = e.getMessage();
        assertTrue(
            "Got exception \"" + msg + "\" but expected \"" + match + "\"",
            msg.matches(match));
    }
}

// End FarragoEngineDriverTest.java
