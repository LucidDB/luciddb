/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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
import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.test.*;


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
        FarragoUnregisteredJdbcEngineDriver driver =
            FarragoTestCase.newJdbcEngineDriver();

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
        HashMap<String,String> ref = new HashMap<String, String>();
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
        FarragoUnregisteredJdbcEngineDriver driver =
            FarragoTestCase.newJdbcEngineDriver();
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
        assertTrue("FarragoJdbcEngineConnection",
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
        assertTrue("FarragoJdbcEngineConnection",
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
     * tests that session parameter values make it to sessions_view.
     */
    public void testSessionParams()
        throws Exception
    {
        final String driverURI = "jdbc:farrago:";
        final String sessionName = "FarragoJdbcTest.testSessionParams session";
        final String sessQuery =
            "SELECT * FROM sys_boot.mgmt.sessions_view "
            + " WHERE session_name = '" + sessionName + "'";
        FarragoUnregisteredJdbcEngineDriver driver =
            FarragoTestCase.newJdbcEngineDriver();

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
            String key = (String) enumer.nextElement();
            String val = (String) props.get(key);
            buf.append(key).append(" => ");
            buf.append("\"").append(val).append("\"");
        }
        buf.append("}");
        return buf.toString();
    }
}

// End FarragoEngineDriverTest.java
