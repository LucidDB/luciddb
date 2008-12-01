/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 Disruptive Tech
// Copyright (C) 2006-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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

import java.io.*;

import java.sql.*;

import net.sf.farrago.util.*;

import org.eigenbase.util.*;


/**
 * Rudimentary JUnit tests for the SqlRunner class.
 *
 * @author chard
 * @version $Id$
 */
public class FarragoSqlRunnerTest
    extends FarragoTestCase
{
    //~ Static fields/initializers ---------------------------------------------

    private static final String userName = "sa";
    private static final String password = null;

    //~ Instance fields --------------------------------------------------------

    private String serverUrl = "jdbc:farrago:";
    private String testScript =
        FarragoProperties.instance().expandProperties(
            "${FARRAGO_HOME}/unitsql/runner/easy.sql");

    //~ Constructors -----------------------------------------------------------

    public FarragoSqlRunnerTest(String testName)
        throws Exception
    {
        super(testName);
        serverUrl = newJdbcEngineDriver().getBaseUrl();
    }

    //~ Methods ----------------------------------------------------------------

    // Negative tests

    public void testNullPath()
    {
        try {
            SqlRunner.instance().runScript(null, serverUrl, userName, password);
        } catch (Throwable e) {
            assertNotNull(e.getMessage());
        }
    }

    public void testNullUrl()
    {
        try {
            SqlRunner.instance().runScript(
                testScript,
                null,
                userName,
                password);
        } catch (Throwable e) {
            assertNotNull(e.getMessage());
        }
    }

    public void testNullUser()
    {
        try {
            SqlRunner.instance().runScript(
                testScript,
                serverUrl,
                null,
                password);
        } catch (Throwable e) {
            assertNotNull(e.getMessage());
        }
    }

    public void testInvalidPath()
    {
        try {
            SqlRunner.instance().runScript(
                "foo.sql",
                serverUrl,
                userName,
                password);
        } catch (SQLException e) {
            assertNotNull(e.getMessage());
        }
    }

    public void testInvalidUrl()
    {
        try {
            SqlRunner.instance().runScript(
                testScript,
                "jdbc:notfarrago",
                userName,
                password);
        } catch (SQLException e) {
            assertNotNull(e.getMessage());
        }
    }

    public void testInvalidUser()
    {
        try {
            SqlRunner.instance().runScript(
                testScript,
                serverUrl,
                "bozo",
                "clown");
        } catch (SQLException e) {
            assertNotNull(e.getMessage());
        }
    }

    // Positive tests

    public void testScript()
        throws SQLException
    {
        SqlRunner.instance().runScript(
            testScript,
            serverUrl,
            userName,
            password);
    }

    public void testScriptWithRedirect()
        throws IOException, SQLException
    {
        addDiffMask("\\$Id.*\\$");
        addDiffMask("jdbc:.*:>");   // allows testing with other drivers/URLs
        String scriptBase =
            testScript.substring(
                0,
                testScript.lastIndexOf('.'));
        File f = new File(scriptBase);
        PrintStream stream = new PrintStream(openTestLogOutputStream(f));
        SqlRunner.instance().runScript(
            testScript,
            serverUrl,
            userName,
            password,
            stream,
            null);
        assertTrue(!Util.isNullOrEmpty(stream.toString()));
        diffTestLog();
    }
}

// End FarragoSqlRunnerTest.java
