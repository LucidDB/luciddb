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
        addDiffMask("jdbc:.*:>"); // allows testing with other drivers/URLs
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
