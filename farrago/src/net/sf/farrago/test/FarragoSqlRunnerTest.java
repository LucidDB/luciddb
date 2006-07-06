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
package net.sf.farrago.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.SQLException;

import org.eigenbase.util.Util;

import net.sf.farrago.util.SqlRunner;


/**
 * Rudimentary JUnit tests for the SqlRunner class.
 * @author chard
 * @version $Id$
 */
public class FarragoSqlRunnerTest extends FarragoTestCase
{
    private String serverUrl = "jdbc:farrago:";
    private String testScript = "unitsql/runner/easy.sql";
    private static final String userName = "sa";
    private static final String password = null;

    public FarragoSqlRunnerTest(String testName)
    throws Exception
    {
        super(testName);
        serverUrl = newJdbcEngineDriver().getBaseUrl();
    }

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
            SqlRunner.instance().runScript(testScript, null, userName, password);
        } catch (Throwable e) {
            assertNotNull(e.getMessage());
        }
    }
    
    public void testNullUser()
    {
        try {
            SqlRunner.instance().runScript(testScript, serverUrl, null, password);
        } catch (Throwable e) {
            assertNotNull(e.getMessage());
        }
    }
    
    public void testInvalidPath()
    {
        try {
            SqlRunner.instance().runScript("foo.sql", serverUrl, userName, password);
        } catch (SQLException e) {
            assertNotNull(e.getMessage());
        }
    }
    
    public void testInvalidUrl()
    {
        try {
            SqlRunner.instance().runScript(testScript, "jdbc:notfarrago",
                userName, password);
        } catch (SQLException e) {
            assertNotNull(e.getMessage());
        }
    }
    
    public void testInvalidUser()
    {
        try {
            SqlRunner.instance().runScript(testScript, serverUrl, "bozo", "clown");
        } catch (SQLException e) {
            assertNotNull(e.getMessage());
        }
    }
    
    // Positive tests
    
    public void testScript()
    throws SQLException
    {
        SqlRunner.instance().runScript(testScript, serverUrl,
            userName, password);
    }
    
    public void testScriptWithRedirect() throws IOException, SQLException
    {
        String scriptBase = testScript.substring(0, testScript.lastIndexOf('.'));
        File f = new File(scriptBase);
        PrintStream stream = new PrintStream(openTestLogOutputStream(f));
        SqlRunner.instance().runScript(testScript, serverUrl, userName,
            password, stream, null);
        assertTrue(!Util.isNullOrEmpty(stream.toString()));
        diffTestLog();
    }
}
