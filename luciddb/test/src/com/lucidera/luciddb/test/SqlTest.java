/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Copyright (C) 2005-2007 The Eigenbase Project
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
package com.lucidera.luciddb.test;

import org.luciddb.jdbc.*;
import java.io.*;
import java.sql.*;
import java.util.regex.*;
import java.util.logging.*;
import junit.framework.*;
import net.sf.farrago.catalog.*;
import net.sf.farrago.test.concurrent.*;
import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.util.*;
import sqlline.SqlLine;

import org.eigenbase.test.*;

/**
 * Simple sql test for bh-base test in the luciddb area
 *
 * @author boris
 * @version $Id$
 */

public class SqlTest extends DiffTestCase {

    String[] args;
    String sqlFile;
    File sqlFileSansExt;
    String urlPrefix;
    String username;
    String passwd;

    public SqlTest(String testName) throws Exception
    {
        super(testName);

        // set luciddb-specific properties
        System.setProperty("net.sf.farrago.defaultSessionFactoryLibraryName",
            "class:org.luciddb.session.LucidDbSessionFactory");
        System.setProperty("net.sf.farrago.test.jdbcDriverClass",
            "org.luciddb.jdbc.LucidDbLocalDriver");

        String eigenhome = System.getenv("EIGEN_HOME");
        System.setProperty("java.util.logging.config.file",
            eigenhome + File.separator +
            "luciddb" + File.separator +
            "trace" + File.separator +
            "LucidDbTrace.properties");
        System.setProperty("net.sf.farrago.home",
            eigenhome + File.separator +
            "luciddb");
        System.setProperty("net.sf.farrago.catalog",
            eigenhome + File.separator +
            "luciddb" + File.separator +
            "catalog");
        
        // obtain sql-file parameter - required
        sqlFile = System.getProperty("sql-file");
        // following parameters are not required
        String driverName = System.getProperty("jdbc-driver", "");
        username = System.getProperty("username", "");
        if (username.equals("")) {
            username = FarragoCatalogInit.SA_USER_NAME;
        }
        passwd = System.getProperty("passwd", "");
        urlPrefix = System.getProperty("url", "");
        if (urlPrefix.equals("")) {
            if (driverName.equals("")) {
                driverName = FarragoProperties.instance().
                    testJdbcDriverClass.get();
            }
            Class clazz = Class.forName(driverName);
            LucidDbLocalDriver driver =
                (LucidDbLocalDriver) clazz.newInstance();
            urlPrefix = driver.getUrlPrefix();
        } else {
            Class clazz = Class.forName(driverName);
        }
        System.out.println("sql-file: " + sqlFile);
        System.out.println("jdbc-driver: " + driverName);
        System.out.println("username: " + username);
        assert (sqlFile.endsWith(".sql") || sqlFile.endsWith(".mtsql"));
        sqlFileSansExt =
            new File(sqlFile.substring(0, sqlFile.length() - 4));
        args =
            new String [] {
                "-u", urlPrefix, "-d",
                driverName, "-n",
                username, "-p",
                passwd,
                "--force=true", "--silent=true",
                "--showWarnings=false", "--maxWidth=1024"
            };
    }

    public void testSql()
    {
        addDiffMask("\\$Id.*\\$");

        Logger tracer = LucidDbTestHarness.tracer;
        tracer.info("Entering test case " + sqlFile);
        Runtime rt = Runtime.getRuntime();
        rt.gc();
        rt.gc();
        long heapSize = rt.totalMemory() - rt.freeMemory();
        tracer.info("JVM heap size = " + heapSize);
        
        PrintStream savedOut = System.out;
        PrintStream savedErr = System.err;
        FileInputStream inputStream = null;
        // NOTE jvs 26-Apr-2006:  We no longer close connection;
        // that's what keeps the engine running for the duration
        // of a suite.
        try {
            // read from the specified file
            if (sqlFile.endsWith(".sql")) {
                inputStream = new FileInputStream(sqlFile.toString());
                runSqllineTest(inputStream);
            } else {
                assert(sqlFile.endsWith(".mtsql"));
                runMtsqlTest();
            }
        } catch (Exception e) {
            fail("Failed with Unexpected Exception: " +
                e.toString());
        } finally {
            System.setOut(savedOut);
            System.setErr(savedErr);
            try {
                if (inputStream != null) {
                    inputStream.close();
                }
            } catch (Throwable t) {
                System.out.println("ERROR INPUT STREAM CLOSE THREW EXCEPTION!"
                    + t.toString());
            }
            tracer.info("Leaving test case " + sqlFile);
        }
    }

    private void runSqllineTest(InputStream inputStream)
        throws Exception
    {
        // to make sure the session is closed properly, append the
        // !quit command
        String quitCommand = "\n!quit\n";
        ByteArrayInputStream quitStream =
            new ByteArrayInputStream(quitCommand.getBytes());

        SequenceInputStream sequenceStream =
            new SequenceInputStream(inputStream, quitStream);
        OutputStream outputStream =
            openTestLogOutputStream(sqlFileSansExt);
        PrintStream printStream = new PrintStream(outputStream);
        System.setOut(printStream);
        System.setErr(printStream);

        // tell SqlLine not to exit (this boolean is active-low)
        System.setProperty("sqlline.system.exit", "true");
        // set limit to min on plan cache
        if (urlPrefix.contains("luciddb")) {
            Connection conn = LucidDbTestHarness.startupEngine(
                urlPrefix, username, passwd);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(
                "call sys_boot.mgmt.flush_code_cache()");
        }
        SqlLine.mainWithInputRedirection(args, sequenceStream);
        printStream.flush();
        diffTestLog();
    }

    private void runMtsqlTest()
        throws Exception
    {
        LucidDbTestHarness.startupEngine(
            urlPrefix, username, passwd);
        MtsqlTestCase testCase = new MtsqlTestCase(sqlFile);
        testCase.go(urlPrefix);
    }

    /**
     * Dummy method not used
     */
    protected File getTestlogRoot()
    {
        return null;
    }

    private void diffFail (
        File logFile,
        int lineNumber) throws Exception
    {
        System.out.println("DIFF FAILED!");
        final String message =
            "diff detected at line " + lineNumber + " in " + logFile;
        throw new Exception(message +
            fileContents(refFile) +
            fileContents(logFile));
    }

    /**
     * Helper clsas for invoking a multi-threaded SQL script (testname.mtsql).
     * Inherits FarragoTestConcurrentTest, but bypasses some of the usual
     * malarkey that happens in setUp and tearDown.
     */
    private static class MtsqlTestCase extends FarragoTestConcurrentTest
    {
        MtsqlTestCase(String name)
            throws Exception
        {
            super(name);
        }
        
        public void go(String jdbcUrl)
            throws Exception
        {
            runTest(jdbcUrl);
        }
    }
}
// End SqlTest.java
