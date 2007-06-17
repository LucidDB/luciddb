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
package com.lucidera.luciddb.test.bh;

import com.lucidera.jdbc.*;
import java.io.*;
import java.util.regex.*;
import net.sf.farrago.catalog.*;
import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.util.*;
import org.eigenbase.blackhawk.compose.AutoTest;
import org.eigenbase.blackhawk.compose.Parameters;
import org.eigenbase.blackhawk.compose.TestContext;
import sqlline.SqlLine;
/**
 * Simple sql test for bh-base test in the luciddb area
 *
 * @author boris
 * @version $Id$
 */

public class SqlTest extends AutoTest {

    String[] args;
    String sqlFile;
    private OutputStream logOutputStream;    
    File sqlFileSansExt;
    File logFile;    
    File refFile;
    /** Diff masks defined so far */
    // private List diffMasks;
    private String diffMasks;
    Matcher compiledDiffMatcher;
    private String ignorePatterns;
    Matcher compiledIgnoreMatcher;

    public SqlTest(TestContext tc) throws Exception
    {
        super(tc);

        // set luciddb-specific properties
        System.setProperty("net.sf.farrago.defaultSessionFactoryLibraryName",
            "class:com.lucidera.farrago.LucidDbSessionFactory");
        System.setProperty("net.sf.farrago.test.jdbcDriverClass",
            "com.lucidera.jdbc.LucidDbLocalDriver");

        Parameters param = tc.getParameters();
        // obtain sql-file parameter - required
        sqlFile = param.getString("sql-file");

        // following parameters are not required
        String driverName = param.getString("jdbc-driver", "");
        String username = param.getString("username", "");
        if (username.equals("")) {
            username = FarragoCatalogInit.SA_USER_NAME;
        }
        String passwd = param.getString("passwd", "");
        String urlPrefix = param.getString("url", "");
        if (urlPrefix.equals("")) {
            if (driverName.equals("")) {
                driverName = FarragoProperties.instance().
                    testJdbcDriverClass.get();
            }
            Class clazz = Class.forName(driverName);
            LucidDbLocalDriver driver =
                (LucidDbLocalDriver) clazz.newInstance();
            urlPrefix = driver.getUrlPrefix();
        }
        assert (sqlFile.endsWith(".sql"));
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

    public boolean testSql() 
    {
        diffMasks = "";
        addDiffMask("\\$Id.*\\$");
        
        PrintStream savedOut = System.out;
        PrintStream savedErr = System.err;
        FileInputStream inputStream = null;
        try {
            // read from the specified file
            inputStream = new FileInputStream(sqlFile.toString());
            // to make sure the connection is closed properly, append the
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
            SqlLine.mainWithInputRedirection(args, sequenceStream);
            printStream.flush();
            diffTestLog();
        } catch (Exception e) {
            return failure("Failed with Unexpected Exception: " + 
                e.toString(),e);
        } finally {
            System.setOut(savedOut);
            System.setErr(savedErr);
            try {
                inputStream.close();
            } catch (Throwable t) {
                tc.inform("ERROR INPUT STREAM CLOSE THREW EXCEPTION!" + t.toString());
            }
        }

        return success("No problem");
    }

    /**
     * NOTE: cut and pasted from DiffTestCase.java
     * Initializes a diff-based test, overriding the default
     * log file naming scheme altogether.
     *
     * @param testFileSansExt full path to log filename, without .log/.ref
     * extension
     */
    protected OutputStream openTestLogOutputStream(File testFileSansExt)
        throws IOException
    {
        assert (logOutputStream == null);

        logFile = new File(testFileSansExt.toString() + ".log");
        logFile.delete();

        refFile = new File(testFileSansExt.toString() + ".ref");

        logOutputStream = new FileOutputStream(logFile);
        return logOutputStream;
    }
    /**
     * Finishes a diff-based test.  Output that was written to the Writer
     * returned by openTestLog is diffed against a .ref file, and if any
     * differences are detected, the test case fails.  Note that the diff
     * used is just a boolean test, and does not create any .dif ouput.
     *
     * <p>
     * NOTE: if you wrap the Writer returned by openTestLog() (e.g. with a
     * PrintWriter), be sure to flush the wrapping Writer before calling this
     * method.
     * </p>
     */


    protected void diffTestLog()
        throws IOException, Exception
    {
        int n = 0;
        assert (logOutputStream != null);
        logOutputStream.close();
        logOutputStream = null;

        if (!refFile.exists()) {
            throw new Exception("Reference file " + refFile + " does not exist");
        }

        // TODO:  separate utility method somewhere
        FileReader logReader = null;
        FileReader refReader = null;
        try {
/** don't do the gc trick for now
            if (compiledIgnoreMatcher != null) {
                if (gcInterval != 0) {
                    n++;
                    if ( n == gcInterval) {
                        n = 0;
                        System.gc();
                    }
                }
            }
**/
            logReader = new FileReader(logFile);
            refReader = new FileReader(refFile);
            LineNumberReader logLineReader = new LineNumberReader(logReader);
            LineNumberReader refLineReader = new LineNumberReader(refReader);
            for (;;) {
                String logLine = logLineReader.readLine();
                String refLine = refLineReader.readLine();
                while (logLine != null && matchIgnorePatterns(logLine)) {
                    // System.out.println("logMatch Line:" + logLine);
                    logLine = logLineReader.readLine();
                }
                while (refLine != null && matchIgnorePatterns(refLine)) {
                    // System.out.println("refMatch Line:" + logLine);
                    refLine = refLineReader.readLine();
                }
                if ((logLine == null) || (refLine == null)) {
                    if (logLine != null) {
                        diffFail(logFile, logLineReader.getLineNumber());
                    }
                    if (refLine != null) {
                        diffFail(logFile, refLineReader.getLineNumber());
                    }
                    break;
                }
                logLine = applyDiffMask(logLine);
                refLine = applyDiffMask(refLine);
                if (!logLine.equals(refLine)) {
                    diffFail(logFile, logLineReader.getLineNumber());
                }
            }
        } finally {
            if (logReader != null) {
                logReader.close();
            }
            if (refReader != null) {
                refReader.close();
            }
        }

        // no diffs detected, so delete redundant .log file
        logFile.delete();
    }

    /**
     * Adds a diff mask.  Strings matching the given regular expression
     * will be masked before diffing.  This can be used to suppress
     * spurious diffs on a case-by-case basis.
     *
     * @param mask a regular expression, as per String.replaceAll
     */
    protected void addDiffMask(String mask)
    {
        // diffMasks.add(mask);
        if (diffMasks.length() == 0) {
            diffMasks = mask;
        } else {
            diffMasks = diffMasks + "|" + mask;
        }
        Pattern compiledDiffPattern = Pattern.compile(diffMasks);
        compiledDiffMatcher = compiledDiffPattern.matcher("");
    }

    protected void addIgnorePattern(String javaPattern)
    {
        if (ignorePatterns.length() == 0) {
            ignorePatterns = javaPattern;
        } else {
            ignorePatterns = ignorePatterns + "|" + javaPattern;
        }
        Pattern compiledIgnorePattern = Pattern.compile(ignorePatterns);
        compiledIgnoreMatcher = compiledIgnorePattern.matcher("");
    }

    private String applyDiffMask(String s)
    {
        if (compiledDiffMatcher != null) {
            compiledDiffMatcher.reset(s);
            // we assume most of lines do not match
            // so compiled matches will be faster than replaceAll.
            if (compiledDiffMatcher.find()) {
                return s.replaceAll(diffMasks, "XYZZY");
            }
        }
        return s;
    }
    private boolean matchIgnorePatterns(String s)
    {
        if (compiledIgnoreMatcher != null) {
            compiledIgnoreMatcher.reset(s);
            return compiledIgnoreMatcher.matches();
        }
        return false;
    }

    private void diffFail (
        File logFile,
        int lineNumber) throws Exception
    {
        tc.inform("DIFF FAILED!");
        final String message =
            "diff detected at line " + lineNumber + " in " + logFile;
        throw new Exception(message + 
            fileContents(refFile) + 
            fileContents(logFile));
    }

    /**
     * Returns the contents of a file as a string.
     */ 
    private static String fileContents(File file)
    {
        try {
            char[] buf = new char[2048];
            final FileReader reader = new FileReader(file);
            int readCount;
            final StringWriter writer = new StringWriter();
            while ((readCount = reader.read(buf)) >= 0) {
                writer.write(buf, 0, readCount);
            }
            return writer.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}



// End SqlTest.java
