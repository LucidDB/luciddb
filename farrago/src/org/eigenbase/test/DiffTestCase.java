/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

package org.eigenbase.test;

import java.io.*;
import java.util.*;
import java.util.regex.*;

import junit.framework.*;

import org.eigenbase.util.*;


/**
 * DiffTestCase is an abstract base for JUnit tests which produce multi-line
 * output to be verified by diffing against a pre-existing reference file.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class DiffTestCase extends TestCase
{
    //~ Instance fields -------------------------------------------------------

    /** Name of current .log file. */
    private File logFile;

    /** Name of current .ref file. */
    private File refFile;

    /** OutputStream for current test log. */
    private OutputStream logOutputStream;

    /** Diff masks defined so far */
    // private List diffMasks;
    private String diffMasks;
    Pattern compiledDiffPattern;
    private String ignorePatterns;
    Pattern compiledIgnorePattern;

    int gcInterval;

    //~ Constructors ----------------------------------------------------------

    /**
     * Initializes a new DiffTestCase.
     *
     * @param testCaseName JUnit test case name
     */
    protected DiffTestCase(String testCaseName)
        throws Exception
    {
        super(testCaseName);

        // diffMasks = new ArrayList();
        diffMasks = "";
        ignorePatterns = "";
        compiledIgnorePattern = null;
        compiledDiffPattern = null;
        gcInterval = 0;
    }

    //~ Methods ---------------------------------------------------------------

    // implement TestCase
    protected void setUp()
        throws Exception
    {
        super.setUp();
        // diffMasks.clear();
        diffMasks = "";
        ignorePatterns = "";
        compiledIgnorePattern = null;
        compiledDiffPattern = null;
        gcInterval = 0;
    }

    // implement TestCase
    protected void tearDown()
        throws Exception
    {
        try {
            if (logOutputStream != null) {
                logOutputStream.close();
                logOutputStream = null;
            }
        } finally {
            super.tearDown();
        }
    }

    /**
     * Initializes a diff-based test.  Any existing .log and .dif files
     * corresponding to this test case are deleted, and a new, empty .log
     * file is created.  The default log file location is a subdirectory
     * under the result getTestlogRoot(), where the subdirectory
     * name is based on the unqualified name of the test class.
     * The generated log file name will be testMethodName.log,
     * and the expected reference file will be testMethodName.ref.
     *
     * @return Writer for log file, which caller should use as a destination
     *         for test output to be diffed
     */
    protected Writer openTestLog()
        throws Exception
    {
        File testClassDir =
            new File(
                getTestlogRoot(),
                ReflectUtil.getUnqualifiedClassName(getClass()));
        testClassDir.mkdirs();
        File testLogFile = new File(testClassDir,
                getName());
        return new OutputStreamWriter(openTestLogOutputStream(testLogFile));
    }

    /**
     * @return the root under which testlogs should be written
     */
    protected abstract File getTestlogRoot()
        throws Exception;

    /**
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
        throws IOException
    {
        int n = 0;
        assert (logOutputStream != null);
        logOutputStream.close();
        logOutputStream = null;

        if (!refFile.exists()) {
            Assert.fail("Reference file " + refFile + " does not exist");
        }

        // TODO:  separate utility method somewhere
        FileReader logReader = null;
        FileReader refReader = null;
        try {
            if (compiledIgnorePattern != null) {
                if (gcInterval != 0) {
                    n++;
                    if ( n == gcInterval) {
                        n = 0;
                        System.gc();
                    }
                }
            }
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
                        diffFail(logFile, logLineReader);
                    }
                    if (refLine != null) {
                        diffFail(logFile, refLineReader);
                    }
                    break;
                }
                logLine = applyDiffMask(logLine);
                refLine = applyDiffMask(refLine);
                if (!logLine.equals(refLine)) {
                    diffFail(logFile, logLineReader);
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
     * set the number of lines for garbage collection.
     *
     * @param n an integer, the number of line for garbage collection, 0 means 
     * no garbage collection.
     */
    protected void setGC(int n)
    {
        gcInterval = n;
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
        compiledDiffPattern = Pattern.compile(diffMasks);
    }

    protected void addIgnorePattern(String javaPattern)
    {
        if (ignorePatterns.length() == 0) {
            ignorePatterns = javaPattern;
        } else {
            ignorePatterns = ignorePatterns + "|" + javaPattern;
        }
        compiledIgnorePattern = Pattern.compile(ignorePatterns);
    }

    private String applyDiffMask(String s)
    {
        // TODO:  reuse a single java.util.regex.Matcher
        /*
        for (int i = 0; i < diffMasks.size(); ++i) {
            String mask = (String) diffMasks.get(i);
            s = s.replaceAll(mask, "XYZZY");
        }
        */
        if (compiledDiffPattern != null) {
            // we assume most of lines do not match
            // so compiled matches will be faster than replaceAll.
            if (compiledDiffPattern.matcher(s).find()) {
                return s.replaceAll(diffMasks, "XYZZY");
            }
        }

        return s;
    }
    private boolean matchIgnorePatterns(String s)
    {
        if (compiledIgnorePattern != null) {
            return compiledIgnorePattern.matcher(s).matches();
        }
        return false;
    }

    private void diffFail(
        File logFile,
        LineNumberReader lineReader)
    {
        Assert.fail("diff detected at line " + lineReader.getLineNumber()
            + " in " + logFile);
    }
}


// End DiffTestCase.java
