/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

import junit.framework.*;

import net.sf.farrago.util.FarragoProperties;

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
    private List diffMasks;

    //~ Constructors ----------------------------------------------------------

    /**
     * Initialize a new DiffTestCase.
     *
     * @param testCaseName JUnit test case name
     */
    protected DiffTestCase(String testCaseName)
        throws Exception
    {
        super(testCaseName);

        diffMasks = new ArrayList();
    }

    //~ Methods ---------------------------------------------------------------

    // implement TestCase
    protected void setUp()
        throws Exception
    {
        super.setUp();
        diffMasks.clear();
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
     * Initialize a diff-based test.  Any existing .log and .dif files
     * corresponding to this test case are deleted, and a new, empty .log
     * file is created.  The log file location will be in the same
     * directory as the .java file defining the test case, where the
     * path prefix is based on the result of method getSourceRoot().
     * See method shouldIncludeClassInLogFileName() for the naming
     * convention of the log file within its directory.
     *
     * @return Writer for log file, which caller should use as a destination
     *         for test output to be diffed
     */
    protected Writer openTestLog()
        throws Exception
    {
        File testLogDir = getTestlogRoot();

        // walk down the package tree
        String className = getClass().getName();
        for (;;) {
            int iDot = className.indexOf('.');
            if (iDot == -1) {
                break;
            }
            testLogDir = new File(
                    testLogDir,
                    className.substring(0, iDot));
            assert (testLogDir.exists());
            className = className.substring(iDot + 1);
        }

        File testLogFile;
        if (shouldIncludeClassInLogFileName()) {
            testLogFile = new File(testLogDir, className + "." + getName());
        } else {
            testLogFile = new File(
                    testLogDir,
                    getName());
        }

        return new OutputStreamWriter(openTestLogOutputStream(testLogFile));
    }

    /**
     * Get the root of the Java source tree.  The default is to use
     * ${net.sf.farrago.home}/src, but subclasses may override.
     *
     * @return src root as File
     */
    protected File getSourceRoot()
        throws Exception
    {
        return new File(
            FarragoProperties.instance().homeDir.get(),
            "src");
    }

    /**
     * @return the root under which testlogs should be written
     */
    protected File getTestlogRoot()
        throws Exception
    {
        return getSourceRoot();
    }

    /**
     * Control log file base naming convention.  If this returns true
     * (the default), the base log filename will be
     * TestClassName.testCaseName.log.  This is overkill when an entire
     * subdirectory is devoted to a single test class; in that case,
     * the subclass should override this method to return false,
     * and the base log filename will be testCaseName.log.
     *
     * @return true if the class name should be included in the log filename
     */
    protected boolean shouldIncludeClassInLogFileName()
    {
        return true;
    }

    /**
     * Initialize a diff-based test, overriding the default
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
     * Finish a diff-based test.  Output that was written to the Writer
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
            logReader = new FileReader(logFile);
            refReader = new FileReader(refFile);
            LineNumberReader logLineReader = new LineNumberReader(logReader);
            LineNumberReader refLineReader = new LineNumberReader(refReader);
            for (;;) {
                String logLine = logLineReader.readLine();
                String refLine = refLineReader.readLine();
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
     * Add a diff mask.  Strings matching the given regular expression
     * will be masked before diffing.  This can be used to suppress
     * spurious diffs on a case-by-case basis.
     *
     * @param mask a regular expression, as per String.replaceAll
     */
    protected void addDiffMask(String mask)
    {
        diffMasks.add(mask);
    }

    private String applyDiffMask(String s)
    {
        // TODO:  reuse a single java.util.regex.Matcher
        for (int i = 0; i < diffMasks.size(); ++i) {
            String mask = (String) diffMasks.get(i);
            s = s.replaceAll(mask, "XYZZY");
        }
        return s;
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
