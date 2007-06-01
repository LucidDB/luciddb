/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
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
package net.sf.farrago.test;

import java.io.*;

import junit.framework.*;

import net.sf.farrago.util.*;


/**
 * FarragoSqlTest is a JUnit harness for executing tests which are implemented
 * by running an SQL script and diffing the output against a reference file
 * containing the expected results.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoSqlTest
    extends FarragoTestCase
{
    //~ Constructors -----------------------------------------------------------

    public FarragoSqlTest(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ----------------------------------------------------------------

    public static Test suite()
        throws Exception
    {
        return gatherSuite(
            FarragoProperties.instance().testFilesetUnitsql.get(true),
            new FarragoSqlTestFactory() {
                public FarragoTestCase createSqlTest(String testName)
                    throws Exception
                {
                    return new FarragoSqlTest(testName);
                }
            });
    }

    protected static Test gatherSuite(
        String fileSet,
        FarragoSqlTestFactory fac)
        throws Exception
    {
        StringReader stringReader = new StringReader(fileSet);
        LineNumberReader lineReader = new LineNumberReader(stringReader);
        TestSuite suite = new TestSuite();
        for (;;) {
            String file = lineReader.readLine();
            if (file == null) {
                break;
            }
            suite.addTest(fac.createSqlTest(file));
        }
        return wrappedSuite(suite);
    }

    protected void setUp()
        throws Exception
    {
        // run cleanup before each test case
        runCleanup();
        super.setUp();

        // NOTE jvs 5-Nov-2005:  SQL tests tend to be memory-intensive,
        // and the JVM doesn't always gc often enough, so help it out here
        // to avoid spurious OutOfMemory errors and generally speed things up.
        // However, putting this in FarragoTestCase slows down non-SQL
        // tests by gc'ing too frequently, so don't do that.
        Runtime rt = Runtime.getRuntime();
        rt.gc();
        rt.gc();
        tracer.info(
            "Java heap in use after gc = "
            + (rt.totalMemory() - rt.freeMemory()));
    }

    protected void runTest()
        throws Exception
    {
        // Applu the diff masks for a .REF file compare
        setRefFileDiffMasks();

        runSqlLineTest(getName());
    }

    //~ Inner Interfaces -------------------------------------------------------

    public interface FarragoSqlTestFactory
    {
        public FarragoTestCase createSqlTest(String testName)
            throws Exception;
    }
}

// End FarragoSqlTest.java
