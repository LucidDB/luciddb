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
        // Apply the diff masks for a .REF file compare
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
