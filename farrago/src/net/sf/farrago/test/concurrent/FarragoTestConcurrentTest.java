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
package net.sf.farrago.test.concurrent;

import java.io.*;

import junit.framework.*;

import net.sf.farrago.util.*;


/**
 * FarragoTestConcurrentTest executes a variety of SQL DML and DDL commands via
 * a multi-threaded test harness in an effort to detect errors in concurrent
 * execution.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public class FarragoTestConcurrentTest
    extends FarragoTestConcurrentScriptedTestCase
{
    //~ Constructors -----------------------------------------------------------

    public FarragoTestConcurrentTest(String name)
        throws Exception
    {
        super(name);
    }

    //~ Methods ----------------------------------------------------------------

    public static Test suite()
        throws Exception
    {
        return gatherSuite(
            FarragoProperties.instance().testFilesetConcurrent.get(true),
            new FarragoConcurrentSqlTestFactory() {
                public FarragoTestConcurrentTestCase createSqlTest(
                    String testName)
                    throws Exception
                {
                    return new FarragoTestConcurrentTest(testName);
                }
            });
    }

    // REVIEW: SZ: 10/21/2004: Copied this from FarragoSqlTest.  If
    // that method moved into FarragoTestCase, we could reuse it.
    protected static Test gatherSuite(
        String fileSet,
        FarragoConcurrentSqlTestFactory fac)
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

    // Copied this from FarragoSqlTest.
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

    protected void runTest(String jdbcUrl)
        throws Exception
    {
        // mask out source control Id
        addDiffMask("\\$Id.*\\$");

        runScript(getName(), jdbcUrl);
    }

    protected void runTest()
        throws Exception
    {
        // mask out source control Id
        addDiffMask("\\$Id.*\\$");

        runScript(getName(), getJdbcUri(newJdbcEngineDriver()));
    }

    //~ Inner Interfaces -------------------------------------------------------

    public interface FarragoConcurrentSqlTestFactory
    {
        public FarragoTestConcurrentTestCase createSqlTest(String testName)
            throws Exception;
    }
}

// End FarragoTestConcurrentTest.java
