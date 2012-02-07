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
import java.util.*;

import net.sf.farrago.test.*;
import net.sf.farrago.catalog.FarragoCatalogInit;
import org.eigenbase.test.concurrent.*;

/**
 * FarragoTestConcurrentTestCase provides a basic harness for executing
 * multi-threaded test cases. It extends {@link FarragoTestCase} to exploit
 * the multi-thread test utility from the package {@link org.eigenbase.test.concurrent}.
 *
 * To build a test case programmatically,
 * obtain a {@link ConcurrentTestCommandGenerator} via the method
 * {@link #newCommandGenerator()} and use it to configure one or more threads to
 * execute one or more commands. Once configured, run the test by calling
 *{@link #executeTest(ConcurrentTestCommandGenerator, boolean, String)}.
 * An alternative is to write a test script in the mtsql format;
 * see {@link FarragoTestConcurrentScriptedTestCase}.
 *
 * <p><b>Note:</b> To properly configure a test based on this class, you must
 * provide a <code>suite()</code> method that wraps the tests in your class with
v * the necessary initialization from {@link FarragoTestCase}. The normal
 * implementation is:
 *
 * <pre>
 *   ...
 *   import junit.framework.Test;
 *   ...
 *
 *   public class MyTest
 *       extends FarragoTestConcurrentTestCase
 *   {
 *       ...
 *       public static Test suite()
 *       {
 *           return wrappedSuite(MyTest.class);
 *       }
 *       ...
 *   }
 * </pre>
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public abstract class FarragoTestConcurrentTestCase
    extends FarragoTestCase
{
    //~ Constructors -----------------------------------------------------------

    protected FarragoTestConcurrentTestCase(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Instantiates a new ConcurrentTestCommandGenerator.
     */
    protected ConcurrentTestCommandGenerator newCommandGenerator()
    {
        return new ConcurrentTestCommandGenerator();
    }

    /**
     * Instantiates a new ConcurrentTestCommandScript with the
     * given file.
     */
    protected ConcurrentTestCommandScript
    newScriptedCommandGenerator(
        String filename)
        throws IOException
    {
        return new ConcurrentTestCommandScript(filename);
    }

    /**
     * Executes a test case, possibly after interpolating synchronization.
     *
     * <b>Note: If <code>synchronizeClockTicks</code> is <code>
     * true</code>, the command lists configured for each thread are normalized
     * so that each thread has the same set of execution orders (or clock
     * ticks). When a thread is missing a clock tick that other threads contain,
     * a no-op command is added for the clock tick. Prior to execution, a
     * synchronization object is created. All
     * ConcurrentTestCommandExecutors for the test wait at a
     * synchronization point until all threads have reached the point before
     * continuing on to execute the clock tick's command. If <code>
     * synchronizeClockTicks</code> is <code>false</code>, no synchronization
     * occurs and all threads execute their commands, in order, as fast as they
     * can.
     *
     * <p>Each ConcurrentTestCommandExecutor's first error is reported on
     * System.err. If any ConcurrentTestCommandExecutor has an error, the
     * test fails.
     *
     * @param synchronizeClockTicks flag for thread synchronization (see above)
     * @param cmdGen embodies the test case
     * @param jdbcURL identifies server
     * @throws Exception if no connection found or if a thread operation is
     * interrupted
     */
    protected void executeTest(
        ConcurrentTestCommandGenerator cmdGen,
        boolean synchronizeClockTicks,
        String jdbcURL)
        throws Exception
    {
        setDataSource(cmdGen, jdbcURL);
        innerExecuteTest(cmdGen, synchronizeClockTicks);
        epilog(cmdGen);
    }

    protected void setDataSource(
        ConcurrentTestCommandGenerator cmdGen,
        String jdbcURL)
    {
        Properties jdbcProps = new Properties();
        jdbcProps.setProperty("user", FarragoCatalogInit.SA_USER_NAME);
        cmdGen.setDataSource(jdbcURL, jdbcProps);
    }

    protected void innerExecuteTest(
        ConcurrentTestCommandGenerator cmdGen,
        boolean synchronizeClockTicks)
        throws Exception
    {
        if (synchronizeClockTicks) {
            cmdGen.synchronizeCommandSets();
        }
        assertTrue(
            "test case has invalid synchronization",
            cmdGen.hasValidSynchronization());
        cmdGen.execute();
    }

    private void epilog(ConcurrentTestCommandGenerator cmdGen)
    {
        if (cmdGen.failed()) {
            System.err.println("Testcase: " + getName());
            for (ConcurrentTestCommandGenerator.FailedThread f
                : cmdGen.getFailedThreads())
            {
                System.err.println(f.name + " failed " + f.location);
                f.failure.printStackTrace(System.err);
                System.err.println();
            }
            System.err.println("-----\n");
            fail();
        }
    }
}

// End FarragoTestConcurrentTestCase.java
