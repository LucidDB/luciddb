/*
// Farrago is a relational database management system.
// (C) Copyright 2004-2004, Disruptive Tech
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/
package net.sf.farrago.test.concurrent;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import net.sf.farrago.test.FarragoTestCase;

/**
 * FarragoTestConcurrentTestCase provides a basic harness for executing
 * multi-threaded test cases.
 *
 * <p>Obtain a {@link FarragoTestCommandGenerator} via the method
 * {@link #newCommandGenerator()} and use it to configure one or more
 * threads to execute one or more commands.  Once configured, pass the
 * FarragoTestCommandGenerator to the
 * {@link #executeTest(FarragoTestCommandGenerator, boolean)} method
 * to run the test.
 *
 * <b>Note:</b> To properly configure a test based on this class, you
 * must provide a <code>suite()</code> method that wraps the tests in
 * your class with the necessary initialization from {@link
 * FarragoTestCase}.  The normal implementation is:
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
public abstract class FarragoTestConcurrentTestCase extends FarragoTestCase
{
    //~ Instance fields -------------------------------------------------------

    private boolean debug = false;
    private PrintStream printStream = System.out;

    //~ Constructors ----------------------------------------------------------

    protected FarragoTestConcurrentTestCase(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Instantiates a new FarragoTestConcurrentCommandGenerator.
     */
    protected FarragoTestConcurrentCommandGenerator newCommandGenerator()
    {
        return new FarragoTestConcurrentCommandGenerator();
    }

    /**
     * Instantiates a new FarragoTestConcurrentScriptedCommandGenerator with the
     * given file.
     */
    protected 
    FarragoTestConcurrentScriptedCommandGenerator newScriptedCommandGenerator(
        String filename) throws IOException
    {
        return new FarragoTestConcurrentScriptedCommandGenerator(filename);
    }

    /**
     * Creates {@link FarragoTestConcurrentCommandExecutor} objects for the
     * threads defined in the given
     * {@link FarragoTestConcurrentCommandGenerator} and then starts
     * the command executors.
     *
     * <b>Note: If <code>synchronizeClockTicks</code> is
     * <code>true</code>, the command lists configured for each thread
     * are normalized so that each thread has the same set of
     * execution orders (or clock ticks).  When a thread is missing a
     * clock tick that other threads contain, a no-op command is added
     * for the clock tick.  Prior to execution, a synchronization
     * object is created.  All FarragoTestConcurrentCommandExecutors
     * for the test wait at a synchronization point until all threads
     * have reached the point before continuing on to execute the
     * clock tick's command. If <code>synchronizeClockTicks</code> is
     * <code>false</code>, no synchronization occurs and all threads
     * execute their commands, in order, as fast as they can.
     *
     * <p>Each FarragoTestConcurrentCommandExecutor's first error is
     * reported on System.err.  If any
     * FarragoTestConcurrentCommandExecutor has an error, the test
     * fails.
     *
     * @param commandGenerator the configuration for this test
     * @param synchronizeClockTicks flag for thread synchronization (see above)
     * @throws Exception if {@link FarragoTestCase#newJdbcEngineDriver}
     *                   fails or if a thread operation is interrupted
     */
    protected void executeTest(
        FarragoTestConcurrentCommandGenerator commandGenerator,
        boolean synchronizeClockTicks)
        throws Exception
    {
        if (synchronizeClockTicks) {
            commandGenerator.synchronizeCommandSets();
        }

        commandGenerator.validateSynchronization(this);

        Set threadIds = commandGenerator.getThreadIds();

        FarragoTestConcurrentCommandExecutor.Sync sync =
            new FarragoTestConcurrentCommandExecutor.Sync(threadIds.size());

        String jdbcURL = newJdbcEngineDriver().getUrlPrefix();

        // initialize command executors
        FarragoTestConcurrentCommandExecutor [] threads =
            new FarragoTestConcurrentCommandExecutor[threadIds.size()];

        int threadIndex = 0;
        for (Iterator i = threadIds.iterator(); i.hasNext();) {
            Integer threadId = (Integer) i.next();
            Iterator commands = commandGenerator.getCommandIterator(threadId);

            if (debug) {
                printStream.println("Thread ID: " + threadId + " ("
                                    + commandGenerator.getThreadName(threadId)
                                    + ")");
                commandGenerator.printCommands(printStream, threadId);
            }

            threads[threadIndex++] =
                new FarragoTestConcurrentCommandExecutor(
                    threadId.intValue(),
                    commandGenerator.getThreadName(threadId),
                    jdbcURL,
                    commands,
                    sync,
                    debug ? printStream : null);
        }

        // start all the threads
        for (int i = 0, n = threads.length; i < n; i++) {
            threads[i].start();
        }

        // wait for all threads to finish
        for (int i = 0, n = threads.length; i < n; i++) {
            threads[i].join();
        }

        // check for failure(s)
        if (commandGenerator.requiresCustomErrorHandling()) {
            for (int i = 0, n = threads.length; i < n; i++) {
                FarragoTestConcurrentCommandExecutor executor = threads[i];
                
                if (executor.getFailureCause() != null) {
                    commandGenerator.customErrorHandler(executor);
                }
            }
        } else {
            boolean failure = false;
            boolean needHeader = true;
            for (int i = 0, n = threads.length; i < n; i++) {
                Throwable failureCause = threads[i].getFailureCause();
                
                if (failureCause != null) {
                    if (needHeader) {
                        System.err.println("Testcase: " + getName());
                        needHeader = false;
                    }
                    
                    System.err.println(threads[i].getName() + " failed "
                                       + threads[i].getFailureLocation());
                    failureCause.printStackTrace(System.err);
                    System.err.println();
                    failure = true;
                }
            }
            
            if (failure) {
                System.err.println("-----\n");
                fail();
            }
        }
    }

    protected void setDebug(boolean enabled)
    {
        debug = enabled;
    }

    protected void setDebug(
        boolean enabled,
        PrintStream alternatePrintStream)
    {
        debug = enabled;
        printStream = alternatePrintStream;
    }
}
