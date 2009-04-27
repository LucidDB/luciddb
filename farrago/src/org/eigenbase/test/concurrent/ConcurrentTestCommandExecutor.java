/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2009 The Eigenbase Project
// Copyright (C) 2006-2009 SQLstream, Inc.
// Copyright (C) 2006-2009 LucidEra, Inc.
// Portions Copyright (C) 2006-2009 John V. Sichi
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
package org.eigenbase.test.concurrent;

import java.io.PrintStream;
import java.sql.*;
import java.util.*;

/**
 * ConcurrentTestCommandExecutor is a thread that executes a sequence of
 * {@link ConcurrentTestCommand commands} on a JDBC connection.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
class ConcurrentTestCommandExecutor extends Thread
{
    //~ Instance fields --------------------------------------------------------

    /**
     * The id for this thread.
     */
    private Integer threadId;

    /**
     * JDBC URL to connect with.
     */
    private String jdbcURL;

    /**
     * JDBC Connection properties.
     */
    private Properties jdbcProps;

    /**
     * Command sequence for this thread.
     */
    private Iterator commands;

    /**
     * Used to synchronize command execution.
     */
    private Sync synchronizer;

    /**
     * JDBC connection for commands.
     */
    private Connection connection;

    /**
     * Current JDBC Statement. May be null.
     */
    private Statement statement;

    /**
     * First exception thrown by the thread.
     */
    private Throwable error;

    /**
     * Location of {@link #error}.
     */
    private String when;

    /**
     * Debugging print stream. May be null.
     */
    private final PrintStream debugPrintStream;

    /**
     * Command throwing error *
     */
    private ConcurrentTestCommand errorCommand;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a ConcurrentTestCommandExecutor with the given thread
     * ID, JDBC URL, commands and synchronization object.
     *
     * @param threadId the thread ID (see {@link
     * ConcurrentTestCommandGenerator})
     * @param threadName the thread's name
     * @param jdbcURL the JDBC URL to connect to
     * @param jdbcProps JDBC Connnection properties (user, password, etc.)
     * @param commands the sequence of commands to execute -- null elements
     * indicate no-ops
     * @param synchronizer synchronization object (may not be null);
     * @param debugPrintStream if non-null a PrintStream to use for debugging
     * output (may help debugging thread synchronization issues)
     */
    ConcurrentTestCommandExecutor(
        int threadId,
        String threadName,
        String jdbcURL,
        Properties jdbcProps,
        Iterator commands,
        Sync synchronizer,
        PrintStream debugPrintStream)
    {
        this.threadId = new Integer(threadId);
        this.jdbcURL = jdbcURL;
        this.jdbcProps = jdbcProps;
        this.commands = commands;
        this.synchronizer = synchronizer;
        this.debugPrintStream = debugPrintStream;

        this.setName("Command Executor " + threadName);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Executes the configured commands.
     */
    public void run()
    {
        try {
            connection = DriverManager.getConnection(jdbcURL, jdbcProps);
            if (connection.getMetaData().supportsTransactions()) {
                connection.setAutoCommit(false);
            }
        } catch (Throwable t) {
            handleError(t, "during connect", null);
        }

        // stepNumber is used to reconstitute the original step
        // numbers passed by the test case author.
        int stepNumber = 0;

        while (commands.hasNext()) {
            ConcurrentTestCommand command =
                (ConcurrentTestCommand) commands.next();

            if (!(command
                    instanceof ConcurrentTestCommandGenerator.AutoSynchronizationCommand))
            {
                stepNumber++;
            }

            //  if (debugPrintStream != null) {
            //      debugPrintStream.println(Thread.currentThread().getName()
            //                               + ": Step "
            //                               + stepNumber
            //                               + ": "
            //                               + System.currentTimeMillis());
            //  }

            // synchronization commands are always executed, lest we deadlock
            boolean isSync =
                command
                instanceof ConcurrentTestCommandGenerator.SynchronizationCommand;

            if (isSync
                || ((connection != null)
                    && (command != null)
                    && (error == null)))
            {
                try {
                    command.execute(this);
                } catch (Throwable t) {
                    handleError(t, "during step " + stepNumber, command);
                }
            }
        }

        try {
            if (connection != null) {
                if (connection.getMetaData().supportsTransactions()) {
                    connection.rollback();
                }
                connection.close();
            }
        } catch (Throwable t) {
            handleError(t, "during connection close", null);
        }
    }

    /**
     * Handles details of an exception during execution.
     */
    private void handleError(
        Throwable error,
        String when,
        ConcurrentTestCommand command)
    {
        this.error = error;
        this.when = when;
        this.errorCommand = command;

        if (debugPrintStream != null) {
            debugPrintStream.println(
                Thread.currentThread().getName() + ": "
                + when);
            error.printStackTrace(debugPrintStream);
        }
    }

    /**
     * Obtains the thread's JDBC connection.
     */
    public Connection getConnection()
    {
        return connection;
    }

    /**
     * Obtains the thread's current JDBC statement. May return null.
     */
    public Statement getStatement()
    {
        return statement;
    }

    /**
     * Sets the thread's current JDBC statement. To clear the JDBC statement use
     * {@link #clearStatement()}.
     */
    public void setStatement(Statement stmt)
    {
        // assert that we don't already have a statement
        assert (statement == null);

        statement = stmt;
    }

    /**
     * Clears the thread's current JDBC statement. To set the JDBC statement use
     * {@link #setStatement(Statement)}.
     */
    public void clearStatement()
    {
        statement = null;
    }

    /**
     * Retrieves the object used to synchronize threads at a point in the list
     * of commands.
     */
    public Sync getSynchronizer()
    {
        return synchronizer;
    }

    /**
     * Checks whether an exception occurred during execution. If this method
     * returns null, the thread's commands all succeeded. If this method returns
     * non-null, see {@link #getFailureLocation()} for details on which command
     * caused the failure.
     */
    public Throwable getFailureCause()
    {
        return error;
    }

    /**
     * Returns location (e.g., command number) for exception returned by {@link
     * #getFailureCause()}.
     */
    public String getFailureLocation()
    {
        return when;
    }

    public ConcurrentTestCommand getFailureCommand()
    {
        return errorCommand;
    }

    public Integer getThreadId()
    {
        return threadId;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Synchronization object that allows multiple
     * ConcurrentTestCommandExecutors to execute commands in lock-step.
     * Requires that all ConcurrentTestCommandExecutors have the same
     * number of commands.
     */
    public static class Sync
    {
        private int numThreads;
        private int numWaiting;

        public Sync(int numThreads)
        {
            assert (numThreads > 0);
            this.numThreads = numThreads;
            this.numWaiting = 0;
        }

        synchronized void waitForOthers()
            throws InterruptedException
        {
            if (++numWaiting == numThreads) {
                numWaiting = 0;
                notifyAll();
            } else {
                // REVIEW: SZ 6/17/2004: Need a timeout here --
                // otherwise a test case will hang forever if there's
                // a deadlock.  The question is, how long should the
                // timeout be to avoid falsely detecting deadlocks?
                wait();
            }
        }
    }
}

// End ConcurrentTestCommandExecutor.java
