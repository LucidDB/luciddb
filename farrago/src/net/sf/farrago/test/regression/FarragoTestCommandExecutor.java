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
package net.sf.farrago.test.regression;

import java.util.Iterator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * FarragoTestCommandExecutor is a thread that executes a sequence of
 * {@link FarragoTestCommand commands} on a JDBC connection.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public class FarragoTestCommandExecutor
    extends Thread
{
    /** JDBC URL to connect with. */
    private String jdbcURL;

    /** Command sequence for this thread. */
    private Iterator commands;

    /** 
     * Used to synchronize command execution.
     */
    private Sync synchronizer;

    /** JDBC connection for commands. */
    private Connection connection;

    /** Current JDBC Statement.  May be null. */
    private Statement statement;

    /** First exception thrown by the thread. */
    private Throwable error;

    /** Location of {@link #error}. */
    private String when;


    /**
     * Constructs a FarragoTestCommandExecutor with the given thread
     * ID, JDBC URL, commands and synchronization object.
     *
     * @param threadId the thread ID (see {@link FarragoTestCommandGenerator})
     * @param jdbcURL the JDBC URL to connect to
     * @param commands the sequence of commands to execute -- null
     *                 elements indicate no-ops
     * @param synchronizer synchronization object (may not be null);
     */
    FarragoTestCommandExecutor(int threadId,
                               String jdbcURL,
                               Iterator commands,
                               Sync synchronizer)
    {
        this.jdbcURL = jdbcURL;
        this.commands = commands;
        this.synchronizer = synchronizer;

        this.setName("Command Executor " + threadId);
    }


    /**
     * Executes the configured commands.
     */
    public void run()
    {
        try {
            connection = DriverManager.getConnection(jdbcURL);
            connection.setAutoCommit(false);
        } catch(Throwable t) {
            error = t;
            when = "during connect";
        }

        // stepNumber is used to reconstitute the original step
        // numbers passed by the test case author.
        int stepNumber = 0;

        while(commands.hasNext()) {
            FarragoTestCommand command = (FarragoTestCommand)commands.next();

            if (!(command instanceof FarragoTestCommandGenerator.AutoSynchronizationCommand)) {
                stepNumber++;
            }

            // synchronization commands are always executed, lest we deadlock
            boolean isSync = command instanceof FarragoTestCommandGenerator.SynchronizationCommand;

            if (isSync ||
                (connection != null && command != null && error == null)) {
                try {
                    command.execute(this);
                } catch(Throwable t) {
                    error = t;
                    when = "during step " + stepNumber;
                }
            }
        }

        try {
            if (connection != null) {
                connection.rollback();
                connection.close();
            }
        } catch(Throwable t) {
            error = t;
            when = "during connection close";
        }
    }


    /**
     * Obtain the thread's JDBC connection.
     */
    public Connection getConnection()
    {
        return connection;
    }


    /**
     * Obtain the thread's current JDBC statement.  May return null.
     */
    public Statement getStatement()
    {
        return statement;
    }
    

    /**
     * Set the thread's current JDBC statement.  To clear the JDBC
     * statement use {@link #clearStatement()}.
     */
    public void setStatement(Statement stmt)
    {
        // assert that we don't already have a statement
        assert(statement == null);

        statement = stmt;
    }
    

    /**
     * Clear the thread's current JDBC statement.  To set the JDBC
     * statement use {@link #setStatement(Statement)}.
     */
    public void clearStatement()
    {
        statement = null;
    }


    /**
     * Retrieve the object used to synchronize threads at a point in
     * the list of commands.
     */
    public Sync getSynchronizer()
    {
        return synchronizer;
    }
    

    /**
     * Check whether an exception occurred during execution.  If this
     * method returns null, the thread's commands all succeeded.  If
     * this method returns non-null, see {@link #getFailureLocation()}
     * for details on which command caused the failure.
     */
    public Throwable getFailureCause()
    {
        return error;
    }

    /**
     * Return location (e.g., command number) for exception returned
     * by {@link #getFailureCause()}.
     */
    public String getFailureLocation()
    {
        return when;
    }

    
    /**
     * Synchronization object that allows multiple
     * FarragoTestCommandExecutors to execute commands in lock-step.
     * Requires that all FarragoTestCommandExecutors have the same
     * number of commands.
     */
    public static class Sync
    {
        private int numThreads;
        private int numWaiting;

        public Sync(int numThreads)
        {
            assert(numThreads > 0);
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
