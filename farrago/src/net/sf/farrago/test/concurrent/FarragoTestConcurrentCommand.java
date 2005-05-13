/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
package net.sf.farrago.test.concurrent;


/**
 * FarragoTestConcurrentCommand represents a command, sequentially executed by
 * {@link FarragoTestConcurrentCommandExecutor}, during a concurrency test
 * ({@link FarragoTestConcurrentTestCase}.
 *
 * <p>FarragoTestConcurrentCommand instances are normally instantiated by the
 * {@link FarragoTestConcurrentCommandGenerator} class.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public interface FarragoTestConcurrentCommand
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Executes this command.  The
     * FarragoTestConcurrentCommandExecutor provides access to a JDBC
     * connection and previously prepared statements.
     *
     * @param exec the FarragoTestConcurrentCommandExecutor firing
     *             this command.
     * @see FarragoTestConcurrentCommandExecutor#getStatement()
     * @see FarragoTestConcurrentCommandExecutor#setStatement(java.sql.Statement)
     * @throws Exception to indicate a test failure
     */
    void execute(FarragoTestConcurrentCommandExecutor exec)
        throws Exception;

    /**
     * Marks a command to show that it is expected to fail, and
     * indicates how.  Used for negative tests.  Normally when a
     * command fails the embracing test fails (see {@link
     * FarragoTestConcurrentTestCase#executeTest(FarragoTestConcurrentCommandGenerator,
     * boolean)}).  But when a marked command fails, the error is
     * caught and inspected: if it matches the expected error, the
     * test continues.  However if it does not match, if another kind
     * of exception is thrown, or if no exception is caught, then the
     * test fails.
     *
     * Assumes the error is indicated by a java.sql.SQLException.
     * Optionally checks for the expected error condition by matching
     * the error message against a regular expression. (Scans the list
     * of chained SQLExceptions).
     *
     * @param comment a brief description of the expected error
     * @param pattern null, or a regular expression that matches the
     *                expected error message.
     */
    FarragoTestConcurrentCommand markToFail(
        String comment,
        String pattern);

    //~ Inner Classes ---------------------------------------------------------

    /** Indicates that a command should have failed, but instead succeeded,
     * which is a test error */
    public static class ShouldHaveFailedException extends RuntimeException
    {
        public final String description;

        public ShouldHaveFailedException(String description)
        {
            this.description = description;
        }
    }
}
