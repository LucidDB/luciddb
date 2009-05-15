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

/**
 * ConcurrentTestCommand represents a command, sequentially executed by
 * {@link ConcurrentTestCommandExecutor}, during a concurrency test
 *
 * <p>ConcurrentTestCommand instances are normally instantiated by the
 * {@link ConcurrentTestCommandGenerator} class.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public interface ConcurrentTestCommand
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Executes this command. The ConcurrentTestCommandExecutor provides
     * access to a JDBC connection and previously prepared statements.
     *
     * @param exec the ConcurrentTestCommandExecutor firing this command.
     *
     * @throws Exception to indicate a test failure
     *
     * @see ConcurrentTestCommandExecutor#getStatement()
     * @see ConcurrentTestCommandExecutor#setStatement(java.sql.Statement)
     */
    void execute(ConcurrentTestCommandExecutor exec)
        throws Exception;

    /**
     * Marks a command to show that it is expected to fail, and indicates how.
     * Used for negative tests. Normally when a command fails the embracing test
     * fails.
     * But when a marked command fails, the error is caught and inspected: if it
     * matches the expected error, the test continues. However if it does not
     * match, if another kind of exception is thrown, or if no exception is
     * caught, then the test fails. Assumes the error is indicated by a
     * java.sql.SQLException. Optionally checks for the expected error condition
     * by matching the error message against a regular expression. (Scans the
     * list of chained SQLExceptions).
     *
     * @param comment a brief description of the expected error
     * @param pattern null, or a regular expression that matches the expected
     * error message.
     */
    ConcurrentTestCommand markToFail(
        String comment,
        String pattern);

    /**
     * Returns true if the command should fail. This allows special error
     * handling for expected failures that don't have patterns.
     *
     * @return true if command is expected to fail
     */
    boolean isFailureExpected();

    /**
     * Set this command to expect a patternless failure.
     */
    ConcurrentTestCommand markToFail();

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Indicates that a command should have failed, but instead succeeded, which
     * is a test error
     */
    public static class ShouldHaveFailedException
        extends RuntimeException
    {
        private final String description;

        public ShouldHaveFailedException(String description)
        {
            this.description = description;
        }

        public String getDescription()
        {
            return description;
        }
    }
}

// End ConcurrentTestCommand.java
