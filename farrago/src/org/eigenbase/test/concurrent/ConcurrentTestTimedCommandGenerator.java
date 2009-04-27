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
import java.util.*;

/**
 * ConcurrentTestTimedCommandGenerator extends {@link
 * ConcurrentTestCommandGenerator} and repeats the configured command
 * sequence until a certain amount of time has elapsed.
 *
 * <p>The command sequence is always completed in full, even if the time limit
 * has been exceeded. Therefore, the time limit can only be considered the
 * minimum length of time that the test will run and not a guarantee of how long
 * the test will take.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public class ConcurrentTestTimedCommandGenerator
    extends ConcurrentTestCommandGenerator
{
    //~ Instance fields --------------------------------------------------------

    private int runTimeSeconds;
    private long endTimeMillis;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a new ConcurrentTestTimedCommandGenerator that will run
     * for at least the given amount of time. See {@link
     * ConcurrentTestTimedCommandGenerator} for more information on the
     * semantics of run-time length.
     *
     * @param runTimeSeconds minimum run-time length, in seconds
     */
    public ConcurrentTestTimedCommandGenerator(int runTimeSeconds)
    {
        super();

        this.runTimeSeconds = runTimeSeconds;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Retrieves an Iterator based on the configured commands. This Iterator,
     * when it reaches the end of the command list will compare the current time
     * with the test's end time. If there is time left, the Iterator will repeat
     * the command sequence.
     *
     * <p>The test's end time is computed by taking the value of <code>
     * System.currentTimeMillis()</code> the first time this method is called
     * (across all thread IDs) and adding the configured run time.
     *
     * @param threadId the thread ID to get an Iterator on
     */
    Iterator getCommandIterator(Integer threadId)
    {
        synchronized (this) {
            if (endTimeMillis == 0L) {
                endTimeMillis =
                    System.currentTimeMillis() + (runTimeSeconds * 1000);
            }
        }

        return new TimedIterator(
            getCommands(threadId),
            endTimeMillis);
    }

    /**
     * Outputs command sequence and notes how long the sequence will be
     * repeated.
     */
    void printCommands(
        PrintStream out,
        Integer threadId)
    {
        super.printCommands(out, threadId);
        out.println("Repeat sequence for " + runTimeSeconds + " seconds");
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * TimedIterator is an Iterator that repeats a given collection's elements
     * until <code>System.currentTimeMillis() >= endTimeMillis</code>.
     */
    private class TimedIterator
        implements Iterator
    {
        private Object [] commands;
        private long endTimeMillis;
        private int commandIndex;

        private TimedIterator(
            Collection commands,
            long endTimeMillis)
        {
            this.commands = commands.toArray();
            this.endTimeMillis = endTimeMillis;
            this.commandIndex = 0;
        }

        public boolean hasNext()
        {
            if (commandIndex < commands.length) {
                return true;
            }

            if (System.currentTimeMillis() < endTimeMillis) {
                commandIndex = 0;
                return commands.length > 0; // handle empty array
            }

            return false;
        }

        public Object next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            return commands[commandIndex++];
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }
}

// End ConcurrentTestTimedCommandGenerator.java
