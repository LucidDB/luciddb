/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
package org.eigenbase.runtime;

import java.util.*;

import junit.framework.*;

import org.eigenbase.util.*;


/**
 * Test case for {@link TimeoutQueueTupleIter}.
 */
public class TimeoutQueueTupleIterTest
    extends TestCase
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Multiplier which determines how long each logical clock tick lasts, and
     * therefore how fast the test is run. If you are getting sporadic problems,
     * raise the value. 100 seems to be too low; 200 seems to be OK on my 1.8GHz
     * laptop.
     */
    private static final int tickMillis = 1000;

    //~ Instance fields --------------------------------------------------------

    /**
     * Timestamp at which the test started. All timeouts are relative to this.
     */
    private long startTime;

    //~ Constructors -----------------------------------------------------------

    public TimeoutQueueTupleIterTest(String s)
        throws Exception
    {
        super(s);
    }

    //~ Methods ----------------------------------------------------------------

    public void testDummy()
    {
    }

    // NOTE jvs 21-Oct-2006:  I'm disabling this test because
    // it fails sporadically, and we're planning to eliminate
    // usage of this class anyway (http://issues.eigenbase.org/browse/FRG-168).
    public void _testTimeoutTupleIter()
    {
        startTime = System.currentTimeMillis();
        String [] values = { "a", "b", null, "d" };
        TickIterator tickIter = new TickIterator(values, false, startTime);
        TimeoutQueueTupleIter timeoutIter = new TimeoutQueueTupleIter(tickIter);
        timeoutIter.start();

        // tick 1: fetchNext returns "a"
        assertFetchNext(
            timeoutIter,
            "a",
            toMillis(1.1));

        // nothing available until tick 2
        assertFetchNextTimesOut(
            timeoutIter,
            toMillis(1.5));

        // tick 2: fetchNext returns "b"
        assertFetchNext(
            timeoutIter,
            "b",
            toMillis(2.1));

        // tick 3: fetchNext returns null
        assertFetchNext(
            timeoutIter,
            null,
            toMillis(3.1));

        assertFetchNextTimesOut(
            timeoutIter,
            toMillis(3.8));

        // tick 4: fetchNext returns "d"
        assertFetchNext(
            timeoutIter,
            "d",
            toMillis(4.5));

        // tick 5: fetchNext returns NoDataReason.END_OF_DATA
        assertFetchNext(
            timeoutIter,
            TupleIter.NoDataReason.END_OF_DATA,
            toMillis(6.0));
    }

    private void assertFetchNext(
        TimeoutQueueTupleIter timeoutIter,
        Object expected,
        long timeoutMillis)
    {
        try {
            Object actual = timeoutIter.fetchNext(timeoutMillis);
            assertEquals(expected, actual);
        } catch (QueueIterator.TimeoutException e) {
            fail("fetchNext() timed out at " + new Date());
        }
    }

    private void assertFetchNextTimesOut(
        TimeoutQueueTupleIter timeoutIter,
        long timeoutMillis)
    {
        try {
            Object o = timeoutIter.fetchNext(timeoutMillis);
            Util.discard(o);
            fail("fetchNext() did not time out at " + new Date());
        } catch (QueueIterator.TimeoutException e) {
            // success -- we timed out
        }
    }

    private long toMillis(double tick)
    {
        long endTime = startTime + (long) (tick * tickMillis);
        return endTime - System.currentTimeMillis();
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Iterator which returns an element from an array on a regular basis.
     *
     * <p>Every clock tick until the array is exhausted, {@link
     * Iterator#hasNext} returns true, then the following clock tick, {@link
     * Iterator#next} returns an object. If you call a method too early, the
     * method waits until the appropriate time.
     */
    private static class TickIterator
        extends AbstractTupleIter
    {
        private final boolean verbose;
        private final long startTime;
        private int current;
        private final Object [] values;

        TickIterator(
            Object [] values,
            boolean verbose,
            long startTime)
        {
            this.values = values;
            this.verbose = verbose;
            this.startTime = startTime;
        }

        private void waitUntil(int tick)
        {
            long timeToWait =
                (startTime + (tick * TimeoutQueueTupleIterTest.tickMillis))
                - System.currentTimeMillis();
            if (timeToWait > 0) {
                try {
                    Thread.sleep(timeToWait);
                } catch (InterruptedException e) {
                }
            }
        }

        public Object fetchNext()
        {
            int tick = current + 1;
            waitUntil(tick);
            if (current < values.length) {
                Object value = values[current];
                if (verbose) {
                    System.out.println(
                        new Date() + " (tick " + tick + ") return "
                        + value);
                }
                ++current;
                return value;
            }

            return NoDataReason.END_OF_DATA;
        }

        public void restart()
        {
            throw new UnsupportedOperationException();
        }

        public void closeAllocation()
        {
        }

        public static void demo()
        {
            String [] values = { "a", "b", "c" };
            TickIterator tickIterator =
                new TickIterator(
                    values,
                    true,
                    System.currentTimeMillis());
            Object o;
            while ((o = tickIterator.fetchNext()) != NoDataReason.END_OF_DATA) {
                Util.discard(o);
            }
        }
    }
}

// End TimeoutQueueTupleIterTest.java
