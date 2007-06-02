/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 Disruptive Tech
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
 * Test case for {@link TimeoutQueueIterator}.
 */
public class TimeoutIteratorTest
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

    public TimeoutIteratorTest(String s)
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
    public void _testTimeoutIterator()
    {
        startTime = System.currentTimeMillis();
        String [] values = { "a", "b", null, "d" };
        TickIterator tickIter = new TickIterator(values, false, startTime);
        TimeoutQueueIterator timeoutIter = new TimeoutQueueIterator(tickIter);
        timeoutIter.start();

        // tick 1: hasNext() returns true at tick 1
        // tick 2: next() returns "a"
        // tick 2: object is available
        assertHasNext(
            timeoutIter,
            true,
            toMillis(2.1));

        // call next with zero timeout -- it already has the answer
        assertNext(timeoutIter, "a", 0);

        // tick 3: hasNext returns true at tick 3
        assertHasNextTimesOut(
            timeoutIter,
            toMillis(2.7));
        assertHasNextTimesOut(
            timeoutIter,
            toMillis(2.9));

        // tick 4: next returns "b"
        // tick 4: object is available
        assertNextTimesOut(
            timeoutIter,
            toMillis(3.3));

        // call next with zero timeout will timeout immediately (not the
        // same as JDBC ResultSet.setQueryTimeout(0), which means don't
        // timeout ever)
        assertNextTimesOut(timeoutIter, 0);
        assertNextTimesOut(
            timeoutIter,
            toMillis(3.6));
        assertNextTimesOut(
            timeoutIter,
            toMillis(3.8));
        assertNext(
            timeoutIter,
            "b",
            toMillis(4.2));

        // tick 5: hasNext returns true
        // tick 6: next returns null (does not mean end of data)
        // tick 6: object is available
        // tick 7: hasNext returns true
        // tick 8: next returns "d"
        // tick 8: object is available
        assertHasNext(
            timeoutIter,
            true,
            toMillis(8.1));

        // call hasNext twice in succession
        assertHasNext(
            timeoutIter,
            true,
            toMillis(8.2));

        // call hasNext with zero timeout -- it already has the answer
        assertHasNext(timeoutIter, true, 0);

        // call hasNext with non-zero timeout -- it already has the answer
        assertHasNext(timeoutIter, true, 10);
        assertNext(
            timeoutIter,
            null,
            toMillis(8.2));

        // call next() without calling hasNext() is legal
        assertNext(
            timeoutIter,
            "d",
            toMillis(8.3));
        assertHasNextTimesOut(
            timeoutIter,
            toMillis(8.4));
        assertHasNextTimesOut(
            timeoutIter,
            toMillis(8.5));

        // tick 9: hasNext returns false
        // tick 9: no object is available
        assertHasNext(
            timeoutIter,
            false,
            toMillis(10.5));
        try {
            timeoutIter.next(100);
            fail("did not throw NoSuchElementException");
        } catch (QueueIterator.TimeoutException e) {
            fail("next() timed out");
        } catch (NoSuchElementException e) {
            // perfect
        }
    }

    private void assertHasNext(
        TimeoutQueueIterator timeoutIter,
        boolean expected,
        long timeoutMillis)
    {
        try {
            boolean b = timeoutIter.hasNext(timeoutMillis);
            assertEquals(expected, b);
        } catch (QueueIterator.TimeoutException e) {
            fail("hasNext() timed out at " + new Date());
        }
    }

    private void assertHasNextTimesOut(
        TimeoutQueueIterator timeoutIter,
        long timeoutMillis)
    {
        try {
            if (false) {
                System.out.println(
                    "entering hasNext at " + new Date()
                    + " with " + timeoutMillis);
            }
            boolean b = timeoutIter.hasNext(timeoutMillis);
            fail(
                "hasNext() returned " + b + " and did not time out at "
                + new Date());
        } catch (QueueIterator.TimeoutException e) {
            // success -- we timed out
        }
    }

    private void assertNext(
        TimeoutQueueIterator timeoutIter,
        Object expected,
        long timeoutMillis)
    {
        try {
            Object actual = timeoutIter.next(timeoutMillis);
            assertEquals(expected, actual);
        } catch (QueueIterator.TimeoutException e) {
            fail("next() timed out at " + new Date());
        }
    }

    private void assertNextTimesOut(
        TimeoutQueueIterator timeoutIter,
        long timeoutMillis)
    {
        try {
            Object o = timeoutIter.next(timeoutMillis);
            Util.discard(o);
            fail("next() did not time out at " + new Date());
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
     * <p>Every clock tick until the array is exhausted, {@link #hasNext}
     * returns true, then the following clock tick, {@link #next} returns an
     * object. If you call a method too early, the method waits until the
     * appropriate time.
     */
    private static class TickIterator
        implements Iterator
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

        public boolean hasNext()
        {
            int tick = (current * 2) + 1;
            waitUntil(tick);
            if (current < values.length) {
                if (verbose) {
                    System.out.println(
                        new Date() + " (tick " + tick
                        + ") hasNext returns true");
                }
                return true;
            } else {
                if (verbose) {
                    System.out.println(
                        new Date() + " (tick " + tick
                        + ") hasNext returns false");
                }
                return false;
            }
        }

        private void waitUntil(int tick)
        {
            long timeToWait =
                (startTime + (tick * TimeoutIteratorTest.tickMillis))
                - System.currentTimeMillis();
            if (timeToWait > 0) {
                try {
                    Thread.sleep(timeToWait);
                } catch (InterruptedException e) {
                }
            }
        }

        public Object next()
        {
            int tick = (current * 2) + 2;
            waitUntil(tick);
            Object value = values[current];
            if (verbose) {
                System.out.println(
                    new Date() + " (tick " + tick + ") return "
                    + value);
            }
            ++current;
            return value;
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        public static void demo()
        {
            String [] values = { "a", "b", "c" };
            TickIterator tickIterator =
                new TickIterator(
                    values,
                    true,
                    System.currentTimeMillis());
            while (tickIterator.hasNext()) {
                Util.discard(tickIterator.next());
            }
        }
    }
}

// End TimeoutIteratorTest.java
