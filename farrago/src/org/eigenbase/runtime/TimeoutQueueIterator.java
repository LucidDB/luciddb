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

import org.eigenbase.util.*;


// REVIEW: SWZ: 7/13/2006: In principal this class also exhibits the same bug
// that was fixed in //open/dt/dev/.../TimeoutQueueTupleIter#2.  It doesn't
// occur because this class is no longer used with row objects.  It is still
// used for "explain plan" but since the output there is immutable Strings,
// and because the query timeout isn't propagated to the result set used,
// the bug doesn't occur.  Leaving it unfixed here, since the pattern used to
// fix TimeoutQueueTupleIter is hard to apply to this class's Iterator
// calling convention.  Prehaps we should migrate explain plan to TupleIter
// convention and eliminate this class altogether.

/**
 * Adapter which allows you to iterate over an {@link Iterator} with a timeout.
 *
 * <p>The interface is similar to an {@link Iterator}: the {@link #hasNext}
 * method tests whether there are more rows, and the {@link #next} method gets
 * the next row. Each has a timeout parameter, and throws a {@link
 * QueueIterator.TimeoutException} if the timeout is exceeded. There is also a
 * {@link #close} method, which you must call.
 *
 * <p>The class is implemented using a thread which reads from the underlying
 * iterator and places the results into a {@link QueueIterator}. If a method
 * call times out, the underlying thread will wait for the result of the call
 * until it completes.
 *
 * <p>There is no facility to cancel the fetch from the underlying iterator.
 *
 * @author tleung
 * @version $Id$
 * @since Jun 20, 2004
 * @testcase
 */
public class TimeoutQueueIterator
{
    //~ Instance fields --------------------------------------------------------

    protected final QueueIterator queueIterator; // only protected to suit QueueIteratorTest
    private final Iterator producer;
    private Thread thread;

    //~ Constructors -----------------------------------------------------------

    public TimeoutQueueIterator(Iterator producer)
    {
        this.producer = producer;
        this.queueIterator = new QueueIterator();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns whether the producer has another row, if that can be determined
     * within the timeout interval.
     *
     * @param timeoutMillis Millisonds to wait; less than or equal to zero means
     * don't wait
     *
     * @throws QueueIterator.TimeoutException if producer does not answer within
     * the timeout interval
     */
    public boolean hasNext(long timeoutMillis)
        throws QueueIterator.TimeoutException
    {
        return queueIterator.hasNext(timeoutMillis);
    }

    /**
     * Returns the next row from the producer, if it can be fetched within the
     * timeout interval.
     *
     * @throws QueueIterator.TimeoutException if producer does not answer within
     * the timeout interval
     */
    public Object next(long timeoutMillis)
        throws QueueIterator.TimeoutException
    {
        return queueIterator.next(timeoutMillis);
    }

    /**
     * Starts the thread which reads from the consumer.
     *
     * @pre thread == null // not previously started
     */
    public synchronized void start()
    {
        Util.pre(thread == null, "thread == null");
        thread =
            new Thread() {
                public void run()
                {
                    doWork();
                }
            };
        thread.setName("TimeoutQueueIterator" + thread.getName());
        thread.start();
    }

    /**
     * Releases the resources used by this iterator, including killing the
     * underlying thread.
     *
     * @param timeoutMillis Timeout while waiting for the underlying thread to
     * die. Zero means wait forever.
     */
    public synchronized void close(long timeoutMillis)
    {
        if (thread != null) {
            try {
                // Empty the queue -- the thread will wait for us to consume
                // all items in the queue, hanging the join call.
                while (queueIterator.hasNext()) {
                    queueIterator.next();
                }
                thread.join(timeoutMillis);
            } catch (InterruptedException e) {
            }
            thread = null;
        }
    }

    /**
     * Reads objects from the producer and writes them into the QueueIterator.
     * This is the method called by the thread when you call {@link #start}.
     * Never throws an exception.
     */
    private void doWork()
    {
        try {
            while (producer.hasNext()) {
                final Object o = producer.next();
                queueIterator.put(o);
            }

            // Signal that the stream ended without error.
            queueIterator.done(null);
        } catch (Throwable e) {
            // Signal that the stream ended with an error.
            queueIterator.done(e);
        }
    }
}

// End TimeoutQueueIterator.java
