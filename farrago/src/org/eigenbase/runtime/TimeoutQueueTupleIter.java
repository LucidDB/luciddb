/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2002-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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

import org.eigenbase.util.Util;

/**
 * Adapter which allows you to iterate over an {@link TupleIter} with a timeout.
 *
 * <p>The interface is similar to an {@link TupleIter}: the {@link #fetchNext}
 * method retrieves rows and indicates when there are no more rows. It has a 
 * timeout parameter, and throws a {@link QueueIterator.TimeoutException} if 
 * the timeout is exceeded. There is also a {@link #closeAllocation} method, 
 * which you must call.
 *
 * <p>The class is implemented using a thread which reads from the underlying
 * TupleIter and places the results into a {@link QueueIterator}. If a method
 * call times out, the underlying thread will wait for the result of the call
 * until it completes.
 *
 * <p>There is no facility to cancel the fetch from the underlying iterator.
 *
 * @author Stephan Zuecher (based on tleung's TimeoutQueueIterator)
 * @version $Id$
 */
public class TimeoutQueueTupleIter
{
    //~ Instance fields -------------------------------------------------------

    private final QueueIterator queueIterator;
    private final TupleIter producer;
    private Thread thread;

    //~ Constructors ----------------------------------------------------------

    public TimeoutQueueTupleIter(TupleIter producer)
    {
        this.producer = producer;
        this.queueIterator = new QueueIterator();
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * 
     */
    public Object fetchNext(long timeoutMillis) 
        throws QueueIterator.TimeoutException
    {
        long endTime = System.currentTimeMillis() + timeoutMillis;
        if (queueIterator.hasNext(timeoutMillis)) {
            long remainingTimeout =
                endTime - System.currentTimeMillis();
            if (remainingTimeout <= 0) {
                // hasNext() took too long
                throw new QueueIterator.TimeoutException();
            }
            
            return queueIterator.next(remainingTimeout);
        } else {
            return TupleIter.NoDataReason.END_OF_DATA;
        }
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
        thread.setName("TimeoutQueueTupleIter" + thread.getName());
        thread.start();
    }

    /**
     * Releases the resources used by this iterator, including killing the
     * underlying thread.
     *
     * @param timeoutMillis Timeout while waiting for the underlying thread to
     *   die. Zero means wait forever.
     */
    public synchronized void closeAllocation(long timeoutMillis)
    {
        if (thread != null) {
            try {
                // Empty the queue -- the thread will wait for us to consume
                // all items in the queue, hanging the join call.
                while(queueIterator.hasNext()) {
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
            while(true) {
                Object next = producer.fetchNext();
                
                if (next == TupleIter.NoDataReason.END_OF_DATA) {
                    break;
                } else if (next instanceof TupleIter.NoDataReason) {
                    // TODO: SWZ: 2/23/2006: Better exception
                    throw new RuntimeException();
                }
                
                queueIterator.put(next);
            }
            
            // Signal that the stream ended without error.
            queueIterator.done(null);
        } catch (Throwable e) {
            // Signal that the stream ended with an error.
            queueIterator.done(e);
        }
    }
}

// End TimeoutQueueTupleIter.java
