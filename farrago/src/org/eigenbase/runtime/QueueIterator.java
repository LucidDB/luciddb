/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 * Adapter which exposes a 'push' producer on one thread into an
 * {@link Iterator} for use by another thread.
 *
 * <p>The queue contains at most one object. If you call {@link
 * #next}, your thread will wait until another thread calls {@link #put} or
 * {@link #done}. Nulls are allowed. If the producer has an error, they can
 * pass it to the consumer via {@link #done}.</p>
 *
 * @author jhyde
 * @since Oct 20, 2003
 * @version $Id$
 */
public class QueueIterator implements Iterator
{
    //~ Instance fields -------------------------------------------------------

    protected Object next;

    /**
     * The producer notifies <code>empty</code> every time it produces an
     * object (or finishes). The consumer waits for it.
     */
    protected Semaphore empty;

    /**
     * Conversely, the consumer notifies <code>full</code> every time it reads
     * the next object. The producer waits for it, then starts work.
     */
    protected Semaphore full;
    protected Throwable throwable;
    protected boolean hasNext;

    /** Protects the {@link #full} semaphore. */
    protected boolean waitingForProducer;

    //~ Constructors ----------------------------------------------------------

    public QueueIterator()
    {
        empty = new Semaphore(0);
        full = new Semaphore(1);
        waitingForProducer = true;
        hasNext = true;
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Producer calls <code>done</code> to say that there are no more objects,
     * setting <code>throwable</code> if there was an error.
     */
    public void done(Throwable throwable)
    {
        // This method is NOT synchronized. If it were, it would deadlock with
        // the waiting consumer thread. The consumer thread has called
        // consumed.release(), and that is sufficient.
        full.acquire(); // wait for consumer thread to use previous
        hasNext = false;
        next = null;
        this.throwable = throwable;
        empty.release(); // wake up consumer thread
    }

    // implement Iterator
    public synchronized boolean hasNext()
    {
        if (waitingForProducer) {
            empty.acquire(); // wait for producer to produce one
            waitingForProducer = false;
        }
        if (!hasNext) {
            checkError();
        }
        return hasNext;
    }

    /**
     * As {@link #hasNext}, but throws {@link TimeoutException} if no row is
     * available within the timeout.
     *
     * @param timeoutMillis Milliseconds to wait; less than or equal to zero
     *   means don't wait
     */
    public synchronized boolean hasNext(long timeoutMillis)
        throws TimeoutException
    {
        if (waitingForProducer) {
            // wait for producer to produce one
            boolean isLocked = empty.tryAcquire(timeoutMillis);
            if (!isLocked) {
                // Throw an exception indicating timeout. It's not a 'real'
                // error, so we don't set the '_throwable' field. It's OK
                // to call this method again.
                throw new TimeoutException();
            }
            waitingForProducer = false;
        }
        if (!hasNext) {
            checkError();
        }
        return hasNext;
    }

    // implement Iterator
    public synchronized Object next()
    {
        if (waitingForProducer) {
            empty.acquire(); // wait for producer to produce one
            waitingForProducer = false;
        }
        if (!hasNext) {
            checkError();

            // It is illegal to call next when there are no more objects.
            throw new NoSuchElementException();
        }
        Object o = next;
        waitingForProducer = true;
        full.release();
        return o;
    }

    /**
     * As {@link #next}, but throws {@link TimeoutException} if no row is
     * available within the timeout.
     *
     * @param timeoutMillis Milliseconds to wait; less than or equal to zero
     *   means don't wait
     */
    public synchronized Object next(long timeoutMillis)
        throws TimeoutException
    {
        if (waitingForProducer) {
            // wait for producer to produce one
            boolean isLocked = empty.tryAcquire(timeoutMillis);
            if (!isLocked) {
                // Throw an exception indicating timeout. It's not a 'real'
                // error, so we don't set the '_throwable' field. It's OK
                // to call this method again.
                throw new TimeoutException();
            }
            waitingForProducer = false;
        }
        if (!hasNext) {
            checkError();

            // It is illegal to call next when there are no more objects.
            throw new NoSuchElementException();
        }
        Object o = next;
        waitingForProducer = true;
        full.release();
        return o;
    }

    /**
     * Producer calls <code>put</code> to add another object (which may be
     * null).
     *
     * @throws IllegalStateException if this method is called after
     *   {@link #done}
     */
    public void put(Object o)
    {
        // This method is NOT synchronized. If it were, it would deadlock with
        // the waiting consumer thread. The consumer thread has called
        // full.release(), and that is sufficient.
        full.acquire(); // wait for consumer thread to use previous
        if (!hasNext) {
            // It is illegal to add a new object after done() has been called.
            throw new IllegalStateException();
        }
        next = o;
        empty.release(); // wake up consumer thread
    }

    // implement Iterator
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Throws an error if one has been set via {@link #done(Throwable)}.
     */
    protected void checkError()
    {
        if (throwable == null) {
            ;
        } else if (throwable instanceof RuntimeException) {
            throw (RuntimeException) throwable;
        } else if (throwable instanceof Error) {
            throw (Error) throwable;
        } else {
            throw new Error("error: " + throwable);
        }
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Thrown by {@link #hasNext(long)} and {@link #next(long)} to indicate that
     * operation timed out before rows were available.
     */
    public static class TimeoutException extends Exception
    {
    }
}


// End QueueIterator.java
