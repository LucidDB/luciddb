/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

import org.eigenbase.trace.EigenbaseLogger;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.logging.Logger;
import java.util.logging.Level;


/**
 * Adapter that exposes a 'push' producer as an {@link Iterator}.
 * Supports one or more producers feeding into a single consumer. The consumer and
 * the producers must each run in its own thread. When there are several producers
 * the data is merged as it arrives: no sorting.
 *
 * <p>The queue contains at most one object. If you call {@link
 * #next}, your thread will wait until a producer thread calls {@link #put} or
 * {@link #done}. Nulls are allowed. If a producer has an error, it can
 * pass it to the consumer via {@link #done}.</p>
 *
 * @author jhyde
 * @since Oct 20, 2003
 * @version $Id$
 */
public class QueueIterator implements Iterator
{
    //~ Instance fields -------------------------------------------------------
    private int numProducers;
    // a wrapping class can provide its tracer, which is used here to trace synchronization events
    private final EigenbaseLogger tracer; 

    /** next Iterator value (can be null) */
    protected Object next;
    /** false when Iterator is finished */
    protected boolean hasNext;
    /** true when Iterator is waiting for next value from producer.
     * Protects the {@link #full} semaphore. */
    protected boolean waitingForProducer;
    protected Throwable throwable;

    /**
     * A  producer notifies <code>empty</code> every time it produces an
     * object (or finishes). The consumer waits for it.
     */
    protected Semaphore empty;

    /**
     * Conversely, the consumer notifies <code>full</code> every time it reads
     * the next object. The producers wait for it, then one starts work.
     */
    protected Semaphore full;


    //~ Constructors ----------------------------------------------------------

    /** default constructor (one producer, no tracer) */
    public QueueIterator()
    {
        this(1, null);
    }


    /**
     * @param n number of producers
     * @param tracer trace to this Logger, or null.
     */
    public QueueIterator(int n, Logger tracer)
    {
        numProducers = n;
        this.tracer = (tracer == null)? null : new EigenbaseLogger(tracer);

        if (n == 0) {
            hasNext = false;            // done now
            waitingForProducer = false; // aren't any
            return;
        }
        empty = new Semaphore(0);       // no data yet
        full = new Semaphore(1);        // one empty data slot
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
        // full.release(), and that is sufficient.
        if (tracer != null) tracer.log(Level.FINE, "{0} producer waits", this);
        full.acquire(); // wait for consumer thread to use previous
        if (tracer != null) tracer.log(Level.FINE, "{0} producer proceeds", this);
        numProducers--;
        if (numProducers == 0 || throwable != null) {
            // shut down the iterator
            hasNext = false;
            next = null;
            this.throwable = throwable;
            if (tracer != null) tracer.log(Level.FINE, "{0} done; producer yields to consumer", this);
            empty.release(); // wake up consumer thread

        } else {
            // yield to other producers, let consumer sleep
            if (tracer != null) tracer.log(Level.FINE, "{0} producer yields to other producers", this);
            full.release();
        } 
    }

    // implement Iterator
    public synchronized boolean hasNext()
    {
        if (waitingForProducer) {
            if (tracer != null) tracer.log(Level.FINE, "{0} consumer waits", this);
            empty.acquire(); // wait for producer to produce one
            if (tracer != null) tracer.log(Level.FINE, "{0} consumer proceeds", this);
            waitingForProducer = false;
        }
        if (!hasNext) {
            checkError();
            onClose();
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
            if (tracer != null) tracer.log(Level.FINE, "{0} consumer waits", this);
            boolean isLocked = empty.tryAcquire(timeoutMillis);
            if (!isLocked) {
                // Throw an exception indicating timeout. It's not a 'real'
                // error, so we don't set the '_throwable' field. It's OK
                // to call this method again.
                if (tracer != null) tracer.log(Level.FINE, "{0} consumer timed out", this);
                throw new TimeoutException();
            }
            if (tracer != null) tracer.log(Level.FINE, "{0} consumer proceeds", this);
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
            if (tracer != null) tracer.log(Level.FINE, "{0} consumer waits", this);
            empty.acquire(); // wait for producer to produce one
            if (tracer != null) tracer.log(Level.FINE, "{0} consumer proceeds", this);
            waitingForProducer = false;
        }
        if (!hasNext) {
            checkError();

            // It is illegal to call next when there are no more objects.
            throw new NoSuchElementException();
        }
        Object o = next;
        waitingForProducer = true;
        if (tracer != null) tracer.log(Level.FINE, "{0} consumer wakes producer", this);
        full.release();
        if (tracer != null) tracer.log(Level.FINER, "{0} => {1}", this, o);
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
        if (tracer != null) tracer.log(Level.FINE, "{0} consumer waits", this);
            boolean isLocked = empty.tryAcquire(timeoutMillis);
            if (!isLocked) {
                // Throw an exception indicating timeout. It's not a 'real'
                // error, so we don't set the '_throwable' field. It's OK
                // to call this method again.
                if (tracer != null) tracer.log(Level.FINE, "{0} consumer times out", this);
                throw new TimeoutException();
            }
            if (tracer != null) tracer.log(Level.FINE, "{0} consumer proceeds", this);
            waitingForProducer = false;
        }
        if (!hasNext) {
            checkError();

            // It is illegal to call next when there are no more objects.
            throw new NoSuchElementException();
        }
        Object o = next;
        waitingForProducer = true;
        if (tracer != null) tracer.log(Level.FINE, "{0} consumer wakes producer", this);
        full.release();
        if (tracer != null) tracer.log(Level.FINER, "{0} => {1}", this, o);
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
        if (tracer != null) tracer.log(Level.FINER, "{0} put {1}", this, o);
        if (tracer != null) tracer.log(Level.FINE, "{0} producer waits", this);
        full.acquire(); // wait for consumer thread to use previous
        if (tracer != null) tracer.log(Level.FINE, "{0} producer proceeds", this);
        if (!hasNext) {
            // It is illegal to add a new object after done() has been called.
            throw new IllegalStateException();
        }
        next = o;
        if (tracer != null) tracer.log(Level.FINE, "{0} producer wakes consumer", this);
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

    /**
     * Called once the iterator returns false for hasNext().  Default
     * implementation does nothing, but subclasses can use this
     * for cleanup actions.
     */
    protected void onClose()
    {
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Thrown by {@link QueueIterator#hasNext(long)} and {@link QueueIterator#next(long)} to indicate that
     * operation timed out before rows were available.
     */
    public static class TimeoutException extends Exception
    {
    }
}


// End QueueIterator.java
