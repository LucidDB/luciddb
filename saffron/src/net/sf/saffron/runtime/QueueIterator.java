/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
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
package net.sf.saffron.runtime;

import java.util.Iterator;

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

    private Object next_;

    /**
     * The producer notifies <code>empty</code> every time it produces an
     * object (or finishes). The consumer waits for it.
     */
    private Semaphore empty;

    /**
     * Conversely, the consumer notifies <code>full</code> every time it reads
     * the next object. The producer waits for it, then starts work.
     */
    private Semaphore full;
    private Throwable throwable_;
    private boolean hasNext_;

    /** Protects the <code>avail_</code> semaphore. */
    private boolean waitingForProducer_;

    //~ Constructors ----------------------------------------------------------

    public QueueIterator()
    {
        empty = new Semaphore(0);
        full = new Semaphore(1);
        waitingForProducer_ = true;
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
        // consumed.leave(), and that is sufficient.
        full.enter(); // wait for consumer thread to use previous
        hasNext_ = false;
        next_ = null;
        throwable_ = throwable;
        empty.leave(); // wake up consumer thread
    }

    // implement Iterator
    public synchronized boolean hasNext()
    {
        if (waitingForProducer_) {
            empty.enter(); // wait for producer to produce one
            waitingForProducer_ = false;
        }
        if (!hasNext_) {
            checkError();
        }
        return hasNext_;
    }

    // implement Iterator
    public synchronized Object next()
    {
        if (waitingForProducer_) {
            empty.enter(); // wait for producer to produce one
            waitingForProducer_ = false;
        }
        if (!hasNext_) {
            checkError();
            throw new Error("no more");
        }
        Object o = next_;
        waitingForProducer_ = true;
        full.leave();
        return o;
    }

    /**
     * Producer calls <code>put</code> to add another object (which may be
     * null).
     */
    public void put(Object o)
    {
        // This method is NOT synchronized. If it were, it would deadlock with
        // the waiting consumer thread. The consumer thread has called
        // full.leave(), and that is sufficient.
        full.enter(); // wait for consumer thread to use previous
        hasNext_ = true;
        next_ = o;
        empty.leave(); // wake up consumer thread
    }

    // implement Iterator
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Throws an error if one has been set via {@link #done(Throwable)}.
     */
    private void checkError()
    {
        if (throwable_ == null) {
            ;
        } else if (throwable_ instanceof RuntimeException) {
            throw (RuntimeException) throwable_;
        } else if (throwable_ instanceof Error) {
            throw (Error) throwable_;
        } else {
            throw new Error("error: " + throwable_);
        }
    }
}

class Semaphore
{
    //~ Instance fields -------------------------------------------------------

    int count;

    //~ Constructors ----------------------------------------------------------

    Semaphore(int count)
    {
        this.count = count;
    }

    //~ Methods ---------------------------------------------------------------

    synchronized void enter()
    {
        while (count <= 0) {
            try {
                wait();
            } catch (InterruptedException e) {
            }
        }

        // we have control, decrement the count
        count--;
    }

    synchronized void leave()
    {
        count++;
        notify();
    }
}

// End QueueIterator.java
