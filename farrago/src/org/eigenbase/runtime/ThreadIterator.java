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

import java.util.Iterator;
import java.util.concurrent.*;

import org.eigenbase.util.Util;

/**
 * <code>ThreadIterator</code> converts 'push' code to 'pull'.  You implement
 * {@link #doWork} to call {@link #put} with each row, and this class invokes
 * it in a separate thread. Then the results come out via the familiar {@link
 * Iterator} interface. For example,
 * <blockquote>
 * <pre>class ArrayIterator extends ThreadIterator {
 *   Object[] a;
 *   ArrayIterator(Object[] a) {
 *     this.a = a;
 *     start();
 *   }
 *   protected void doWork() {
 *     for (int i = 0; i < a.length; i++) {
 *       put(a[i]);
 *     }
 *   }
 * }</pre>
 * </blockquote>
 * Or, more typically, using an anonymous class:
 * <blockquote>
 * <pre>Iterator i = new ThreadIterator() {
 *   int limit;
 *   public ThreadIterator start(int limit) {
 *     this.limit = limit;
 *     return super.start();
 *   }
 *   protected void doWork() {
 *     for (int i = 0; i < limit; i++) {
 *       put(new Integer(i));
 *     }
 *   }
 * }.start(100);
 * while (i.hasNext()) {
 *   <em>etc.</em>
 * }</pre>
 * </blockquote>
 */
public abstract class ThreadIterator extends QueueIterator implements Iterator,
    Runnable,
    Iterable
{
    private Thread thread;
    
    //~ Constructors ----------------------------------------------------------

    public ThreadIterator()
    {
    }

    public ThreadIterator(BlockingQueue queue)
    {
        super(1, null, queue);
    }

    //~ Methods ---------------------------------------------------------------

    // implement Iterable
    public Iterator iterator()
    {
        return start();
    }

    // implement Runnable
    public void run()
    {
        boolean calledDone = false;
        try {
            doWork();
        } catch (Throwable e) {
            done(e);
            calledDone = true;
        } finally {
            if (!calledDone) {
                done(null);
            }
        }
    }

    /**
     * The implementation should call {@link #put} with each row.
     */
    protected abstract void doWork();

    protected ThreadIterator start()
    {
        assert(thread == null);
        thread = new Thread(this);
        // Make the thread a daemon so that we don't have to worry
        // about cleaning it up.  This is important since we can't
        // be guaranteed that onClose will get called (someone
        // may create an iterator and then forget about it), so
        // requiring a join() call would be a bad idea.  Of course,
        // if someone does forget about it, and the producer
        // thread gets stuck on a full queue, it will never exit,
        // and will become a resource leak.
        thread.setDaemon(true);
        thread.start();
        return this;
    }

    // implement QueueIterator
    protected void onEndOfQueue()
    {
        thread = null;
    }
}

// End ThreadIterator.java
