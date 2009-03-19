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


/**
 * A counting semaphore. Conceptually, a semaphore maintains a set of permits.
 * Each {@link #acquire()} blocks if necessary until a permit is available, and
 * then takes it. Each {@link #release()} adds a permit, potentially releasing a
 * blocking acquirer. However, no actual permit objects are used; the Semaphore
 * just keeps a count of the number available and acts accordingly.
 *
 * <p>Semaphores are often used to restrict the number of threads than can
 * access some (physical or logical) resource.
 *
 * <p>Note that JDK 1.5 contains <a
 * href="http://java.sun.com/j2se/1.5.0/docs/api/java/util/concurrent/Semaphore.html">
 * a Semaphore class</a>. We should obsolete this class when we upgrade.
 *
 * @author jhyde
 * @version $Id$
 */
public class Semaphore
{
    //~ Static fields/initializers ---------------------------------------------

    private static final boolean verbose = false;

    //~ Instance fields --------------------------------------------------------

    private int count;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a Semaphore with the given number of permits.
     */
    public Semaphore(int count)
    {
        this.count = count;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Acquires a permit from this semaphore, blocking until one is available.
     */
    public synchronized void acquire()
    {
        // REVIEW (jhyde, 2004/7/23): the JDK 1.5 Semaphore class throws
        //   InterruptedException; maybe we should too.
        while (count <= 0) {
            try {
                wait();
            } catch (InterruptedException e) {
            }
        }

        // we have control, decrement the count
        count--;
    }

    /**
     * Acquires a permit from this semaphore, if one becomes available within
     * the given waiting time.
     *
     * <p>If timeoutMillisec is less than or equal to zero, does not wait at
     * all.
     */
    public synchronized boolean tryAcquire(long timeoutMillisec)
    {
        long enterTime = System.currentTimeMillis();
        long endTime = enterTime + timeoutMillisec;
        long currentTime = enterTime;
        if (verbose) {
            System.out.println(
                "tryAcquire: enter=" + (enterTime % 100000)
                + ", timeout=" + timeoutMillisec + ", count=" + count
                + ", this=" + this + ", date=" + new Date());
        }

        while ((count <= 0) && (currentTime < endTime)) {
            // REVIEW (jhyde, 2004/7/23): the equivalent method in the JDK 1.5
            //   Semaphore class throws InterruptedException; maybe we should
            //   too.
            try {
                // Note that wait(0) means no timeout (wait forever), whereas
                // tryAcquire(0) means don't wait
                assert (endTime - currentTime) > 0 : "wait(0) means no timeout!";
                wait(endTime - currentTime);
            } catch (InterruptedException e) {
            }
            currentTime = System.currentTimeMillis();
        }

        if (verbose) {
            System.out.println(
                "enter=" + (enterTime % 100000) + ", now="
                + (currentTime % 100000) + ", end=" + (endTime % 100000)
                + ", timeout=" + timeoutMillisec + ", remain="
                + (endTime - currentTime) + ", count=" + count + ", this="
                + this + ", date=" + new Date());
        }

        // we may have either been timed out or notified
        // let's check which is the case
        if (count <= 0) {
            if (verbose) {
                System.out.println("false");
            }

            // lock still not released - we were timed out!
            return false;
        } else {
            if (verbose) {
                System.out.println("true");
            }

            // we have control, decrement the count
            count--;
            return true;
        }
    }

    /**
     * Releases a permit, returning it to the semaphore.
     */
    public synchronized void release()
    {
        count++;
        notify();
    }
}

// End Semaphore.java
