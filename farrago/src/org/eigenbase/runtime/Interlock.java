/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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

import java.nio.ByteBuffer;


/**
 * A synchronization primitive which allows a producer and a consumer to use
 * the same resources without treading on each other's feet.
 *
 * <p>At most one of the producer and consumer has access at a time. The
 * synchronization ensures that the call sequence is as follows:<ul>
 *
 * <li{@link #beginWriting()} (called by producer)
 * <li{@link #endWriting()} (called by producer)
 * <li{@link #beginReading()} (called by consumer)
 * <li{@link #endReading()} (called by consumer)
 * </ul>
 *
 * <p>{@link ExclusivePipe} is a simple extension to this class containing a
 * {@link ByteBuffer} as the shared resource.
 *
 * @author jhyde
 * @version $Id$
 */
public class Interlock
{
    /**
     * The producer notifies <code>empty</code> every time it finishes writing.
     * The consumer waits for it.
     */
    private final Semaphore empty = new Semaphore(0);

    /**
     * The consumer notifies <code>full</code> every time it finishes reading.
     * The producer waits for it, then starts work.
     */
    private final Semaphore full = new Semaphore(1);

    public Interlock()
    {
    }

    /**
     * Acquires the buffer, in preparation for writing.
     *
     * <p>The producer should call this method.
     * After this call completes, the consumer's call to
     * {@link #beginReading()} will block until the producer has called
     * {@link #endWriting()}.
     */
    public void beginWriting()
    {
        full.acquire(); // wait for consumer thread to use previous
    }

    /**
     * Releases the buffer after writing.
     *
     * <p>The producer should call this method.
     * After this call completes, the producers's call to
     * {@link #beginWriting()} will block until the consumer has called
     * {@link #beginReading()} followed by {@link #endReading()}.
     */
    public void endWriting()
    {
        empty.release(); // wake up consumer
    }

    /**
     * Acquires the buffer, in preparation for reading.
     *
     * <p>After this call completes, the producer's call to
     * {@link #beginWriting()} will block until the consumer has called
     * {@link #endReading()}.
     */
    public void beginReading()
    {
        empty.acquire(); // wait for producer to produce one
    }

    /**
     * Releases the buffer after reading its contents.
     *
     * <p>The consumer should call this method.
     * After this call completes, the consumer's call to
     * {@link #beginReading()} will block until the producer has called
     * {@link #beginWriting()} followed by {@link #endWriting()}.
     */
    public void endReading()
    {
        full.release(); // wake up producer
}
}

// End Interlock.java
