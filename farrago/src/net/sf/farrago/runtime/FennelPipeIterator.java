/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Copyright (C) 2005-2005 DisruptiveTech
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
*/
package net.sf.farrago.runtime;

import org.eigenbase.runtime.Interlock;

import java.nio.ByteBuffer;

/**
 * FennelPipeIterator implements the
 * {@link org.eigenbase.runtime.RestartableIterator} interface, receiving
 * data from a producer as {@link ByteBuffer} objects, and unmarshalling them
 * to a consumer.
 *
 * <p>Data is not copied between the producer and consumer. To this end,
 * the class is synchronized so that only one of the producer and consumer
 * can be active at a time. The synchronization automatically ensures that the
 * calls occur in the following order: <ul>
 *
 * <li>The producer calls {@link #write(java.nio.ByteBuffer, int)},
 * <li>The consumer calls {@link #populateBuffer()} via the {@link #hasNext}
 *     method of the base class;
 * <li>The consumer calls {@link #releaseBuffer()} via the {@link #next}
 *     method of the base class, when the last record in the buffer has been
 *     read.
 * <li>Repeat.</ul>
 *
 * @author Julian Hyde
 * @version $Id$
 */
public class FennelPipeIterator extends FennelAbstractIterator
{
    /**
     * Interlock manages the synchronization between producer and consumer.
     */
    private final Interlock interlock = new Interlock();

    /**
     * Creates a new FennelPipeIterator object.
     *
     * @param tupleReader FennelTupleReader to use to interpret Fennel data
     */
    public FennelPipeIterator(FennelTupleReader tupleReader)
    {
        super(tupleReader);
    }

    public void restart()
    {
        // nothing to do
    }

    protected int populateBuffer()
    {
        // Wait until the producer has finished populating the buffer.
        interlock.beginReading();
        return byteBuffer.limit();
    }

    protected void releaseBuffer()
    {
        // signal that the producer can start writing to the buffer
        interlock.endReading();
    }

    /**
     * Writes the contents of a direct byte buffer into this iterator.
     *
     * <p>This method is called by the producer, typically from JNI.
     * When it is complete, the {@link #populateBuffer()} method will be able
     * to proceed.
     *
     * <p>Note: the {@link ByteBuffer} must be direct.
     * Also, the limit of the byte buffer is ignored; the
     * <code>byteCount</code> parameter is used instead.
     */
    public void write(ByteBuffer byteBuffer, int byteCount)
    {
        // Wait until the consumer has finished reading from the buffer.
        interlock.beginWriting();
        byteBuffer.limit(byteCount);
        this.byteBuffer = byteBuffer;
        this.bufferAsArray = byteBuffer.array();
        // Signal that the consumer can start reading. This will allow the
        // populateBuffer method to complete.
        interlock.endWriting();
    }
}

// End FennelPipeIterator.java
