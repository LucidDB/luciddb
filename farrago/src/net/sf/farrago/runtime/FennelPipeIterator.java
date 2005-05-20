/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package net.sf.farrago.runtime;

import org.eigenbase.runtime.Interlock;

import java.nio.ByteBuffer;
import java.util.logging.Logger;

import net.sf.farrago.trace.FarragoTrace;

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
    // TODO: don't borrow the neighbors' tracer
    private static final Logger tracer =
        FarragoTrace.getFarragoIteratorResultSetTracer();

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

        // Create an empty byteBuffer which will cause us to fetch new rows the
        // first time our consumer tries to fetch. TODO: Add a new state 'have
        // not yet checked whether we have more data'.
        bufferAsArray = new byte[0];
        byteBuffer = ByteBuffer.wrap(bufferAsArray);
        byteBuffer.clear();
        byteBuffer.limit(0);
    }

    protected int populateBuffer()
    {
        // Wait until the producer has finished populating the buffer.
//        System.out.println("populateBuffer: 1");
        interlock.beginReading();
//        System.out.println("populateBuffer: 2");
        return byteBuffer.limit();
    }

    protected void releaseBuffer()
    {
        // signal that the producer can start writing to the buffer
//        System.out.println("endReading: 1");
        interlock.endReading();
//        System.out.println("endReading: 2");
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
    public void write(ByteBuffer byteBuffer, int byteCount) throws Throwable
    {
        try {
            System.out.println("write: byteCount=" + byteCount);
            // Wait until the consumer has finished reading from the buffer.
            interlock.beginWriting();
//            System.out.println("write: 2");
            byteBuffer.limit(byteCount);
//            System.out.println("write: 3");
            this.byteBuffer = byteBuffer;
            this.bufferAsArray = byteBuffer.array();
            // Signal that the consumer can start reading. This will allow the
            // populateBuffer method to complete.
//            System.out.println("write: 4");
            interlock.endWriting();
//            System.out.println("write: 5");
        } catch (Throwable e) {
            tracer.throwing(null, null, e);
            throw e;
        }
    }
}

// End FennelPipeIterator.java
