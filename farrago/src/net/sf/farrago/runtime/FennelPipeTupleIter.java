/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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

import java.nio.*;

import java.util.concurrent.*;
import java.util.logging.*;

import net.sf.farrago.trace.*;

import org.eigenbase.util.*;


/**
 * FennelPipeTupleIter implements the {@link org.eigenbase.runtime.TupleIter}
 * interface, receiving data from a producer as {@link ByteBuffer} objects, and
 * unmarshalling them to a consumer.
 *
 * <p>A FennelPipeTupleIter has a C++ peer, a JavaSinkExecstream. The peer sends
 * marshalled data, wrapped as a ByteBuffer. The reader has a current buffer
 * from which it unmarshals rows on demand, and a queue of buffers to read next.
 * The queue is synchronized; but it is left available to the writer between
 * TupleIter calls (#fetchNext()). Otherwise, if it were unavailable for a long
 * time, the peer XO would block, which is a severe problem for a single-thread
 * XO scheduler.
 *
 * @author Julian Hyde, Stephan Zuercher
 * @version $Id$
 */
public class FennelPipeTupleIter
    extends FennelAbstractTupleIter
{
    //~ Static fields/initializers ---------------------------------------------

    private static final int QUEUE_LENGTH = 16;

    protected static final Logger tracer =
        FarragoTrace.getFennelPipeIteratorTracer();

    private static final int BUFFER_SIZE = 65536;

    //~ Instance fields --------------------------------------------------------

    // byteBuffer is the current buffer, and belongs exclusively to the reader
    // (this object)

    /**
     * free Buffers
     */
    private ArrayBlockingQueue<ByteBuffer> freeBuffers;
    private ArrayBlockingQueue<ByteBuffer> rowBuffers;

    private ByteBuffer dummyBuffer;

    //~ Constructors -----------------------------------------------------------

    /**
     * creates a new FennelPipeTupleIter object.
     *
     * @param tupleReader FennelTupleReader to use to interpret Fennel data
     */
    public FennelPipeTupleIter(FennelTupleReader tupleReader)
    {
        super(tupleReader);
        freeBuffers = new ArrayBlockingQueue<ByteBuffer>(QUEUE_LENGTH);
        for (int i = 0; i < QUEUE_LENGTH; i++) {
            ByteBuffer bb = ByteBuffer.wrap(new byte[BUFFER_SIZE]);
            bb.order(ByteOrder.nativeOrder());
            bb.clear();
            freeBuffers.offer(bb);
        }

        // Allocate one more slot than number of buffers to fit dummyBuffer.
        rowBuffers = new ArrayBlockingQueue<ByteBuffer>(QUEUE_LENGTH + 1);
        // An empty buffer which will cause us to fetch new rows the first time
        // our consumer tries to fetch. TODO: Add a new state 'have not yet
        // checked whether we have more data'.
        dummyBuffer = ByteBuffer.wrap(new byte[0]);
        dummyBuffer.order(ByteOrder.nativeOrder());
        dummyBuffer.clear();
        dummyBuffer.limit(0);
        byteBuffer = dummyBuffer;
        bufferAsArray = byteBuffer.array();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * adds a buffer to the buffer queue. The writer peer calls this.
     */
    private void enqueue(ByteBuffer bb)
    {
        boolean success = rowBuffers.offer(bb);
        assert (success);
    }

    /**
     * Gets the head buffer from the queue. If the queue is empty, blocks until
     * a buffer arrives.
     */
    private ByteBuffer dequeue()
    {
        ByteBuffer head = null;
        while (head == null) {
            try {
                head = rowBuffers.take();
            } catch (InterruptedException ie) {
                assert (false);
            }
        }
        return head;
    }

    // override FennelAbstractTupleIter to trace
    public void restart()
    {
        tracer.fine(this.toString());
        super.restart();
    }

    // override FennelAbstractTupleIter
    protected void traceNext(Object val)
    {
        if (tracer.isLoggable(Level.FINEST)) {
            tracer.finest(getStatus(this.toString()) + " => " + val);
        }
    }

    // implement TupleIter
    public void closeAllocation()
    {
        tracer.fine("close");
        enqueue(dummyBuffer);           // send EOQ in case reader is blocked
    }

    protected int populateBuffer()
    {
        if (tracer.isLoggable(Level.FINER)) {
            tracer.finer(this + " reader waits");
        }
        ByteBuffer prevBuffer = byteBuffer;
        byteBuffer = dequeue(); // get next buffer; may block
        if (prevBuffer != dummyBuffer) {
            prevBuffer.order(ByteOrder.nativeOrder());
            prevBuffer.clear();
            boolean success = freeBuffers.offer(prevBuffer);
            assert (success);
        }

        int n = byteBuffer.limit();
        if (n > 0) {
            bufferAsArray = byteBuffer.array();
        }
        if (tracer.isLoggable(Level.FINE)) {
            tracer.fine(getStatus(this.toString()) + " => " + n);
        }
        return n;
    }

    /**
     * Gets a direct ByteBuffer suitable for #write. The C++ caller may pin the
     * backing array (JNI GetByteArrayElements), copy data, and then pass back
     * the ByteBuffer by calling #write.
     */
    public ByteBuffer getByteBuffer(int size)
    {
        assert (size <= BUFFER_SIZE);

        if (size == 0) {
            return dummyBuffer;
        }

        ByteBuffer head = null;
        head = freeBuffers.poll();
        if (head != null) {
            // Important to set correct limit here. Otherwise memcpy() in
            // upstream C++ XO caused SEGV
            head.limit(size);
        }
        return head;
    }

    /**
     * Writes the contents of a byte buffer into this iterator. To avoid an
     * extra copy here, the buffer should be direct and expose its backing array
     * (ie <code>byteBufer.hasArray() == true</code>). (Unfortunately the result
     * of JNI NewDirectByteBuffer() need not have a backing array).
     *
     * <p>This method is called by the producer, typically from JNI.
     *
     * <p>The limit of the byte buffer is ignored; the <code>bblen</code>
     * parameter is used instead.
     *
     * @param bb A ByteBuffer containing the new data
     * @param bblen size of {@code bb} in bytes; 0 means end of data.
     */
    public void write(ByteBuffer bb, int bblen)
        throws Throwable
    {
        try {
            bb.limit(bblen);
            bb.position(0);
            assert (bb.hasArray());
            if (tracer.isLoggable(Level.FINER)) {
                tracer.finer(this + " writer waits");
            }
            enqueue(bb);
            if (tracer.isLoggable(Level.FINE)) {
                tracer.fine(
                    this + " writer put (buf: " + bb + " bytes: " + bblen);
            }
        } catch (Throwable e) {
            tracer.throwing(null, null, e);
            throw e;
        }
        return;
    }
}

// End FennelPipeTupleIter.java
