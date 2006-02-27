/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2005-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
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

import org.eigenbase.runtime.RestartableIterator;
import org.eigenbase.util.ArrayQueue;
import net.sf.farrago.trace.FarragoTrace;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.NoSuchElementException;
import java.util.logging.Logger;
import java.util.logging.Level;


/**
 * FennelPipeIterator implements the {@link RestartableIterator}
 * interface, receiving data from a producer as {@link ByteBuffer}
 * objects, and unmarshalling them to a consumer.
 *
 * <p> A FennelPipeIterator has a C++ peer, a JavaSinkExecstream.
 * The peer sends marshalled data, wrapped as a ByteBuffer.
 * The reader has a current buffer from which it unmarshals rows on demand,
 * and a queue of buffers to read next.
 * The queue is synchronized; but it is left available to the writer between Iterator
 * calls (#next() and #hasNext()). Otherwise, if it were unavailable for a long time,
 * the peer XO would block, which is a severe problem for a single-thread XO scheduler.
 *
 * @author Julian Hyde
 * @version $Id$
 */
public class FennelPipeIterator extends FennelAbstractTupleIter 
    implements RestartableIterator
{
    private static final Logger tracer = FarragoTrace.getFennelPipeIteratorTracer();

    // byteBuffer is the current buffer, and belongs exclusively to the reader (this object)

    private ArrayQueue moreBuffers = null; // buffers from the writer, not yet read

    private Object next = null; // current row

    /** adds a buffer to the buffer queue. The writer peer calls this. */
    private void enqueue(ByteBuffer bb)
    {
        synchronized(moreBuffers) {
            moreBuffers.offer(bb);
            if (moreBuffers.size() == 1)    // was empty
                moreBuffers.notify();        // REVIEW mb: Use Semaphore instead?
        }
    }

    /** Gets the head buffer from the queue. If the queue is empty, blocks until a buffer arrives. */
    private ByteBuffer dequeue()
    {
        Object head = null;
        synchronized(moreBuffers) {
            head =  moreBuffers.poll();
            while (head == null) {
                try {
                    moreBuffers.wait();
                } catch (InterruptedException e) {
                }
                head = moreBuffers.poll();
            }
        }
        return (ByteBuffer) head;
    }


    /**
     * creates a new FennelPipeIterator object.
     *
     * @param tupleReader FennelTupleReader to use to interpret Fennel data
     */
    public FennelPipeIterator(FennelTupleReader tupleReader)
    {
        super(tupleReader);

        // start with an empty buffer queue
        moreBuffers = new ArrayQueue(2);

        // Create an empty byteBuffer which will cause us to fetch new rows the
        // first time our consumer tries to fetch. TODO: Add a new state 'have
        // not yet checked whether we have more data'.
        bufferAsArray = new byte[0];
        byteBuffer = ByteBuffer.wrap(bufferAsArray);
        byteBuffer.clear();
        byteBuffer.limit(0);
    }

    // override FennelAbstractTupleIter to trace
    public void restart()
    {
        tracer.fine(this.toString());
        super.restart();
    }

    // implement Iterator
    public boolean hasNext()
    {
        if (next != null) {
            traceHasNext(true);
            return true;
        }

        Object fetched = fetchNext();
        if (fetched == NoDataReason.END_OF_DATA) {
            traceHasNext(false);
            return false;
        }

        // Old-style iterator convention doesn't handle anything but
        // END_OF_DATA
        assert(!(fetched instanceof NoDataReason));

        next = fetched;

        traceHasNext(true);
        return true;
    }

    private void traceHasNext(boolean hasNextResult)
    {
        if (!tracer.isLoggable(Level.FINE)) {
            return;
        }

        if (!hasNextResult || tracer.isLoggable(Level.FINER)) {
            String msg = getStatus(this.toString()) + " => " + hasNextResult;

            if (!hasNextResult) {
                tracer.fine(msg);
            } else {
                tracer.finer(msg);
            }
        }
    }
            
    // implement Iterator
    public Object next()
    {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        Object result = next;
        next = null;
        return result;
    }


    // override FennelAbstractTupleIter
    protected void traceNext(Object val)
    {
        if (tracer.isLoggable(Level.FINER)) 
            tracer.finer(getStatus(this.toString())+" => " + val);
    }

    // implement Iterator
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    // implement TupleIter
    public void closeAllocation()
    {
        // REVIEW: SWZ: 2/23/2006: Deallocate byteBuffer here?
    }

    protected int populateBuffer()
    {
        tracer.fine(this + " reader waits");
        byteBuffer = dequeue();         // get next buffer; may block
        int n = byteBuffer.limit(); 
        if (n > 0)
            bufferAsArray = byteBuffer.array();
        if (tracer.isLoggable(Level.FINE)) {
            tracer.fine(getStatus(this.toString()) + " => " + n);
        }
        return n;
    }

    /**
     * Gets a direct ByteBuffer suitable for #write. The C++ caller may pin the
     * backing array (JNI GetByteArrayElements), copy data, and then pass back the
     * ByteBuffer by calling #write.
     */
    public ByteBuffer getByteBuffer(int size)
    {
        // REVIEW mb 8/22/05 Why not call ByteBuffer.allocateDirect(size) ?
        // Why can't C++ peer call it directly?
        // Or else recycle buffer with a free list.
        byte b[] = new byte[size];
        ByteBuffer bb = ByteBuffer.wrap(b);
        bb.order(ByteOrder.nativeOrder());
        bb.clear();
        return bb;
    }


    /**
     * Writes the contents of a byte buffer into this iterator.
     * To avoid an extra copy here, the buffer should be direct and expose
     * its backing array (ie <code>byteBufer.hasArray() == true</code>).
     * (Unfortunately the result of JNI NewDirectByteBuffer() need not have
     * a backing array).
     
     * <p>This method is called by the producer, typically from JNI.
     * <p>The limit of the byte buffer is ignored; the
     * <code>bblen</code> parameter is used instead.

     * @param bb     A ByteBuffer containing the new data
     * @param bblen  size of {@code bb} in bytes; 0 means end of data.
     */
    public void write(ByteBuffer bb, int bblen) throws Throwable
    {
        try {
            bb.limit(bblen);
            bb.position(0);
            if (!bb.hasArray())  {
                // argh, have to wrap a copy of the new data
                tracer.fine("copies buffer");
                byte b[] = new byte[bblen];
                bb.rewind();
                bb.get(b);
                bb = ByteBuffer.wrap(b);
            }
                
            tracer.fine(this + " writer waits (buf: " + bb + " bytes: " + bblen);
            enqueue(bb);
            tracer.fine(this + " writer proceeds");
            
        } catch (Throwable e) {
            tracer.throwing(null, null, e);
            throw e;
        }
    }
}

// End FennelPipeIterator.java
