/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
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
package net.sf.farrago.runtime;

import net.sf.farrago.trace.FarragoTrace;
import org.eigenbase.util.*;

import java.nio.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * JavaPushTupleStream is the peer of a Fennel C++ JavaPushSource XO.
 * (compare {@link JavaTupleStream})
 *
 * It obtains rows from a Java Iterator, converts them to fennel tuples, and
 * marshals them into buffers shared with the peer.
 *
 * To avoid blocking the XO, it runs in parallel to it. Since the delays between
 * input rows are unpredictable, it runs a separate thread that fetches and
 * converts rows. The C++ XO can be scheduled independently: each time the XO
 * runs, it asks the java side for a buffer of data; but if a buffer is not
 * available, it returns immediately instead of waiting.
 *
 * @author Marc Berkowitz
 * @version $Id$
 */
public class JavaPushTupleStream implements JavaTupleStream
{
    private static final Logger tracer = FarragoTrace.getJavaPushTupleStreamTracer();

    private Iterator iter;              // input source
    private FennelTupleWriter tupleWriter; // converts input
    protected RowReaderThread inputThread;

    // buffer pool, shared by one writer (this object) and one reader (the JavaPushSourceExecStream)
    protected BufferPool pool;
    
    /**
     * Constructs a new JavaPushTupleStream.
     * @param tupleWriter the FennelTupleWriter to use for marshalling tuples
     * @param iter Iterator producing objects
     */
    public JavaPushTupleStream(FennelTupleWriter tupleWriter, Iterator iter)
    {
        this.tupleWriter = tupleWriter;
        this.iter = iter;
        pool = new BufferPool(this);
    }


    /** 
     * Called from native code to read from the stream. Returns the next buffer of
     * data, or null if no data is ready. Does not block.
     */
    public ByteBuffer getBuffer()
    {
        return pool.getDataBuffer(true);
    }

    /**
     * Called by the reader to return a buffer obtained by {@link #getBuffer()}
     * after it has been consumed.
     */
    public void freeBuffer(ByteBuffer buf)
    {
        pool.freeBuffer(buf);
    }

    /**
     * Called from native code to start this stream.
     * @param buffers Direct Byte Buffers allocated by the native side.
     */
    public void open(ByteBuffer[] buffers)
    {
        pool.traceBuffers(Level.FINE, "open stream with buffers: ", buffers);
        pool.open(buffers);
        inputThread = new RowReaderThread();
        // start marshalling input data
        tracer.fine(this+"starting thread");
        inputThread.start();
    }


    /**
     * Called from native code to close this stream.
     */
    public void close()
    {
        tracer.fine(toString());
        // REVIEW is this right?
        inputThread.stop(true);
        pool.close();
    }


    // implement JavaTupleStream
    public void restart()
    {
        tracer.fine(toString());
        // REVIEW is this right?
        Util.restartIterator(iter);
    }


    //~ Inner Class

    /**
     * A BufferPool manages a set of ByteBuffers shared by two clients, a single
     * reader and a single writer. As used here, the writer is the
     * JavaPushTupleStream itself, and the reader is its peer
     * JavaPushSourceExecStream.
     *
     * <p>The pool consists of a free list of blank buffers; a queue of available,
     * written buffers; possibly a buffer currently being written; and possibly
     * a buffer currently being read. Initially all buffers are on the free
     * list.
     * 
     * <p>The lifecycle of a buffer is: <nl>
     * <li>on free list</li>
     * <li>obtained by writer, and filled with data</li>
     * <li>on available queue</li>
     * <li>obtained by reader, and consumed </li>
     * <li>returned to free list, and so on til the pool is closed</li>
     * </nl>
     *
     * <p>The lifecycle of the pool is: <nl>
     * <li>its owner (the JavaPushTupleStream) opens it, providing it a fixed set of buffers.</li>
     * <li>buffers are used as above</li>
     * <li>the owner closes it down, which frees the buffers</li>
     * </nl>
     */
    protected static class BufferPool {
        final private JavaPushTupleStream stream;

        private LinkedList blankBufs = new LinkedList(); // a free list
        private LinkedList writtenBufs = new LinkedList(); // a queue
        private int nBlank = 0;          // length of free list
        private int nWritten = 0;         // length of queue

        public BufferPool(JavaPushTupleStream stream)
        {
            this.stream = stream;
        }

        /** owner initializes the pool */
        public void open(ByteBuffer[] bufs)
        {
            blankBufs.addAll(Arrays.asList(bufs));
            nBlank = blankBufs.size();
        }

        /** owner closes down the pool */
        public void close()
        {
            blankBufs.clear();
            writtenBufs.clear();
            nBlank = nWritten = 0;
        }

        private StringBuffer traceBuffer(StringBuffer sb, ByteBuffer buf)
        {
            if (buf == null)
                sb.append("null");
            else {
                sb.append("@").append(Integer.toHexString(buf.hashCode()));
                sb.append(": ").append(buf);
            }
            return sb;
        }

        public void traceBuffer(Level level, String msg, ByteBuffer buf)
        {
            if (!tracer.isLoggable(level))
                return;
            StringBuffer sb = new StringBuffer();
            sb.append(stream);
            if (msg != null)
                sb.append(" ").append(msg).append(" ");
            tracer.log(level, traceBuffer(sb, buf).toString());
        }

        public void traceBuffers(Level level, String msg, ByteBuffer[] bufs)
        {
            if (!tracer.isLoggable(level))
                return;
            StringBuffer sb = new StringBuffer();
            sb.append(stream);
            if (msg != null)
                sb.append(" ").append(msg).append(" ");
            for (int i = 0; i < bufs.length; i++) {
                if (i > 0) sb.append(", ");
                traceBuffer(sb, bufs[i]);
            }
            tracer.log(level, sb.toString());
        }


        /** reader returns a buffer to the freelist */
        public synchronized void freeBuffer(ByteBuffer buf)
        {
            traceBuffer(Level.FINE, "reader frees buffer", buf);
            blankBufs.addFirst(buf);
            nBlank++;
            notifyAll();
        }

        /** adds a newly written buffer to the queue
         * @param buf the buffer
         */
        public synchronized void addBuffer(ByteBuffer buf)
        {
            traceBuffer(Level.FINE, "writer adds buffer", buf);
            buf.flip();
            writtenBufs.addLast(buf);
            nWritten++;
            notifyAll();
        }

        /** 
         * gets a blank buffer for writing.
         * @param immediate if none available, true means return null, false means wait and return a buffer.
         * @return a writable buffer, or possibly null (in immediate mode or if interrupted)
         */
        public synchronized ByteBuffer getBlankBuffer(boolean immediate)
        {
            if (immediate && (nBlank == 0)) {
                traceBuffer(Level.FINE, "writer gets blank buffer", null);
                return null;
            }

            try {
                while (nBlank == 0) {
                    tracer.fine(stream + "writer waits");
                    wait();
                }
            } catch (InterruptedException e) {
                tracer.fine(stream + "writer wait interrupted => null");
                return null;
            }
            tracer.finer(stream + "writer wait end");

            ByteBuffer buf= (ByteBuffer) blankBufs.removeFirst();
            nBlank--;
            buf.order(ByteOrder.nativeOrder());
            buf.clear();
            traceBuffer(Level.FINE, "writer gets blank buffer", buf);
            return buf;
        }

        /** 
         * gets the next buffer to read.
         * @param immediate if none available, true means return null, false means wait and return a buffer.
         * @return a buffer to read, or possibly null (in immediate mode)
         */
        public synchronized ByteBuffer getDataBuffer(boolean immediate)
        {
            if (immediate && (nWritten == 0)) {
                traceBuffer(Level.FINE, "reader gets buffer", null);
                return null;
            }

            try {
                while (nWritten == 0) {
                    tracer.fine(stream + "reader waits");
                    wait();
                }
            } catch (InterruptedException e) {
                return null;
            }
            tracer.fine(stream + "reader wait ends");

            nWritten--;
            ByteBuffer buf = (ByteBuffer) writtenBufs.removeFirst();
            traceBuffer(Level.FINE, "reader gets buffer", buf);
            return buf;
        }
    };


    // When the stream is opened, it starts a thread that reads all input rows
    // and marshals them into buffers available to the reader (the C++ peer).
    protected class RowReaderThread extends Thread {
        private boolean stopped;
        
        public RowReaderThread() {
            stopped = false;
        }

        // stop the thread, and force an EOS on its output
        // @param block wait for the thread to stop before returning
        public void stop(boolean block) {
            if (stopped)
                return;
            tracer.fine(this + "stopping RowReaderThread");
            stopped = true;
            if (block) {
                try {
                    join();
                } catch (InterruptedException e) {
                }
            }
            tracer.fine(this + "stopping RowReaderThread - done");
        }

        // Read all the input data. OK to block, since in own thread.
        public void run() 
        {
            // get the 1st row
            Object next = iter.hasNext()? iter.next() : null;
            boolean eos = (next == null) || stopped;
            while (!eos) {
                ByteBuffer buf = pool.getBlankBuffer(false);
                if (buf == null) continue; // interrupted
                while (next != null) {
                    if (tracer.isLoggable(Level.FINER))
                        tracer.finer(this + " read row " + next);
                    if (!tupleWriter.marshalTuple(buf, next))
                        break;              // buffer full
                    if (stopped) break;
                    next = iter.hasNext()? iter.next() : null;
                }
                eos = (next == null) || stopped;
                if (eos) tracer.log(Level.FINE, "{0} read EOS", this);
                pool.addBuffer(buf);
            }

            // send an empty buffer to indicate EOS
            if (eos) {
                tracer.finer(this + "reader sending EOS");
                ByteBuffer buf = pool.getBlankBuffer(false);                
                pool.addBuffer(buf);
            }

            tracer.fine(this + "RowReaderThread stopped");
            stopped = true;
        }
    };
}
// End JavaTupleStream.java
