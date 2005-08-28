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

import org.eigenbase.runtime.RestartableIterator;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.logging.Logger;
import java.util.logging.Level;
import net.sf.farrago.trace.FarragoTrace;


/**
 * FennelAbstractIterator implements the {@link RestartableIterator} interface
 * by unmarshalling Fennel tuples from a buffer.
 *
 * <p>FennelAbstractIterator only deals with raw byte buffers; it is the
 * responsibility of the contained {@link FennelTupleReader} object to
 * unmarshal individual fields.
 *
 * <p>Neither does it actually populate the source buffer. This is the
 * responsibility of the {@link #populateBuffer()} method, which must be
 * implemented by the subclass. The subclass may optionally override the
 * {@link #releaseBuffer()} method to tell the producer that it is safe to
 * start producing more data.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FennelAbstractIterator implements RestartableIterator
{
    // TODO: don't borrow the neighbors' tracer
    private static final Logger tracer =
        FarragoTrace.getFarragoIteratorResultSetTracer();

    protected final FennelTupleReader tupleReader;
    protected ByteBuffer byteBuffer;
    protected byte [] bufferAsArray;
    private boolean endOfData;

    /**
     * Creates a new FennelIterator object.
     *
     * @param tupleReader FennelTupleReader to use to interpret Fennel data
     */
    public FennelAbstractIterator(FennelTupleReader tupleReader)
    {
        this.tupleReader = tupleReader;
        this.endOfData = false;
    }

    /**
     * a string that tersely describes the status of the iterator, for tracing.
     * @param prefix prepended to the results
     */
    protected String getStatus(String prefix)
    {
        StringBuffer buf = new StringBuffer(prefix);
        if (prefix.length() > 0) buf.append(" ");
        if (byteBuffer == null) {
            buf.append("(ByteBuffer:null)");
        } else {
            buf.append("(").append(byteBuffer).append(")");
        } 
        if (endOfData) buf.append(" EOD");
        return buf.toString();
    }


    // implement RestartableIterator
    public void restart()
    {
        tracer.fine(this.toString());
        this.endOfData = false;
    }

    // implement Iterator
    // Note that we hold the buffer whenever this returns true.
    public boolean hasNext()
    {
        if (endOfData) {
            return false;
        } else if (byteBuffer.hasRemaining()) {
            return true;
        }
        int cb = populateBuffer();
        if (cb == 0) {
            bufferAsArray = null;
            endOfData = true;
            return false;
        }
        byteBuffer.limit(cb);
        return true;
    }

    // implement Iterator
    public Object next()
    {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        // REVIEW:  is slice allocation worth it?
        ByteBuffer sliceBuffer = byteBuffer.slice();
        sliceBuffer.order(byteBuffer.order());
        Object obj =
            tupleReader.unmarshalTuple(byteBuffer, bufferAsArray, sliceBuffer);
        int newPosition = byteBuffer.position() + sliceBuffer.position();

        // eat final alignment padding
        while ((newPosition & 3) != 0) {
            ++newPosition;
        }
        byteBuffer.position(newPosition);
        // trace shows buffer state after this element, but before refill
        if (tracer.isLoggable(Level.FINER)) 
            tracer.finer(getStatus(this.toString())+" => " + obj);
        if (!byteBuffer.hasRemaining()) {
            releaseBuffer();
        }
        return obj;
    }

    // implement Iterator
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Populates the buffer with a new batch of data, and returns the size in bytes.
     * The buffer position is set to the start.
     * The call blocks til the buffer is filled.
     * A subclass can implement this to fill the buffer itself,
     * or it can work by allowing an outside object to fill the buffer.
     * In order to cooperate with an outside object, a subclass should implement
     * {@link acquireBuffer()} and {@link relaseBuffer()}.
     * @return The number of bytes now in the buffer. 0 means end of stream.
     */
    protected abstract int populateBuffer();


    /**
     * This method is called when the contents of the buffer have been
     * consumed. A subclass may use this method to tell the producer that it
     * can start producing. The default implementation does nothing.
     * The method should not block.
     */
    protected void releaseBuffer()
    {
    }
}

// End FennelAbstractIterator.java
