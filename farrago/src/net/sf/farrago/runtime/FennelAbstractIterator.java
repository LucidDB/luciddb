/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
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
package net.sf.farrago.runtime;

import org.eigenbase.runtime.RestartableIterator;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;


/**
 * FennelAbstractIterator implements the {@link RestartableIterator} interface
 * by unmarshalling Fennel tuples from a buffer.
 *
 * <p>FennelAbsstractIterator only deals with raw byte buffers; it is the
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
    protected final FennelTupleReader tupleReader;
    protected ByteBuffer byteBuffer;
    protected byte [] bufferAsArray;

    /**
     * Creates a new FennelIterator object.
     *
     * @param tupleReader FennelTupleReader to use to interpret Fennel data
     */
    public FennelAbstractIterator(FennelTupleReader tupleReader)
    {
        this.tupleReader = tupleReader;
    }

    // implement Iterator
    public boolean hasNext()
    {
        if (bufferAsArray == null) {
            return false;
        }
        if (byteBuffer.hasRemaining()) {
            return true;
        }
        byteBuffer.clear();
        int cb = populateBuffer();
        if (cb == 0) {
            bufferAsArray = null;
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
        if (byteBuffer.hasRemaining()) {
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
     * Populates the buffer with a new batch of data, and returns the number
     * of bytes read.
     *
     * @return The number of bytes read. 0 means end of stream.
     */
    protected abstract int populateBuffer();

    /**
     * This method is called when the contents of the buffer have been
     * consumed. A subclass may use this method to tell the producer that it
     * can start producing. The default implementation does nothing.
     */
    protected void releaseBuffer()
    {
    }
}

// End FennelAbstractIterator.java
