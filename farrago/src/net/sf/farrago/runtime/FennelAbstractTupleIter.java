/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2005-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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

import org.eigenbase.runtime.*;


/**
 * FennelAbstractTupleIter implements the {@link TupleIter} interface by
 * unmarshalling Fennel tuples from a buffer.
 *
 * <p>FennelAbstractTupleIter only deals with raw byte buffers; it is the
 * responsibility of the contained {@link FennelTupleReader} object to unmarshal
 * individual fields.
 *
 * <p>Neither does it actually populate the source buffer. This is the
 * responsibility of the {@link #populateBuffer()} method, which must be
 * implemented by the subclass. The subclass may optionally override the {@link
 * #requestData()} method to tell the producer that it is safe to start
 * producing more data.
 *
 * @author John V. Sichi, Stephan Zuercher
 * @version $Id$
 */
public abstract class FennelAbstractTupleIter
    implements TupleIter
{

    //~ Instance fields --------------------------------------------------------

    protected final FennelTupleReader tupleReader;
    protected ByteBuffer byteBuffer;
    protected byte [] bufferAsArray;
    private boolean endOfData;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FennelAbstractTupleIter object.
     *
     * @param tupleReader FennelTupleReader to use to interpret Fennel data
     */
    public FennelAbstractTupleIter(FennelTupleReader tupleReader)
    {
        this.tupleReader = tupleReader;
        this.endOfData = false;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * For subclasses that trace, returns a terse description of the status.
     *
     * @param prefix prepended to the results
     */
    protected String getStatus(String prefix)
    {
        StringBuffer sb = new StringBuffer(prefix);
        if (prefix.length() > 0) {
            sb.append(" ");
        }
        if (byteBuffer == null) {
            sb.append("buf: null");
        } else {
            sb.append("buf: ").append(
                Integer.toHexString(byteBuffer.hashCode()));
            sb.append(" (").append(byteBuffer).append(")");
        }
        if (endOfData) {
            sb.append(" EOD");
        }
        return sb.toString();
    }

    // implement TupleIter
    public void restart()
    {
        this.endOfData = false;
    }

    // implement TupleIter
    // Note that we hold the buffer whenever this returns something other
    // than NoDataReason.
    public Object fetchNext()
    {
        if (endOfData) {
            return NoDataReason.END_OF_DATA;
        } else if (byteBuffer.hasRemaining()) {
            return unmarshal();
        }

        int cb = populateBuffer();
        if (cb == 0) {
            bufferAsArray = null;
            endOfData = true;
            return NoDataReason.END_OF_DATA;
        } else if (cb < 0) {
            return NoDataReason.UNDERFLOW;
        }

        byteBuffer.limit(cb);
        return unmarshal();
    }

    private Object unmarshal()
    {
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
        traceNext(obj);
        if (!byteBuffer.hasRemaining()) {
            requestData();
        }
        return obj;
    }

    // override to trace the buffer state after unmarshal(), but before refill
    protected void traceNext(Object val)
    {
    }

    /**
     * Populates the buffer with a new batch of data, and returns the size in
     * bytes. The buffer position is set to the start. The call may block until
     * the buffer is filled or it may return an indication that there is no data
     * currently available. A subclass can implement this to fill the buffer
     * itself, or it can work by allowing an outside object to fill the buffer.
     *
     * @return The number of bytes now in the buffer. 0 means end of stream.
     * Less than 0 means no data currently available.
     */
    protected abstract int populateBuffer();

    /**
     * This method is called when the contents of the buffer have been consumed.
     * A subclass may use this method to tell the producer that it can start
     * producing. The default implementation does nothing. The method should not
     * block.
     */
    protected void requestData()
    {
    }
}

// End FennelAbstractTupleIter.java
