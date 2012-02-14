/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package net.sf.farrago.runtime;

import java.nio.*;

import net.sf.farrago.fennel.tuple.*;

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
    extends AbstractTupleIter
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Singleton helper for aligning tuple buffers correctly.
     */
    private static FennelTupleAccessor tupleAligner = new FennelTupleAccessor();

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
        newPosition = tupleAligner.alignRoundUp(newPosition);

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
