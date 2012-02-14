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

import net.sf.farrago.fennel.*;


/**
 * FennelTransformTupleIter implements the {@link
 * org.eigenbase.runtime.TupleIter} interfaces by reading tuples from a Fennel
 * JavaTransformExecStream.
 *
 * <p>FennelTransformTupleIter only deals with raw byte buffers; it delegates to
 * a {@link FennelTupleReader} object the responsibility to unmarshal individual
 * fields.
 *
 * <p>FennelTransformTupleIter's implementation of {@link #populateBuffer()}
 * does not block.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public class FennelTransformTupleIter
    extends FennelAbstractTupleIter
{
    //~ Instance fields --------------------------------------------------------

    private final int execStreamInputOrdinal;

    private final FennelStreamGraph streamGraph;
    private final FennelStreamHandle streamHandle;
    private final FennelStreamHandle inputStreamHandle;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FennelTransformTupleIter object.
     *
     * @param tupleReader FennelTupleReader to use to interpret Fennel data
     * @param streamGraph underlying FennelStreamGraph
     * @param streamHandle handle to underlying Fennel JavaTransformExecStream
     * @param inputStreamHandle handle to the Fennel ExecStream that this
     * TupleIter reads from -- used only for reset
     * @param inputOrdinal the input stream's ordinal in the underlying
     * JavaTransformExecStream
     * @param bufferSize number of bytes in buffer used for fetching from Fennel
     */
    public FennelTransformTupleIter(
        FennelTupleReader tupleReader,
        FennelStreamGraph streamGraph,
        FennelStreamHandle streamHandle,
        FennelStreamHandle inputStreamHandle,
        int inputOrdinal,
        int bufferSize)
    {
        super(tupleReader);

        this.execStreamInputOrdinal = inputOrdinal;
        this.streamGraph = streamGraph;
        this.streamHandle = streamHandle;
        this.inputStreamHandle = inputStreamHandle;

        // In this implementation of FennelAbstractTupleIter, byteBuffer and
        // bufferAsArray are effectively final. In other implementations, they
        // might be set by populateBuffer.
        bufferAsArray = new byte[bufferSize];
        byteBuffer = ByteBuffer.wrap(bufferAsArray);
        byteBuffer.order(ByteOrder.nativeOrder());
        byteBuffer.clear();
        byteBuffer.limit(0);
    }

    //~ Methods ----------------------------------------------------------------

    // override FennelAbstractTupleIter
    public void restart()
    {
        super.restart();
        bufferAsArray = byteBuffer.array();
        byteBuffer.clear();
        byteBuffer.limit(0);

        // a reset on streamHandle is what got us here -- pass it on
        streamGraph.restart(inputStreamHandle);
    }

    // implement TupleIter
    public void closeAllocation()
    {
        // REVIEW: SWZ: 2/23/2006: Deallocate byteBuffer here?
    }

    /**
     * Non-blocking implementation of {@link FennelTupleIter#populateBuffer()}.
     *
     * @return number of bytes read into {@link FennelTupleIter#byteBuffer}, 0
     * for end of stream, less than 0 for no data available
     */
    protected int populateBuffer()
    {
        byteBuffer.clear();
        int cb =
            streamGraph.transformFetch(
                streamHandle,
                execStreamInputOrdinal,
                bufferAsArray);
        if (cb < 0) {
            // don't let anyone assume anything was read
            byteBuffer.limit(0);
        }
        return cb;
    }
}

// End FennelTransformTupleIter.java
