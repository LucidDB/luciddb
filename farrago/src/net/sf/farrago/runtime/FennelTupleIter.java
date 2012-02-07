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
 * FennelTupleIter implements the {@link org.eigenbase.runtime.TupleIter}
 * interfaces by reading tuples from a Fennel ExecStream.
 *
 * <p>FennelTupleIter only deals with raw byte buffers; it delegates to a {@link
 * FennelTupleReader} object the responsibility to unmarshal individual fields.
 *
 * <p>FennelTupleIter's implementation of {@link #populateBuffer()} blocks.
 *
 * @author John V. Sichi, Stephan Zuercher
 * @version $Id$
 */
public class FennelTupleIter
    extends FennelAbstractTupleIter
{
    //~ Instance fields --------------------------------------------------------

    private final FennelStreamGraph streamGraph;
    private final FennelStreamHandle streamHandle;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FennelTupleIter object.
     *
     * @param tupleReader FennelTupleReader to use to interpret Fennel data
     * @param streamGraph underlying FennelStreamGraph
     * @param streamHandle handle to underlying Fennel ExecStream that this
     * TupleIter reads from
     * @param bufferSize number of bytes in buffer used for fetching from Fennel
     */
    public FennelTupleIter(
        FennelTupleReader tupleReader,
        FennelStreamGraph streamGraph,
        FennelStreamHandle streamHandle,
        int bufferSize)
    {
        super(tupleReader);
        this.streamGraph = streamGraph;
        this.streamHandle = streamHandle;

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
        streamGraph.restart(streamHandle);
    }

    // implement TupleIter
    public void closeAllocation()
    {
        // REVIEW: SWZ: 2/23/2006: Deallocate byteBuffer here?
    }

    /**
     * Blocking implementation of {@link FennelTupleIter#populateBuffer()}.
     *
     * @return number of bytes read into {@link FennelTupleIter#byteBuffer} or 0
     * for end of stream.
     */
    protected int populateBuffer()
    {
        byteBuffer.clear();
        return streamGraph.fetch(streamHandle, bufferAsArray);
    }
}

// End FennelTupleIter.java
