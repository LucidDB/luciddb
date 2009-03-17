/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 Disruptive Tech
// Copyright (C) 2006-2007 LucidEra, Inc.
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
