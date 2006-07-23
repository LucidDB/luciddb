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
