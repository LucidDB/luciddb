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

import net.sf.farrago.fennel.FennelStreamGraph;
import net.sf.farrago.fennel.FennelStreamHandle;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;


/**
 * FennelIterator implements the {@link java.util.Iterator} and
 * {@link org.eigenbase.runtime.RestartableIterator} interfaces by reading
 * tuples from a Fennel ExecStream.
 *
 * <p>FennelIterator only deals with raw byte buffers; it delegates to a
 * {@link FennelTupleReader} object the responsibility to unmarshal individual
 * fields.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelIterator extends FennelAbstractIterator
{
    //~ Instance fields -------------------------------------------------------
    private final FennelStreamGraph streamGraph;
    private final FennelStreamHandle streamHandle;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FennelIterator object.
     *
     * @param tupleReader FennelTupleReader to use to interpret Fennel data
     * @param streamGraph underlying FennelStreamGraph
     * @param streamHandle handle to underlying Fennel TupleStream
     * @param bufferSize number of bytes in buffer used for fetching from
     *     Fennel
     */
    public FennelIterator(
        FennelTupleReader tupleReader,
        FennelStreamGraph streamGraph,
        FennelStreamHandle streamHandle,
        int bufferSize)
    {
        super(tupleReader);
        this.streamGraph = streamGraph;
        this.streamHandle = streamHandle;

        // In this implementation of FennelAbstractIterator, byteBuffer and
        // bufferAsArray are effectively final. In other implementations, they
        // might be set by populateBuffer.
        bufferAsArray = new byte[bufferSize];
        byteBuffer = ByteBuffer.wrap(bufferAsArray);
        byteBuffer.order(ByteOrder.nativeOrder());
        byteBuffer.clear();
        byteBuffer.limit(0);
    }

    //~ Methods ---------------------------------------------------------------

    // implement RestartableIterator
    public void restart()
    {
        bufferAsArray = byteBuffer.array();
        byteBuffer.clear();
        byteBuffer.limit(0);
        streamGraph.restart(streamHandle);
    }

    protected int populateBuffer()
    {
        return streamGraph.fetch(streamHandle, bufferAsArray);
    }

}


// End FennelIterator.java
