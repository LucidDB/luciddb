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

import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.util.*;

import java.nio.*;

import java.util.*;


/**
 * FennelIterator implements the Iterator interface by reading tuples from a
 * Fennel TupleStream.  FennelIterator only deals with raw byte buffers; it
 * is the responsibility of subclasses to unmarshal individual fields.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelIterator implements Iterator
{
    //~ Instance fields -------------------------------------------------------

    private ByteBuffer byteBuffer;
    private FennelStreamGraph streamGraph;
    private FemStreamHandle streamHandle;
    private FennelTupleReader tupleReader;
    private Object target;
    private byte [] bufferAsArray;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FennelIterator object.
     *
     * @param tupleReader FennelTupleReader to use to interpret Fennel data
     * @param streamGraph underlying FennelStreamGraph
     * @param streamHandle handle to underlying Fennel TupleStream
     * @param bufferSize number of bytes in buffer used for fetching from Fennel
     */
    public FennelIterator(
        FennelTupleReader tupleReader,
        FennelStreamGraph streamGraph,
        FemStreamHandle streamHandle,
        int bufferSize)
    {
        this.tupleReader = tupleReader;
        this.streamGraph = streamGraph;
        this.streamHandle = streamHandle;

        bufferAsArray = new byte[bufferSize];
        byteBuffer = ByteBuffer.wrap(bufferAsArray);
        byteBuffer.order(ByteOrder.nativeOrder());
        byteBuffer.clear();
        byteBuffer.limit(0);
    }

    //~ Methods ---------------------------------------------------------------

    // implement Iterator
    public boolean hasNext()
    {
        if (byteBuffer == null) {
            return false;
        }
        if (byteBuffer.hasRemaining()) {
            return true;
        }
        byteBuffer.clear();
        int cb = streamGraph.fetch(
            streamHandle,bufferAsArray);
        if (cb == 0) {
            byteBuffer = null;
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
            tupleReader.unmarshalTuple(byteBuffer,bufferAsArray,sliceBuffer);
        int newPosition = byteBuffer.position() + sliceBuffer.position();

        // eat final alignment padding
        while ((newPosition & 3) != 0) {
            ++newPosition;
        }
        byteBuffer.position(newPosition);
        return obj;
    }

    // implement Iterator
    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}


// End FennelIterator.java
