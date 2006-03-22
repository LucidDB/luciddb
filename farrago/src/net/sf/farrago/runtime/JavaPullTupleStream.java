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
import java.util.*;

import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.runtime.TupleIter;
import org.eigenbase.util.*;

/**
 * JavaPullTupleStream is the counterpart of a Fennel C++ JavaPullSource XO On
 * request from its C++ peer, it gets rows from a Java {@link TupleIter},
 * converts them to fennel tuples, and marshals them into a buffer provided by
 * the peer.  It runs as a subroutine to the XO, which waits for it to
 * complete.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class JavaPullTupleStream implements JavaTupleStream
{
    //~ Instance fields -------------------------------------------------------

    private FennelTupleWriter tupleWriter;
    private final Iterator iter;
    private final TupleIter tupleIter;
    private Object next;

    //~ Constructors ----------------------------------------------------------

    /**
     * Constructs a new JavaPullTupleStream.
     *
     * @param tupleWriter the FennelTupleWriter to use for marshalling tuples
     * @param iter Iterator producing objects
     */
    public JavaPullTupleStream(
        FennelTupleWriter tupleWriter,
        TupleIter tupleIter)
    {
        this.tupleWriter = tupleWriter;
        this.iter = null;
        this.tupleIter = tupleIter;
        next = null;
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Called from native code.
     *
     * @param byteBuffer target buffer for marshalled tuples; note that this
     *        is allocated from within native code so that cache pages can be
     *        used, and so that data never needs to be copied
     *
     * @return number of bytes written to buffer; 0 indicates end of stream;
     *         less than 0 indicates no data currently available
     */
    private int fillBuffer(ByteBuffer byteBuffer)
    {
        if (tupleIter != null) {
            return fillBufferFromTupleIter(byteBuffer);
        }

        if (next == null) {
            if (!iter.hasNext()) {
                return 0;
            }
            next = iter.next();
        }
        byteBuffer.order(ByteOrder.nativeOrder());
        byteBuffer.clear();
        for (;;) {
            if (!tupleWriter.marshalTuple(byteBuffer, next)) {
                break;
            }
            if (!iter.hasNext()) {
                next = null;
                break;
            }
            next = iter.next();
        }
        byteBuffer.flip();
        return byteBuffer.limit();
    }

    /**
     * Called from native code.
     *
     * @param byteBuffer target buffer for marshalled tuples; note that this
     *        is allocated from within native code so that cache pages can be
     *        used, and so that data never needs to be copied
     *
     * @return number of bytes written to buffer, 0 indicates end of stream,
     *         less than 0 indicates iterator underflow
     */
    private int fillBufferFromTupleIter(ByteBuffer byteBuffer)
    {
        if (next == null) {
            Object o = tupleIter.fetchNext();
            
            if (o == TupleIter.NoDataReason.END_OF_DATA) {
                return 0;
            } else if (o == TupleIter.NoDataReason.UNDERFLOW) {
                return -1;
            }
            
            next = o;
        }
        
        byteBuffer.order(ByteOrder.nativeOrder());
        byteBuffer.clear();
        
        for (;;) {
            if (!tupleWriter.marshalTuple(byteBuffer, next)) {
                break;
            }
            
            Object o = tupleIter.fetchNext();
            
            if (o == TupleIter.NoDataReason.END_OF_DATA) {
                // Will return 0 on next call to this method.
                next = null;
                break;
            } else if (o == TupleIter.NoDataReason.UNDERFLOW) {
                // We've marshalled at least one tuple, so we don't
                // return -1 here. Will try again on next call to this 
                // method.
                next = null;
                break;
            }
            
            next = o;
        }
        byteBuffer.flip();
        return byteBuffer.limit();
    }

    // implement JavaTupleStream
    public void restart()
    {
        if (iter != null) {
            Util.restartIterator(iter);
        } else {
            tupleIter.restart();
        }
        next = null;
    }
}


// End JavaPullTupleStream.java

