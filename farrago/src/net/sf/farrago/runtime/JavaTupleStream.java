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

import java.nio.*;
import java.util.*;


/**
 * JavaTupleStream implements the contract expected by the Fennel C++
 * JavaTupleStream XO.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class JavaTupleStream
{
    //~ Instance fields -------------------------------------------------------

    private FennelTupleWriter tupleWriter;
    private Iterator iter;
    private Object next;

    //~ Constructors ----------------------------------------------------------

    /**
     * Construct a new JavaTupleStream.
     *
     * @param tupleWriter the FennelTupleWriter to use for marshalling tuples
     * @param iter Iterator producing objects
     */
    public JavaTupleStream(
        FennelTupleWriter tupleWriter,
        Iterator iter)
    {
        this.tupleWriter = tupleWriter;
        this.iter = iter;
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
     * @return number of bytes written to buffer
     */
    private int fillBuffer(ByteBuffer byteBuffer)
    {
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
}


// End JavaTupleStream.java
