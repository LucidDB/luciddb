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


/**
 * FennelTupleWriter defines an interface for marshalling tuples to be sent to
 * Fennel.  Implementations are responsible for marshalling specific tuple
 * formats.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FennelTupleWriter
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Marshal one tuple if it can fit; otherwise, throw either
     * BufferOverflowException or IndexOutOfBoundsException (depending on
     * whether absolute or relative puts are used).
     *
     * @param sliceBuffer buffer to be filled with marshalled tuple data; on
     *        entry, the buffer position is 0; on return, the buffer position
     *        should be the unaligned end of the tuple
     * @param object subclass-specific object to be marshalled
     *
     * @exception BufferOverflowException see above
     * @exception IndexOutOfBoundException see above
     */
    protected abstract void marshalTupleOrThrow(
        ByteBuffer sliceBuffer,Object object);

    /**
     * Marshal one tuple if it can fit.
     *
     * @param byteBuffer buffer to be filled with marshalled tuple data,
     * starting at current buffer position; on return, the buffer position
     * should be the unaligned end of the tuple
     *
     * @param object subclass-specific object to be marshalled
     *
     * @return whether the marshalled tuple fit in the available
     * buffer space
     */
    public boolean marshalTuple(
        ByteBuffer byteBuffer,Object object)
    {
        try {
            // REVIEW:  is slice allocation worth it?
            ByteBuffer sliceBuffer = byteBuffer.slice();
            sliceBuffer.order(byteBuffer.order());
            marshalTupleOrThrow(sliceBuffer,object);
            int newPosition =
                byteBuffer.position() + sliceBuffer.position();

            // add final alignment padding
            while ((newPosition & 3) != 0) {
                ++newPosition;
            }
            byteBuffer.position(newPosition);
        } catch (BufferOverflowException ex) {
            return false;
        } catch (IndexOutOfBoundsException ex) {
            return false;
        }
        return true;
    }
}


// End FennelTupleWriter.java
