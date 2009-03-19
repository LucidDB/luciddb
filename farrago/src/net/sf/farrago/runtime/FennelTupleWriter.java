/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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

import net.sf.farrago.fennel.tuple.*;


/**
 * FennelTupleWriter defines an interface for marshalling tuples to be sent to
 * Fennel. Implementations are responsible for marshalling specific tuple
 * formats.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FennelTupleWriter
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Matches fennel/tuple/TupleAccessor.cpp.
     */
    private static long MAGIC_NUMBER = 0x9897ab509de7dcf5L;

    /**
     * Singleton helper for aligning tuple buffers correctly.
     */
    private static FennelTupleAccessor tupleAligner = new FennelTupleAccessor();

    //~ Methods ----------------------------------------------------------------

    /**
     * Marshals one tuple if it can fit; otherwise, throws either {@link
     * BufferOverflowException} or {@link IndexOutOfBoundsException} (depending
     * on whether absolute or relative puts are used).
     *
     * @param sliceBuffer buffer to be filled with marshalled tuple data; on
     * entry, the buffer position is 0; on return, the buffer position should be
     * the unaligned end of the tuple
     * @param object subclass-specific object to be marshalled
     *
     * @exception BufferOverflowException see above
     * @exception IndexOutOfBoundsException see above
     */
    protected abstract void marshalTupleOrThrow(
        ByteBuffer sliceBuffer,
        Object object);

    /**
     * Marshals one tuple if it can fit.
     *
     * @param byteBuffer buffer to be filled with marshalled tuple data,
     * starting at current buffer position; on return, the buffer position
     * should be the unaligned end of the tuple
     * @param object subclass-specific object to be marshalled
     *
     * @return whether the marshalled tuple fit in the available buffer space
     */
    public boolean marshalTuple(
        ByteBuffer byteBuffer,
        Object object)
    {
        try {
            // REVIEW:  is slice allocation worth it?
            ByteBuffer sliceBuffer = byteBuffer.slice();
            sliceBuffer.order(byteBuffer.order());

            // In case TupleAccessor's DEBUG_TUPLE_ACCESS is enabled,
            // store the correct magic number at the beginning of the
            // marshalled tuple.  TODO:  don't do this unless needed.
            sliceBuffer.putLong(0, MAGIC_NUMBER);
            marshalTupleOrThrow(sliceBuffer, object);
            int newPosition = byteBuffer.position() + sliceBuffer.position();

            // add final alignment padding
            newPosition = tupleAligner.alignRoundUp(newPosition);

            byteBuffer.position(newPosition);
        } catch (BufferOverflowException ex) {
            return false;
        } catch (BufferUnderflowException ex) {
            // NOTE jvs 19-May-2006:  We shouldn't need this case,
            // but JRockit mistakenly throws underflow instead of overflow.
            return false;
        } catch (IndexOutOfBoundsException ex) {
            return false;
        } catch (IllegalArgumentException ex) {
            // NOTE jvs 31-Aug-2004:  The position() call throws this instead
            // of BufferOverflowException.
            return false;
        }
        return true;
    }
}

// End FennelTupleWriter.java
