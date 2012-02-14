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
