/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
package net.sf.farrago.type.runtime;

import org.eigenbase.util.*;

import java.io.*;
import java.nio.*;


/**
 * BytePointer is instantiated during execution to refer to a contiguous
 * subset of a byte array.  It exists to avoid the need to instantiate a new
 * object for each variable-width value read.
 *
 * <p>
 * NOTE:  BytePointer is not declared to implement NullableValue, although it
 * actually provides the necessary method implementations.  Instead, the
 * NullableValue interface is declared by generated subclasses representing
 * nullable types.  This allows the presence of the NullableValue interface
 * to be used in runtime contexts where we need to determine nullability but
 * have lost explicit type information during code generation.
 * </p>
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class BytePointer extends ByteArrayInputStream
    implements AssignableValue
{
    //~ Static fields/initializers --------------------------------------------

    public static final String ENFORCE_PRECISION_METHOD_NAME =
        "enforceBytePrecision";
    public static final String SET_POINTER_METHOD_NAME = "setPointer";

    /** Read-only global for 0-length byte array */
    public static final byte [] emptyBytes = new byte[0];

    //~ Instance fields -------------------------------------------------------

    // WARNING: The superclass field named count is totally misnamed; it should
    // be end.  Watch out for this.

    /**
     * An allocate-on-demand array used when a new value must
     * be created.
     */
    private byte [] ownBytes;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new BytePointer object.
     */
    public BytePointer()
    {
        super(emptyBytes);
    }

    //~ Methods ---------------------------------------------------------------

    // implement NullableValue
    public void setNull(boolean isNull)
    {
        if (isNull) {
            buf = null;
        } else {
            buf = emptyBytes;
        }
    }

    // implement NullableValue
    public boolean isNull()
    {
        return (buf == null);
    }

    // implement NullableValue
    public Object getNullableData()
    {
        if (buf == null) {
            return null;
        }
        if ((pos == 0) && (count == buf.length)) {
            return buf;
        }
        byte [] copy = new byte[count - pos];
        System.arraycopy(buf, pos, copy, 0, count - pos);
        return copy;
    }

    /**
     * Sets the pointer to reference a buffer.
     *
     * @param buf the buffer to point into
     * @param pos position in buffer to point at
     * @param end buffer position at which valid data ends
     */
    public void setPointer(
        byte [] buf,
        int pos,
        int end)
    {
        if (this.buf == null) {
            // if we've been set to a NULL value, someone has to explicitly
            // call setNull(false) before setPointer can take effect
            return;
        }
        this.buf = buf;
        this.pos = pos;
        this.count = end;
    }

    // implement AssignableValue
    public void assignFrom(Object obj)
    {
        if (obj == null) {
            setNull(true);
        } else if (obj instanceof BytePointer) {
            BytePointer other = (BytePointer) obj;
            buf = other.buf;
            pos = other.pos;
            count = other.count;
        } else if (obj instanceof byte []) {
            buf = (byte []) obj;
            pos = 0;
            count = buf.length;
        } else {
            String string = obj.toString();
            if (string == null) {
                setNull(true);
            } else {
                setNull(false);
                byte [] bytes = getBytesForString(string);
                setPointer(bytes, 0, bytes.length);
            }
        }
    }

    /**
     * Pads or truncates this value according to the given precision.
     *
     * @param precision desired precision
     *
     * @param needPad true if short values should be padded
     *
     * @param padByte byte to pad with
     */
    public void enforceBytePrecision(
        int precision,
        boolean needPad,
        byte padByte)
    {
        if (isNull()) {
            return;
        }
        int len = count - pos;
        if (len > precision) {
            // truncate
            count = pos + precision;
        } else if (needPad && (len < precision)) {
            // pad
            allocateOwnBytes(precision);
            System.arraycopy(buf, pos, ownBytes, 0, len);
            buf = ownBytes;
            for (; len < precision; ++len) {
                buf[len] = padByte;
            }
            pos = 0;
            count = precision;
        }
    }

    private void allocateOwnBytes(int n)
    {
        if ((ownBytes == null) || (ownBytes.length < n)) {
            ownBytes = new byte[n];
        }
    }

    /**
     * Writes the contents of this pointer to a ByteBuffer.
     *
     * @param byteBuffer target
     */
    public final void writeToBuffer(ByteBuffer byteBuffer)
    {
        if (buf == null) {
            return;
        }
        byteBuffer.put(buf, pos, count - pos);
    }

    /**
     * Writes the contents of this pointer to a ByteBuffer
     * at a given offset without modifying the current position.
     *
     * @param byteBuffer target
     *
     * @param offset starting byte offset within buffer
     */
    public final void writeToBufferAbsolute(
        ByteBuffer byteBuffer,
        int offset)
    {
        if (buf == null) {
            return;
        }
        int savedPos = byteBuffer.position();
        try {
            byteBuffer.position(offset);
            byteBuffer.put(buf, pos, count - pos);
        } finally {
            byteBuffer.position(savedPos);
        }
    }

    /**
     * Gets the byte representation of a string.  Subclasses may override.
     *
     * @param string source
     *
     * @return byte representation
     */
    protected byte [] getBytesForString(String string)
    {
        return string.getBytes();
    }

    /**
     * Implementation for Comparable.compareTo() which assumes non-null and
     * does byte-for-byte comparison.
     *
     * @param other another BytePointer to be compared
     *
     * @return same as compareTo
     */
    public int compareBytes(BytePointer other)
    {
        int i1 = pos;
        int i2 = other.pos;
        for (;; ++i1, ++i2) {
            if (i1 == count) {
                // this is either less than or equal to other depending on
                // whether we've reached the end of other
                return i2 - other.count;
            }
            if (i2 == other.count) {
                // we know i1 < count, so this must be greater than other
                return 1;
            }
            int c = (int) other.buf[i2] - (int) buf[i1];
            if (c != 0) {
                return c;
            }
        }
    }

    // implement Object
    public String toString()
    {
        if (buf == null) {
            return null;
        }
        int n = count - pos;
        byte [] bytes = new byte[n];
        System.arraycopy(buf, pos, bytes, 0, n);
        return Util.toStringFromByteArray(bytes, 16);
    }
}


// End BytePointer.java
