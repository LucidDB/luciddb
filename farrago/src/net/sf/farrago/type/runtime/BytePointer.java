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

import net.sf.farrago.resource.*;

import org.eigenbase.util.*;
import org.eigenbase.sql.fun.SqlTrimFunction;

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
    implements AssignableValue, CharSequence
{
    //~ Static fields/initializers --------------------------------------------

    public static final String ENFORCE_PRECISION_METHOD_NAME =
        "enforceBytePrecision";
    public static final String SET_POINTER_METHOD_NAME = "setPointer";
    public static final String GET_BYTE_COUNT_METHOD_NAME = "getByteCount";
    public static final String SUBSTRING_METHOD_NAME = "substring";
    public static final String OVERLAY_METHOD_NAME = "overlay";
    public static final String INITCAP_METHOD_NAME = "initcap";
    public static final String CONCAT_METHOD_NAME = "concat";
    public static final String UPPER_METHOD_NAME = "upper";
    public static final String LOWER_METHOD_NAME = "lower";
    public static final String TRIM_METHOD_NAME = "trim";
    public static final String POSITION_METHOD_NAME = "position";

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
	/**
	 * two temp variables to store the substring pointers
	 *
	 */
	private int S1;
	private int L1;

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

    /**
     * Reduces the value to a substring of the current value.
     *
     * @param starting desired starting position
     *
     * @param length  the length.
     *
     * @param useLength to indicate whether length parameter should be used. 
     *
     */
    public void substring(
        int starting,
        int length,
        boolean useLength)
    {
        calcSubstringPointers( starting, length, getByteCount(), useLength); 
        pos += S1;
        count = pos + L1;
    }

    /**
     * Assigns this value to the result of inserting bp2's value into bp1's
     * value at a specified starting point, possibly deleting a prefix of the
     * remainder of bp1 of a given length.  Implements the SQL string OVERLAY
     * function.
     *
     * @param bp1 string1
     *
     * @param bp2 string2
     *
     * @param s starting point
     *
     * @param l length
     *
     * @param useLength whether to use length parameter
     *
     */
    public void overlay(
        BytePointer bp1,
        BytePointer bp2,
        int starting,
        int length,
        boolean useLength)
    {
        if (!useLength) {
            length = bp2.getByteCount();
        }
        calcSubstringPointers( starting, length, bp1.getByteCount(), true); 
        int totalLength = bp2.getByteCount() + bp1.getByteCount() - L1;
        allocateOwnBytes(totalLength);
        if (L1 == 0 &&  starting > bp1.getByteCount()) {
            System.arraycopy(bp1.buf, bp1.pos, ownBytes, 0, bp1.getByteCount());
            System.arraycopy(bp2.buf, bp2.pos, ownBytes, bp1.getByteCount(), bp2.getByteCount());
        } else {
            System.arraycopy(bp1.buf, bp1.pos, ownBytes, 0, S1);
            System.arraycopy(bp2.buf, bp2.pos, ownBytes, S1, bp2.getByteCount());
            System.arraycopy(bp1.buf, bp1.pos + S1 + L1, 
                ownBytes, S1 + bp2.getByteCount(), 
                bp1.getByteCount() - S1 - L1);
        }
        buf = ownBytes;
        pos = 0;
        count = totalLength;
    }

    /**
     * Assigns this pointer to the result of concatenating two input strings.
     *
     * @param bp1 string1
     *
     * @param bp2 string2
     *
     */
    public void concat(
        BytePointer bp1,
        BytePointer bp2)
    {
        // can not be null.
        allocateOwnBytes(bp1.getByteCount()+bp2.getByteCount());
        System.arraycopy(bp1.buf, bp1.pos, ownBytes, 0, bp1.getByteCount());
        System.arraycopy(bp2.buf, bp2.pos, ownBytes, bp1.getByteCount(), 
                        bp2.getByteCount());
        buf = ownBytes;
        pos = 0;
        count = bp1.getByteCount()+bp2.getByteCount();
    }
    
    public int getByteCount() 
    {
        return available();
    }

    /* 
     * implement CharSequence
     * the Default implementation.
     * Only works for ISO-8859-1
     * If Unicode, or any other variable length
     * encoding, it needs to override these functions.
     *
     */

    public int length() 
    {
        return available();
    }

    public char charAt(int index) 
    {
        return (char) buf[pos+index];
    }

    public CharSequence subSequence(int start, int end) 
    {
        BytePointer bp = new BytePointer();
        if (start < 0 || end < 0 || end >= getByteCount()) {
            throw new IndexOutOfBoundsException();
        }
        bp.setPointer(buf, pos + start, pos + end);
        return bp;
    }

    /**
     * upper the case for each character of the string
     *
     * @param bp1 string1
     *
     */
    public void upper(BytePointer bp1)
    {
        copyFrom(bp1);
        for (int i = 0; i < count; i++) {
            if (Character.isLowerCase((char) ownBytes[i])) {
                ownBytes[i] = (byte)Character.toUpperCase((char) ownBytes[i]);
            }
        }
    }

    /**
     * lower the case for each character of the string
     *
     * @param bp1 string1
     *
     */
    public void lower(BytePointer bp1)
    { 
        copyFrom(bp1);
        for (int i = 0; i < count; i++) {
            if (Character.isUpperCase((char) ownBytes[i])) {
                ownBytes[i] = (byte)Character.toLowerCase((char) ownBytes[i]);
            }
        }
    }
                
    /**
     * initcap the string.
     *
     * @param bp1 string1
     *
     */
    public void initcap(BytePointer bp1)
    {
        boolean bWordBegin = true;
        copyFrom(bp1);
        for (int i = 0; i < count; i++) {
            if (Character.isWhitespace((char) ownBytes[i])) {
                bWordBegin = true;
            } else {
                if (bWordBegin) {
                    if (Character.isLowerCase((char) ownBytes[i])) {
                        ownBytes[i] = (byte)Character.toUpperCase(
                            (char) ownBytes[i]);
                    }
                } else{
                    if (Character.isUpperCase((char) ownBytes[i])) {
                        ownBytes[i] = (byte)Character.toLowerCase(
                            (char) ownBytes[i]);
                    }
                }
                bWordBegin = false;
            }
        }
    }

    public void trim(int trimOrdinal, BytePointer bp1, BytePointer bp2)
    {
        boolean leading = false;
        boolean trailing = false;
        int i;
        byte trimChar;

        if (bp1.getByteCount() != 1) {
            throw FarragoResource.instance().newInvalidFunctionArgument("trim");
        }
        copyFrom(bp2);
        trimChar = bp1.buf[bp1.pos];
        if (trimOrdinal == SqlTrimFunction.Flag.Both.getOrdinal()){
            leading = true;
            trailing = true;
        } else if (trimOrdinal == SqlTrimFunction.Flag.Leading.getOrdinal()){
            leading = true;
        } else {
            assert trimOrdinal == SqlTrimFunction.Flag.Trailing.getOrdinal();
            trailing = true;
        }
        int cnt = count;
        if (leading) {
            for (i = 0; i < cnt; i++) {
                if (buf[i] == trimChar) {
                    pos++;
                } else {
                    break;
                }
            }
        }
        if (trailing) {
            for (i = cnt - 1; i >= 0 ; i--) {
                if (buf[i] == trimChar) {
                    count--;
                } else {
                    break;
                }
            }
        }
    }

    public int position(BytePointer bp1)
    {
        if (bp1.getByteCount() == 0) {
            return 1;
        }
        int cnt1 = bp1.getByteCount();
        int cnt = getByteCount() - cnt1;
        for (int i = 0; i < cnt; i++) {
            boolean stillMatch = true; 
            for (int j = 0; j < cnt1; j++) {
                if (buf[pos+i+j] != bp1.buf[bp1.pos+j]) {
                    stillMatch = false;
                    break;
                }
            }
            if (stillMatch) {
                return i+1;
            }
        }
        return 0;
    }

    private void copyFrom(BytePointer bp1)
    {
        allocateOwnBytes(bp1.getByteCount());
        System.arraycopy(bp1.buf, bp1.pos, ownBytes, 0, bp1.getByteCount());
        buf = ownBytes;
        pos = 0;
        count = bp1.getByteCount();
    }

    /**
     * @sql.2003 Part 2 Section 6.29 General Rule 3
     */

    // we store the result in the member variables to avoid memory allocation.
    
    private void calcSubstringPointers(
        int S,
        int L,
        int LC,
        boolean useLength)
    {
        int e;
        if (useLength) {
            if (L < 0) {
                // If E is less than S, then it means L is negative exception.
                throw FarragoResource.instance().newNegativeLengthForSubstring();
            }
            e = S + L;
        } else {
            e = S;
            if (e <= LC) {
                e = LC + 1;
            }
        }

        // f) and i) in the standard. S > LC or E < 1 
        if (S > LC || e < 1) {
            S1 = L1 = 0;
        } else {
            // f) and ii) in the standard. 
            // calculate the E1 and L1
            S1 = S - 1;
            if (S1 < 0) {
                S1 = 0;
            }
            int e1 = e;
            if (e1 > LC) {
                e1 = LC + 1;
            }
            L1 = e1 - (S1 + 1);
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
            // need to convert the signed byte to unsigned.
            int c = (int) (0xFF & buf[i1]) - (int) (0xFF & other.buf[i2]);
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

    // private static fmt = new DecimalFormat;

    public void cast(double d, int precision)
    {
        // once precision is relaxed, we need to calculate the minimum
        // length and make sure the precision is >= minimum length.
        //
        int len = precision;

        if (d == 0) {
            allocateOwnBytes(3);
            ownBytes[0] = (byte) '0';
            ownBytes[1] = (byte) 'E';
            ownBytes[2] = (byte) '0';
            buf = ownBytes;
            pos = 0;
            count = 3;
            return;
        }
        if (precision < 23) {
            // in sync with 
            throw net.sf.farrago.resource.FarragoResource.instance().newOverflow();
        }
        boolean negative = false;

        if (d < 0.0) {
            negative = true;
            d = -d;
        } else {
            len--;
        }

        int exponent = 0;
        double  tempd;
        boolean exponentNegative = false;

        if (d < 1.0) {
            for (tempd = d; tempd < 1.0; tempd *= 10) {
                exponent++;
            }
            exponentNegative = true;
        } else {
            for (tempd = d; tempd >= 10.0; tempd /= 10) {
                exponent++;
            }
        }

        // E , firstDigit, and sign for exponenent.
        int minimumDigits = exponent + 1 + 1 + 1; 
        if (negative) {
            minimumDigits++;
        }
        if (minimumDigits>len) {
            throw net.sf.farrago.resource.FarragoResource.instance().newOverflow();
        }

        allocateOwnBytes(len);
        int currentLength = len;

        int digitsOfExponent = 0;
        for (int tempe = exponent; tempe != 0; tempe = tempe/10) {
            digitsOfExponent++;
            currentLength--;
            ownBytes[currentLength] = (byte) ('0' + tempe % 10);
        }
        // if less than two digits, need to fill them.
        if (digitsOfExponent == 0) {
            currentLength--;
            ownBytes[currentLength] = (byte) '0';
            digitsOfExponent++;
        }
        if (digitsOfExponent == 1) {
            currentLength--;
            ownBytes[currentLength] = (byte) '0';
        }

        currentLength--;
        if (exponentNegative) {
            ownBytes[currentLength] = (byte) '-';
        } else {
            ownBytes[currentLength] = (byte) '+';
        }
        currentLength--;
        ownBytes[currentLength] = (byte) 'E';
        // now, we can just keep using the floor part.
        int currentStart = 0;
        if (negative) {
            ownBytes[0] = (byte) '-';
            currentStart = 1;
        }

        int currentChar = (int)tempd;
        ownBytes[currentStart] = (byte) (currentChar + '0');
        currentStart++;
        tempd = (tempd - currentChar)*10;
        ownBytes[currentStart] = (byte) '.';
        currentStart++;

        for (int i = currentStart; i < currentLength; i++) {
            if (i != currentLength-1) {
                currentChar = (int)tempd;
            } else {
                currentChar = (int)(tempd+0.5); // last digit, rounding
            }
            // currentChar must be between 0 and 10
            ownBytes[i] = (byte) (currentChar + '0');
            tempd = (tempd - currentChar)*10;
        }

        buf = ownBytes;
        pos = 0;
        count = len;
    }

    public void cast(long l, int precision)
    {
        boolean negative = false;
        if (l < 0) {
            l = -l;
            negative = true;
        }
        int len = 0;
        long templ;

        for (templ = l ;templ != 0; templ = templ/10) 
        {
            len++;
        }

        if (negative) {
            len++;
        }
        if (len > precision) {
            throw net.sf.farrago.resource.FarragoResource.instance().newOverflow();
        }

        if (l == 0) {
            len = 1;
        }
        allocateOwnBytes(len);
        if (l == 0) {
            ownBytes[0] = (byte) '0';
        } else {
            if (negative) {
                ownBytes[0] = (byte) '-';
            }
            int i = 0;
            for (templ=l; templ != 0; i++, templ = templ/10) {
                int currentDigit = (int) (templ % 10);
                ownBytes[len - 1 - i] = (byte) ('0' + (char) currentDigit);
            }
        }

        buf = ownBytes;
        pos = 0;
        count = len;
    }
}


// End BytePointer.java
