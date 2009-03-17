/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2009-2009 The Eigenbase Project
// Copyright (C) 2009-2009 Disruptive Tech
// Copyright (C) 2009-2009 LucidEra, Inc.
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

import org.eigenbase.sql.fun.*;
import org.eigenbase.util14.*;

/**
 * Ucs2CharPointer specializes EncodedCharPointer to interpret its bytes as
 * characters encoded via a UCS-2 charset.
 *
 * @author John Sichi
 * @version $Id$
 */
public class Ucs2CharPointer
    extends EncodedCharPointer
{
    // override BytePointer
    public int length()
    {
        return available() >>> 1;
    }
    
    // override BytePointer
    protected void allocateOwnBytesForPrecision(int n)
    {
        allocateOwnBytes(n << 1);
    }

    protected int getByteCountForPrecision(int n)
    {
        return n << 1;
    }

    // override BytePointer
    public void enforceBytePrecision(
        int precision,
        boolean needPad,
        byte padByte)
    {
        if (isNull()) {
            return;
        }
        int len = count - pos;
        int lenChars = len >>> 1;
        int precBytes = precision << 1;
        if (lenChars > precision) {
            // truncate
            count = pos + precBytes;
        } else if (needPad && (lenChars < precision)) {
            // pad
            allocateOwnBytesForPrecision(precision);
            System.arraycopy(buf, pos, ownBytes, 0, len);
            buf = ownBytes;
            // FIXME jvs 21-Jan-2009:  code below assumes little-endian
            for (; len < precBytes; ++len) {
                buf[len] = padByte;
                ++len;
                buf[len] = 0;
            }
            pos = 0;
            count = precBytes;
        }
    }

    // override BytePointer
    public void substring(
        int starting,
        int length,
        boolean useLength)
    {
        calcSubstringPointers(
            starting,
            length,
            getByteCount() >>> 1,
            useLength);
        pos += (S1 << 1);
        count = pos + (L1 << 1);
    }

    // override BytePointer
    public void overlay(
        BytePointer bp1,
        BytePointer bp2,
        int starting,
        int length,
        boolean useLength)
    {
        assert(bp1 instanceof Ucs2CharPointer);
        assert(bp2 instanceof Ucs2CharPointer);
        
        if (!useLength) {
            length = (bp2.getByteCount() >>> 1);
        }
        calcSubstringPointers(
            starting,
            length,
            bp1.getByteCount() >>> 1,
            true);
        S1 <<= 1;
        L1 <<= 1;
        finishOverlay(bp1, bp2, starting << 1);
    }

    // override BytePointer
    public int position(BytePointer bp1)
    {
        assert(bp1 instanceof Ucs2CharPointer);
        
        int p = super.positionImpl(bp1, 2);
        if (p == 0) {
            return p;
        }
        return (p >>> 1) + 1;
    }
    
    // override BytePointer
    public char charAt(int index)
    {
        int x = pos + (index << 1);
        int b0 = buf[x];
        int b1 = buf[x + 1];
        // FIXME jvs 21-Jan-2009:  code below assumes little-endian
        return (char) ((b1 << 8) + b0);
    }

    protected void setCharAt(int index, char c)
    {
        // FIXME jvs 21-Jan-2009:  code below assumes little-endian
        int x = pos + (index << 1);
        int b0 = ((int) c) & 0xFF;
        int b1 = ((int) c) >>> 8;
        buf[x] = (byte) b0;
        buf[x+1] = (byte) b1;
    }

    // override BytePointer
    public CharSequence subSequence(int start, int end)
    {
        Ucs2CharPointer bp = new Ucs2CharPointer();
        if ((start < 0) || (end < start) || (end >= getByteCount())) {
            throw new IndexOutOfBoundsException();
        }
        bp.setPointer(buf, pos + (start << 1), pos + (end << 1));
        return bp;
    }

    // override EncodedCharPointer
    protected String getCharsetName()
    {
        return ConversionUtil.NATIVE_UTF16_CHARSET_NAME;
    }
    
    // override EncodedCharPointer
    public void trim(int trimOrdinal, BytePointer bp1, BytePointer bp2)
    {
        assert(bp1 instanceof Ucs2CharPointer);
        assert(bp2 instanceof Ucs2CharPointer);
        
        boolean leading = false;
        boolean trailing = false;
        int i;
        char trimChar;

        if (bp1.getByteCount() != 2) {
            throw FarragoResource.instance().InvalidFunctionArgument.ex(
                SqlStdOperatorTable.trimFunc.getName());
        }
        copyFrom(bp2);
        trimChar = bp1.charAt(0);
        if (trimOrdinal == SqlTrimFunction.Flag.BOTH.ordinal()) {
            leading = true;
            trailing = true;
        } else if (trimOrdinal == SqlTrimFunction.Flag.LEADING.ordinal()) {
            leading = true;
        } else {
            assert trimOrdinal == SqlTrimFunction.Flag.TRAILING.ordinal();
            trailing = true;
        }
        int cnt = length();
        if (leading) {
            for (i = 0; i < cnt; i++) {
                if (charAt(0) == trimChar) {
                    pos += 2;
                } else {
                    break;
                }
            }
        }
        if (trailing) {
            if (pos == count) {
                // already trimmed away an entire empty string;
                // don't do it twice!  (FRG-319)
                return;
            }
            // in case pos moved up, reduce cnt
            cnt = length();
            for (i = cnt - 1; i >= 0; i--) {
                if (charAt(i) == trimChar) {
                    count -= 2;
                } else {
                    break;
                }
            }
        }
    }

    // override BytePointer
    public void upper(BytePointer bp1)
    {
        assert(bp1 instanceof Ucs2CharPointer);
        copyFrom(bp1);
        int n = length();
        for (int i = 0; i < n; i++) {
            char c = charAt(i);
            if (Character.isLowerCase(c)) {
                setCharAt(i, Character.toUpperCase(c));
            }
        }
    }
    
    // override BytePointer
    public void lower(BytePointer bp1)
    {
        assert(bp1 instanceof Ucs2CharPointer);
        copyFrom(bp1);
        int n = length();
        for (int i = 0; i < n; i++) {
            char c = charAt(i);
            if (Character.isUpperCase(c)) {
                setCharAt(i, Character.toLowerCase(c));
            }
        }
    }
    
    // override BytePointer
    public void initcap(BytePointer bp1)
    {
        assert(bp1 instanceof Ucs2CharPointer);
        boolean bWordBegin = true;
        copyFrom(bp1);
        int n = length();
        for (int i = 0; i < n; i++) {
            char c = charAt(i);
            if (Character.isWhitespace(c)) {
                bWordBegin = true;
            } else {
                if (bWordBegin) {
                    if (Character.isLowerCase(c)) {
                        setCharAt(i, Character.toUpperCase(c));
                    }
                } else {
                    if (Character.isUpperCase(c)) {
                        setCharAt(i, Character.toLowerCase(c));
                    }
                }
                bWordBegin = false;
            }
        }
    }
    
    // override BytePointer
    public long attemptFastAsciiByteToLong()
    {
        // can't deal with Unicode yet, so punt
        return Long.MAX_VALUE;
    }
}

// End Ucs2CharPointer.java
