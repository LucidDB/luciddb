/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
package net.sf.farrago.fennel.tuple;

import java.io.*;


/**
 * A FennelTupleDatum is a component of FennelTupleData; see the fennel tuple <a
 * href="http://fennel.sourceforge.net/doxygen/html/structTupleDesign.html">
 * design document</a> for more details.
 *
 * <p>This differs from the C++ version as we can't represent pointers in java.
 * Therefore all the primitive accessors have been provided as methods of this
 * object.
 *
 * <p>Internally, this object attempts to bypass object creation during normal
 * use. It does this by wrangling all numeric primitive types into a 64-bit
 * (long) value and all array data into a byte array. This class is JDK 1.4
 * compatible.
 */
// NOTE: things incomplete at this time: (11jan05)
//  - numerics and byte arrays are tracked independently; we should
//    be able to transmogrify one into the other as needed (that is:
//     datum.setShort() allows datum.getBytes() to get a byte array
//  - int64 unsigned is only being kept as unsigned; I think this
//    should be promoted to a BigInteger for this one value, or we
//    drop into storing it as a byte array
//
public class FennelTupleDatum
{
    //~ Instance fields --------------------------------------------------------

    /**
     * length of this data in externallized form.
     */
    private int dataLen;

    /**
     * maximum size of this data in externallized form.
     */
    private int capacity;

    /**
     * the numeric object kept by this tuple; this holds all the numeric
     * primitive type.
     */
    private long numeric;

    /**
     * indicates whether the numeric value has been set.
     */
    private boolean numericSet;

    /**
     * a byte array holding non-numeric information.
     */
    private byte [] rawBytes;

    /**
     * a byte array holding the initial byte array, of capacity size.
     */
    private byte [] initialBytes;

    /**
     * indicates whether the byte array has been set.
     */
    private boolean rawBytesSet;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a raw datum; setCapacity must be called before use.
     */
    public FennelTupleDatum()
    {
        dataLen = 0;
        capacity = 0;
        rawBytesSet = false;
        numeric = 0;
        numericSet = false;
    }

    /**
     * Constructs a datum with a defined capacity. This is the normal
     * constructor.
     */
    public FennelTupleDatum(int capacity)
    {
        dataLen = 0;
        setCapacity(capacity);
        rawBytesSet = false;
        numeric = 0;
        numericSet = false;
    }

    /**
     * copy constructor.
     */
    public FennelTupleDatum(FennelTupleDatum other)
    {
        copyFrom(other);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * sets the capacity of this data item.
     */
    public void setCapacity(int capacity)
    {
        if (this.capacity != capacity) {
            initialBytes = new byte[capacity];
            this.capacity = capacity;
            rawBytes = initialBytes;
            rawBytesSet = false;
        }
    }

    /**
     * gets the capacity of this data item.
     */
    public int getCapacity()
    {
        return capacity;
    }

    /**
     * Indicates whether data is present in this datum.
     */
    public boolean isPresent()
    {
        return (rawBytesSet || numericSet);
    }

    /**
     * resets a datum for reuse.
     */
    public void reset()
    {
        if (rawBytesSet) {
            rawBytesSet = false;
            rawBytes = initialBytes;
        }
        numericSet = false;
        dataLen = 0;

        // should be unnecessary
        numeric = 0;
    }

    /**
     * gets the length, in bytes, of this datum.
     */
    public int getLength()
    {
        return dataLen;
    }

    /**
     * sets the length of this datum's byte array.
     */
    public void setLength(int len)
    {
        dataLen = len;
    }

    /**
     * copy construction helper.
     */
    public void copyFrom(FennelTupleDatum other)
    {
        setCapacity(capacity);
        dataLen = other.dataLen;
        rawBytesSet = other.rawBytesSet;
        rawBytes = other.rawBytes;
        capacity = other.capacity;
        numeric = other.numeric;
        numericSet = other.numericSet;
    }

    /**
     * used by the attribute marshalling to set numeric values.
     */
    public void setLong(long n)
    {
        numericSet = true;
        numeric = n;
    }

    /**
     * sets the numeric value of a signed integer.
     */
    public void setInt(int n)
    {
        setLong((long) n);
    }

    /**
     * sets the numeric value of an unsigned integer.
     */
    public void setUnsignedInt(long val)
    {
        setLong((long) (val & 0xffffffff));
    }

    /**
     * sets the numeric value of a signed short.
     */
    public void setShort(short n)
    {
        setLong((long) n);
    }

    /**
     * sets the numeric value of an unsigned short.
     */
    public void setUnsignedShort(int val)
    {
        setLong((long) (val & 0xffff));
    }

    /**
     * sets the numeric value of a signed byte.
     */
    public void setByte(byte n)
    {
        setLong((long) n);
    }

    /**
     * sets the numeric value of an unsigned byte.
     */
    public void setUnsignedByte(short val)
    {
        setLong((long) (val & 0xff));
    }

    /**
     * sets the numeric value of an unsigned long.
     */
    // FIXME - bogus - we can't represent 64-bit unsigned!
    public void setUnsignedLong(long val)
    {
        setLong(val);
    }

    /**
     * sets the numeric value of a float.
     */
    public void setFloat(float val)
    {
        setInt(Float.floatToIntBits(val));
    }

    /**
     * sets the numeric value of a double.
     */
    public void setDouble(double val)
    {
        setLong(Double.doubleToLongBits(val));
    }

    /**
     * sets a boolean value.
     */
    public void setBoolean(boolean val)
    {
        if (val) {
            setLong(1L);
        } else {
            setLong(0L);
        }
    }

    /**
     * used by the marshalling routines to set a byte array.
     */
    public byte [] setRawBytes()
    {
        rawBytesSet = true;
        return rawBytes;
    }

    /**
     * gets a signed long value.
     */
    public long getLong()
        throws NullPointerException
    {
        if (!numericSet) {
            throw new NullPointerException("numeric not present");
        }
        return numeric;
    }

    /**
     * gets a signed int value.
     */
    public int getInt()
        throws NullPointerException
    {
        if (!numericSet) {
            throw new NullPointerException("numeric not present");
        }
        return (int) numeric;
    }

    /**
     * gets a signed short value.
     */
    public short getShort()
        throws NullPointerException
    {
        if (!numericSet) {
            throw new NullPointerException("numeric not present");
        }
        return (short) numeric;
    }

    /**
     * gets a signed byte value.
     */
    public byte getByte()
        throws NullPointerException
    {
        if (!numericSet) {
            throw new NullPointerException("numeric not present");
        }
        return (byte) numeric;
    }

    /**
     * gets an unsigned byte value
     */
    public short getUnsignedByte()
        throws NullPointerException
    {
        if (!numericSet) {
            throw new NullPointerException("numeric not present");
        }
        return (short) ((numeric << 56) >>> 56);
    }

    /**
     * gets an unsigned short value.
     */
    public int getUnsignedShort()
        throws NullPointerException
    {
        if (!numericSet) {
            throw new NullPointerException("numeric not present");
        }
        return (int) ((numeric << 48) >>> 48);
    }

    /**
     * gets an unsigned int value.
     */
    public long getUnsignedInt()
        throws NullPointerException
    {
        if (!numericSet) {
            throw new NullPointerException("numeric not present");
        }
        return (long) (numeric << 32) >>> 32;
    }

    /**
     * gets an unsigned long value.
     */
    public long getUnsignedLong()
        throws NullPointerException
    {
        if (!numericSet) {
            throw new NullPointerException("numeric not present");
        }

        // FIXME
        return numeric;
    }

    /**
     * gets a float value.
     */
    public float getFloat()
        throws NullPointerException
    {
        if (!numericSet) {
            throw new NullPointerException("numeric not present");
        }
        return Float.intBitsToFloat((int) numeric);
    }

    /**
     * gets a double value.
     */
    public double getDouble()
        throws NullPointerException
    {
        if (!numericSet) {
            throw new NullPointerException("numeric not present");
        }
        return Double.longBitsToDouble(numeric);
    }

    /**
     * gets a boolean value.
     */
    public boolean getBoolean()
        throws NullPointerException
    {
        if (!numericSet) {
            throw new NullPointerException("numeric not present");
        }
        return (numeric != 0);
    }

    /**
     * gets a byte array.
     */
    public byte [] getBytes()
        throws NullPointerException
    {
        if (!rawBytesSet) {
            throw new NullPointerException("bytes not present");
        }
        return rawBytes;
    }

    /**
     * set the byte array.
     */
    public void setBytes(byte [] bytes)
    {
        rawBytes = bytes;
        setLength(rawBytes.length);
        rawBytesSet = true;
    }

    /**
     * set the byte array to a string.
     */
    public void setString(String str)
    {
        rawBytes = str.getBytes();
        setLength(rawBytes.length);
        rawBytesSet = true;
    }

    public void setString(String str, String charsetName)
        throws UnsupportedEncodingException
    {
        if (charsetName != null) {
            // Before adding multi-byte support,
            // always use single byte charset here
            rawBytes = str.getBytes(charsetName);
        } else {
            rawBytes = str.getBytes();
        }
        setLength(rawBytes.length);
        rawBytesSet = true;
    }
}
;

// End FennelTupleDatum.java
