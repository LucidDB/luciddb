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

import java.lang.reflect.*;

import java.nio.*;

import java.sql.*;

import java.util.*;

import org.eigenbase.runtime.*;
import org.eigenbase.util.*;


/**
 * FarragoSyntheticObject refines SyntheticObject with Farrago-specific runtime
 * information such as null values.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoSyntheticObject
    extends SyntheticObject
    implements Struct
{
    //~ Instance fields --------------------------------------------------------

    /**
     * Array of BitReferences to be used by marshal/unmarshal.
     *
     * <p>TODO: assert somewhere that position in this array corresponds to
     * FemTupleAccessor info.</p>
     */
    private BitReference [] bitReferences;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoSyntheticObject object.
     */
    protected FarragoSyntheticObject()
    {
        initFields();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Marshals all bit fields in this tuple.
     *
     * @param byteBuffer destination ByteBuffer
     * @param bitFieldByteOffset absolute offset of first byte of bit fields in
     * ByteBuffer
     */
    public void marshalBitFields(
        ByteBuffer byteBuffer,
        int bitFieldByteOffset)
    {
        if (bitReferences == null) {
            indexBitFields();
        }
        if (bitReferences.length == 0) {
            return;
        }
        byte oneByte = 0;
        byte bitMask = 1;
        int nBitsLeftInByte = 8;
        for (int i = 0; i < bitReferences.length; ++i) {
            if (nBitsLeftInByte == 0) {
                byteBuffer.put(bitFieldByteOffset, oneByte);
                ++bitFieldByteOffset;
                nBitsLeftInByte = 8;
                oneByte = 0;
                bitMask = 1;
            }
            --nBitsLeftInByte;
            if (bitReferences[i].getBit()) {
                oneByte |= bitMask;
            }
            bitMask <<= 1;
        }

        // flush last byte
        byteBuffer.put(bitFieldByteOffset, oneByte);
    }

    /**
     * Unmarshals all bit fields in this tuple.
     *
     * @param byteBuffer source ByteBuffer
     * @param bitFieldByteOffset absolute offset of first byte of bit fields in
     * ByteBuffer
     */
    public void unmarshalBitFields(
        ByteBuffer byteBuffer,
        int bitFieldByteOffset)
    {
        if (bitReferences == null) {
            indexBitFields();
        }
        byte oneByte = 0;
        int nBitsLeftInByte = 0;
        for (int i = 0; i < bitReferences.length; ++i) {
            if (nBitsLeftInByte == 0) {
                oneByte = byteBuffer.get(bitFieldByteOffset);
                ++bitFieldByteOffset;
                nBitsLeftInByte = 8;
            }
            --nBitsLeftInByte;
            boolean bit = ((oneByte & 1) == 1);
            oneByte >>= 1;
            bitReferences[i].setBit(bit);
        }
    }

    /**
     * Constructs an array of BitReferences for use in marshalling and
     * unmarshalling bit fields. Note that this can't be done at construction
     * time, because subclass constructors may change field references. Instead,
     * we do it lazily.
     */
    private void indexBitFields()
    {
        try {
            Field [] fields = getFields();
            List<BitReference> bitReferenceList = new ArrayList<BitReference>();
            for (int i = 0; i < fields.length; ++i) {
                Field field = fields[i];
                Object obj = field.get(this);

                // NOTE:  order has to match Fennel's TupleAccessor.cpp
                if (obj instanceof NullablePrimitive.NullableBoolean) {
                    // add this field's holder object as a bit value
                    bitReferenceList.add(
                        (NullablePrimitive.NullableBoolean) obj);
                } else if (obj instanceof Boolean) {
                    // make up a reflective reference to this field
                    // as a bit value
                    BooleanBitReference bitRef = new BooleanBitReference();
                    bitRef.field = field;
                    bitReferenceList.add(bitRef);
                }
                if (obj instanceof NullableValue) {
                    // add this field's holder object as a null indicator
                    NullIndicatorBitReference bitRef =
                        new NullIndicatorBitReference();
                    bitRef.nullableValue = (NullableValue) obj;
                    bitReferenceList.add(bitRef);
                }
            }
            bitReferences =
                bitReferenceList.toArray(
                    new BitReference[bitReferenceList.size()]);
        } catch (Exception ex) {
            throw Util.newInternal(ex);
        }
    }

    /**
     * Uses reflection to construct instances of all non-primitive fields.
     */
    private void initFields()
    {
        try {
            Field [] fields = getFields();
            for (int i = 0; i < fields.length; ++i) {
                Field field = fields[i];
                Class<?> clazz = field.getType();
                if (!clazz.isPrimitive()) {
                    Object obj = clazz.newInstance();
                    field.set(this, obj);
                }
            }
        } catch (Exception ex) {
            throw Util.newInternal(ex);
        }
    }

    // implement Struct
    public Object [] getAttributes()
    {
        int n = getFields().length;
        Object [] objs = new Object[n];
        for (int i = 0; i < n; ++i) {
            Object obj = getFieldValue(i);
            if (obj instanceof DataValue) {
                obj = ((DataValue) obj).getNullableData();
            }
            objs[i] = obj;
        }
        return objs;
    }

    // implement Struct
    public Object [] getAttributes(Map<String, Class<?>> map)
    {
        throw new UnsupportedOperationException();
    }

    // implement Struct
    public String getSQLTypeName()
        throws SQLException
    {
        // TODO jvs 24-Mar-2005:  Need to burn this into generated subclass
        // for UDT's.
        return "ROW";
    }

    // implement Object
    public String toString()
    {
        // TODO jvs 24-Mar-2005:  check standard for rules on casting
        // struct to string
        Object [] objs = getAttributes();
        return Arrays.asList(objs).toString();
    }

    /**
     * Called at runtime to implement the {@link
     * org.eigenbase.sql.fun.SqlStdOperatorTable#isDifferentFromOperator}
     * operator in a row-size fashion.
     *
     * @param row1 first row to compare
     * @param row2 second row to compare (must be of exact same type as row1)
     *
     * @return whether row1 differs from row2 according to the definition of
     * $IS_DIFFERENT_FROM
     */
    public static boolean testIsDifferentFrom(
        FarragoSyntheticObject row1,
        FarragoSyntheticObject row2)
    {
        assert (row1.getClass() == row2.getClass());
        Object [] vals1 = row1.getAttributes();
        Object [] vals2 = row2.getAttributes();
        assert (vals1.length == vals2.length);
        for (int i = 0; i < vals1.length; ++i) {
            Object val1 = vals1[i];
            Object val2 = vals2[i];
            if ((val1 == null) != (val2 == null)) {
                // one is NULL but the other is not
                return true;
            }

            // fast object identity test also handles case of both NULL
            if (val1 != val2) {
                // Because types are identical, we can use equals
                if (!val1.equals(val2)) {
                    return true;
                }
            }
        }
        return false;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Implementation of BitReference for accessing a null indicator.
     */
    private static class NullIndicatorBitReference
        implements BitReference
    {
        NullableValue nullableValue;

        // implement BitReference
        public void setBit(boolean bit)
        {
            nullableValue.setNull(bit);
        }

        public boolean getBit()
        {
            return nullableValue.isNull();
        }
    }

    /**
     * Implementation of BitReference for accessing a NOT NULL boolean field.
     */
    private class BooleanBitReference
        implements BitReference
    {
        Field field;

        // implement BitReference
        public void setBit(boolean bit)
        {
            try {
                field.setBoolean(FarragoSyntheticObject.this, bit);
            } catch (IllegalAccessException ex) {
                throw Util.newInternal(ex);
            }
        }

        public boolean getBit()
        {
            try {
                return field.getBoolean(FarragoSyntheticObject.this);
            } catch (IllegalAccessException ex) {
                throw Util.newInternal(ex);
            }
        }
    }
}

// End FarragoSyntheticObject.java
