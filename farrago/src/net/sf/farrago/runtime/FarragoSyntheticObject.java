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

import net.sf.farrago.type.runtime.*;

import net.sf.saffron.runtime.*;
import net.sf.saffron.util.*;

import java.lang.reflect.*;

import java.nio.*;

import java.util.*;


/**
 * FarragoSyntheticObject refines Saffron's SyntheticObject with
 * Farrago-specific runtime information such as null values.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoSyntheticObject extends SyntheticObject
{
    //~ Instance fields -------------------------------------------------------

    /**
     * Array of BitReferences to be used by marshal/unmarshal.
     * 
     * <p>
     * TODO:  assert somewhere that position in this array corresponds to
     * FemTupleAccessor info.
     * </p>
     */
    private BitReference [] bitReferences;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoSyntheticObject object.
     */
    protected FarragoSyntheticObject()
    {
        initFields();
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Marshal all bit fields in this tuple.
     *
     * @param byteBuffer destination ByteBuffer
     * @param bitFieldByteOffset absolute offset of first byte of bit fields in
     *        ByteBuffer
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
                byteBuffer.put(bitFieldByteOffset,oneByte);
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
        byteBuffer.put(bitFieldByteOffset,oneByte);
    }

    /**
     * Unmarshal all bit fields in this tuple.
     *
     * @param byteBuffer source ByteBuffer
     * @param bitFieldByteOffset absolute offset of first byte of bit fields in
     *        ByteBuffer
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
     * Construct an array of BitReferences for use in marshalling and
     * unmarshalling bit fields.  Note that this can't be done at
     * construction time, because subclass constructors may change field
     * references.  Instead, we do it lazily.
     */
    private void indexBitFields()
    {
        try {
            Field [] fields = getFields();
            List bitReferenceList = new ArrayList();
            for (int i = 0; i < fields.length; ++i) {
                Field field = fields[i];
                Object obj = field.get(this);
                // NOTE:  order has to match Fennel's TupleAccessor.cpp
                if (obj instanceof NullablePrimitive.NullableBoolean) {
                    // add this field's holder object as a bit value
                    bitReferenceList.add(obj);
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
            bitReferences = new BitReference[bitReferenceList.size()];
            bitReferenceList.toArray(bitReferences);
        } catch (Exception ex) {
            throw Util.newInternal(ex);
        }
    }

    /**
     * Use reflection to construct instances of all non-primitive fields.
     */
    private void initFields()
    {
        try {
            Field [] fields = getFields();
            for (int i = 0; i < fields.length; ++i) {
                Field field = fields[i];
                Class clazz = field.getType();
                if (!clazz.isPrimitive()) {
                    Object obj = clazz.newInstance();
                    field.set(this,obj);
                }
            }
        } catch (Exception ex) {
            throw Util.newInternal(ex);
        }
    }

    /**
     * Implementation of BitReference for accessing a null indicator.
     */
    private static class NullIndicatorBitReference implements BitReference
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
    private class BooleanBitReference implements BitReference
    {
        Field field;

        // implement BitReference
        public void setBit(boolean bit)
        {
            try {
                field.setBoolean(FarragoSyntheticObject.this,bit);
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
