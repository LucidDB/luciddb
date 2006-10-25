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
package net.sf.farrago.fennel.tuple;

import java.nio.*;


/**
 * FennelAttributeAccessor defines how to efficiently unmarshal the value of an
 * attribute from a stored tuple. Derived classes implement various strategies
 * depending on the storage type.
 *
 * <p>All data members are defined at this level rather than in derived classes,
 * which only provide method implementations. This makes it possible to write
 * non-polymorphic access code in cases where the entire tuple is being
 * processed, but polymorphic access code in cases where only a small subset of
 * the attributes are being processed. In theory, this hybrid should yield the
 * highest efficiency, but it needs to be benchmarked and tuned.
 *
 * This class is JDK 1.4 compatible.
 */
public abstract class FennelAttributeAccessor
{

    //~ Instance fields --------------------------------------------------------

    /**
     * Index of this attribute's null indicator bit in the tuple's bit array, or
     * Integer.MAX_VALUE for a NOT NULL attribute.
     */
    public int nullBitNdx;

    /**
     * Byte offset of this attribute within a stored tuple image, or
     * Integer.MAX_VALUE if the start is variable.
     */
    public int fixedOffset;

    /**
     * Indirect offset of the end of this attribute within a stored tuple image,
     * or Integer.MAX_VALUE if the end is fixed.
     */
    public int endIndirectOffset;

    /**
     * Index of this attribute's value in the tuple's bit array, or
     * Integer.MAX_VALUE for a non-bit attribute. NOTE: this is only used for
     * booleans
     */
    public int valueBitNdx;

    /**
     * Copied from FennelTupleAttributeDescriptor.storageSize. This is not used
     * for anything except assertions.
     */
    public int capacity;

    /**
     * pre-computed offsets and masks for testing bit fields.
     */
    protected int valueBitOffset;

    /**
     * pre-computed offsets and masks for testing bit fields.
     */
    protected byte valueBitMask;

    /**
     * pre-computed offsets and masks for testing bit fields.
     */
    protected int nullableBitOffset;

    /**
     * pre-computed offsets and masks for testing bit fields.
     */
    protected byte nullableBitMask;

    //~ Constructors -----------------------------------------------------------

    /*
     * default constructor.
     */
    protected FennelAttributeAccessor()
    {
        nullBitNdx = Integer.MAX_VALUE;
        fixedOffset = Integer.MAX_VALUE;
        endIndirectOffset = Integer.MAX_VALUE;
        valueBitNdx = Integer.MAX_VALUE;
        capacity = 0;
        valueBitOffset = Integer.MAX_VALUE;
        nullableBitOffset = Integer.MAX_VALUE;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * computes bit accessors.
     */
    public void computeBitAccessors(int bitfieldOffset)
    {
        if (valueBitNdx != Integer.MAX_VALUE) {
            valueBitOffset = bitfieldOffset + (valueBitNdx / 8);
            valueBitMask = (byte) (0x01 << (valueBitNdx % 8));
        }
        if (nullBitNdx != Integer.MAX_VALUE) {
            nullableBitOffset = bitfieldOffset + (nullBitNdx / 8);
            nullableBitMask = (byte) (0x01 << (nullBitNdx % 8));
        }
    }

    /**
     * tests if a field is present in a tuple buffer.
     */
    public boolean isPresent(ByteBuffer buf)
    {
        // if it's not nullable, it's always there
        if (nullBitNdx == Integer.MAX_VALUE) {
            return true;
        }
        byte val = buf.get(nullableBitOffset);
        if ((val & nullableBitMask) != 0) {
            return false;
        }
        return true;
    }

    /**
     * sets the presence of a field in a tuple buffer.
     */
    public void setPresent(ByteBuffer buf, boolean isPresent)
    {
        // if it's not nullable, it's always there
        if (nullBitNdx == Integer.MAX_VALUE) {
            // perhaps this should be an assert, but it's possible to call it
            // from code we don't control
            //assert(false);
            return;
        }
        byte val = buf.get(nullableBitOffset);
        if (isPresent) {
            val &= ~(nullableBitMask);
        } else {
            val |= nullableBitMask;
        }
        buf.put(nullableBitOffset, val);
    }

    /**
     * Unmarshalls the attribute's value into a tupledatum holder. Unlike the
     * C++ version, this *does* actually copy data.
     *
     * @param tupleAccessor containing FennelTupleAccessor set up with the
     * current tuple image to be accessed
     * @param value receives the reference to the unmarshalled value
     */
    public abstract void unmarshalValue(
        FennelTupleAccessor tupleAccessor,
        FennelTupleDatum value);

    /**
     * Marshalls value data for the attribute. Only deals with the data bytes,
     * not length and null indicators.
     *
     * @param pDestData the target address where the data should be marshalled
     * @param value the value to be marshalled
     */
    public abstract void marshalValueData(
        ByteBuffer pDestData,
        FennelTupleDatum value);

    //~ Inner Classes ----------------------------------------------------------

    /**
     * marshalls fixed-width byte arrays.
     */
    public static final class FennelFixedWidthAccessor
        extends FennelAttributeAccessor
    {
        public void marshalValueData(
            ByteBuffer pDestData,
            FennelTupleDatum value)
            throws NullPointerException
        {
            /*
            System.out.println("FixedAccessor - marshalling - offset " +
             fixedOffset +                 ", len " + capacity);
             */
            pDestData.position(fixedOffset);
            pDestData.put(
                value.getBytes(),
                0,
                value.getLength());
        }

        public void unmarshalValue(
            FennelTupleAccessor tupleAccessor,
            FennelTupleDatum value)
        {
            /*
            System.out.println("FixedAccessor - marshalling - offset " +
             fixedOffset +                 ", len " + capacity);
             */
            tupleAccessor.getCurrentTupleBuf().position(fixedOffset);
            value.setLength(value.getCapacity());
            tupleAccessor.getCurrentTupleBuf().get(
                value.setRawBytes(),
                0,
                value.getLength());
        }
    }

    /**
     * marshalls a numeric byte (signed or unsigned)
     */
    public static final class FennelByteAccessor
        extends FennelAttributeAccessor
    {
        public void marshalValueData(
            ByteBuffer pDestData,
            FennelTupleDatum value)
            throws NullPointerException
        {
            /*
            System.out.println("FennelByteAccessor - marshalling - offset " +
             fixedOffset +                ", len " + capacity + " at position "
             + pDestData.position());
             */
            pDestData.position(fixedOffset);
            pDestData.put(value.getByte());
        }

        public void unmarshalValue(
            FennelTupleAccessor tupleAccessor,
            FennelTupleDatum value)
        {
            /*
            System.out.println("FennelByteAccessor - unmarshalling - offset " +
             fixedOffset +                ", len " + capacity);
             */
            tupleAccessor.getCurrentTupleBuf().position(fixedOffset);
            value.setByte(tupleAccessor.getCurrentTupleBuf().get());
        }
    }

    /**
     * marshalls a numeric short (signed or unsigned)
     */
    public static final class FennelShortAccessor
        extends FennelAttributeAccessor
    {
        public void marshalValueData(
            ByteBuffer pDestData,
            FennelTupleDatum value)
            throws NullPointerException
        {
            /*
            System.out.println("FennelShortAccessor - marshalling - offset " +
             fixedOffset +                ", len " + capacity + " at position "
             + pDestData.position());
             */
            pDestData.putShort(
                fixedOffset,
                value.getShort());
        }

        public void unmarshalValue(
            FennelTupleAccessor tupleAccessor,
            FennelTupleDatum value)
        {
            /*
            System.out.println("FennelShortAccessor - unmarshalling - offset " +
             fixedOffset +                ", len " + capacity);
             */
            value.setShort(
                tupleAccessor.getCurrentTupleBuf().getShort(fixedOffset));
        }
    }

    /**
     * marshalls a numeric int (signed or unsigned)
     */
    public static class FennelIntAccessor
        extends FennelAttributeAccessor
    {
        public void marshalValueData(
            ByteBuffer pDestData,
            FennelTupleDatum value)
            throws NullPointerException
        {
            /*
            System.out.println("FennelIntAccessor - marshalling - offset " +
             fixedOffset +                ", len " + capacity + " at position "
             + pDestData.position());
             */
            pDestData.putInt(
                fixedOffset,
                value.getInt());
        }

        public void unmarshalValue(
            FennelTupleAccessor tupleAccessor,
            FennelTupleDatum value)
        {
            /*
            System.out.println("FennelIntAccessor - unmarshalling - offset " +
             fixedOffset +                ", len " + capacity);
             */
            value.setInt(
                tupleAccessor.getCurrentTupleBuf().getInt(fixedOffset));
        }
    }

    /**
     * marshalls a numeric long (signed or unsigned)
     */
    public static final class FennelLongAccessor
        extends FennelAttributeAccessor
    {
        public void marshalValueData(
            ByteBuffer pDestData,
            FennelTupleDatum value)
            throws NullPointerException
        {
            /*
            System.out.println("FennelLongAccessor - marshalling - offset " +
             fixedOffset +                ", len " + capacity + " at position "
             + pDestData.position());
             */
            pDestData.putLong(
                fixedOffset,
                value.getLong());
        }

        public void unmarshalValue(
            FennelTupleAccessor tupleAccessor,
            FennelTupleDatum value)
        {
            /*
            System.out.println("FennelLongAccessor - unmarshalling - offset " +
             fixedOffset +                ", len " + capacity);
             */
            value.setLong(
                tupleAccessor.getCurrentTupleBuf().getLong(fixedOffset));
        }
    }

    /**
     * marshalls a numeric bit
     */
    public static final class FennelBitAccessor
        extends FennelAttributeAccessor
    {
        public void marshalValueData(
            ByteBuffer pDestData,
            FennelTupleDatum value)
            throws NullPointerException
        {
            /*
            System.out.println("FennelBitAccessor - marshalling - bitoffset " +
             valueBitOffset +                ", mask " + valueBitMask);
             */
            byte val = pDestData.get(valueBitOffset);
            if (value.getBoolean()) {
                val |= valueBitMask;
            } else {
                val &= ~(valueBitMask);
            }
            pDestData.put(valueBitOffset, val);
        }

        public void unmarshalValue(
            FennelTupleAccessor tupleAccessor,
            FennelTupleDatum value)
        {
            /*
            System.out.println("FennelBitAccessor - unmarshalling - bitoffset "
             + valueBitOffset +                ", mask " + valueBitMask);
             */
            byte val = tupleAccessor.getCurrentTupleBuf().get(valueBitOffset);
            if ((val & valueBitMask) != 0) {
                value.setBoolean(true);
            } else {
                value.setBoolean(false);
            }
        }
    }

    /**
     * marshalls any variable width accessor, either first or subsequent.
     *
     * <p>The first variable width accessor has special handling; it is
     * indicated by having its fixedOffset set to a value other than the default
     * (Integer.MAX_VALUE)
     */
    public static final class FennelVarWidthAccessor
        extends FennelAttributeAccessor
    {
        public void marshalValueData(
            ByteBuffer pDestData,
            FennelTupleDatum value)
            throws NullPointerException
        {
            /*
            System.out.println("FennelVarWidthAccessor - marshalling " +
             value.getLength() +                 " bytes into buffer at position
             " + pDestData.position() +                 ", fixedOffset is " +
             fixedOffset);
             */
            // we assume the position of the bytebuffer has already been set
            pDestData.put(
                value.getBytes(),
                0,
                value.getLength());
        }

        public void unmarshalValue(
            FennelTupleAccessor tupleAccessor,
            FennelTupleDatum value)
        {
            ByteBuffer srcBuf = tupleAccessor.getCurrentTupleBuf();

            int offset;
            int endOffset;
            if (fixedOffset != Integer.MAX_VALUE) {
                // this is the first varwidth buffer
                offset = fixedOffset;
                endOffset =
                    FennelTupleAccessor.readUnsignedShort(
                        srcBuf,
                        endIndirectOffset);
            } else {
                offset =
                    FennelTupleAccessor.readUnsignedShort(
                        srcBuf,
                        endIndirectOffset - 2);
                endOffset =
                    FennelTupleAccessor.readUnsignedShort(
                        srcBuf,
                        endIndirectOffset);
            }
            assert (value.getCapacity() >= (endOffset - offset));
            value.setLength(endOffset - offset);

            /*
            System.out.println("FennelVarWidthAccessor - unmarshalling " +
             value.getLength() + " bytes from offset " + offset + ", length " +
             value.getLength());
             */
            srcBuf.position(offset);
            srcBuf.get(
                value.setRawBytes(),
                0,
                value.getLength());
        }
    }
}
;

// End FennelAttributeAccessor.java
