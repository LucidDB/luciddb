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

import java.util.HashMap;

/**
 * FennelStandardTypeDescriptor implements the
 * {@link FennelStandardTypeDescriptor} enumerations as kept in fennel.
 *
 * This must be kept in sync with any changes to fennel's
 * FennelStandardTypeDescriptor.h
 *
 * @author Mike Bennett
 * @version $Id$
 */
// NOTE: this would be *SO* much cleaner with jdk 1.5 Enum support!
public final class FennelStandardTypeDescriptor
{
    // NOTE: this is currently not used, but it may be needed for
    // XMI parsing so I'm leaving it for now - mbennett 11jan05
    private static final HashMap ordinals;

    private static void addOrdinal(int ordinalType, String desc)
    {
        ordinals.put(new Integer(ordinalType), desc);
    }

    static {
        ordinals = new HashMap(16);
        addOrdinal(FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_INT_8,     "s1");
        addOrdinal(FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_8,    "u1");
        addOrdinal(FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_INT_16,    "s2");
        addOrdinal(FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_16,   "u2");
        addOrdinal(FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_INT_32,    "s4");
        addOrdinal(FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_32,   "u4");
        addOrdinal(FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_INT_64,    "s8");
        addOrdinal(FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_64,   "u8");
        addOrdinal(FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_BOOL,      "bo");
        addOrdinal(FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_REAL,      "r");
        addOrdinal(FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_DOUBLE,    "d");
        addOrdinal(FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_CHAR,      "c");
        addOrdinal(FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_VARCHAR,   "vc");
        addOrdinal(FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_BINARY,    "b");
        addOrdinal(FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_VARBINARY, "vb");
    }

    /**
     * gets the string descriptor for an ordinal
     */
    public static String getDescription(int st) 
    {
        return (String) ordinals.get(new Integer(st));
    }

    /**
     *  abstract base class for all the numeric types. This is only used
     *  within this module to save typing
     */
    public static abstract class FennelNumericType
        implements FennelStoredTypeDescriptor
    {
        private final int ordinal;
        private final int bitCount;
        private final int fixedByteCount;

        /**
         * returns the ordinal representing this type.
         */
        public int getOrdinal()
        { 
            return ordinal; 
        };

        /**
         * returns number of bits in marshalled representation, or 0 for a
         * non-bit type; currently only 0 or 1 is supported.
         */
        public int getBitCount() 
        { 
            return bitCount; 
        };

        /**
         * returns the width in bytes for a fixed-width non-bit type which
         * admits no per-attribute precision, or 0 for types with per-attribute
         * precision; for bit types, this yields the size of the unmarshalled
         * representation.
         */
        public int getFixedByteCount() 
        { 
            return fixedByteCount; 
        };
        
        /**
         * Gets the number of bytes required to store the narrowest value with
         * this type, given a particular max byte count.  For a fixed-width
         * type, the return value is the same as the input.
         *
         * @param maxWidth maximum width for which to compute the minimum
         *
         * @return number of bytes
         */
        public int getMinByteCount(int maxWidth) 
        { 
            return maxWidth; 
        };

        /**
         * Gets the alignment size in bytes required for values of this type,
         * given a particular max byte count.  This must be 1, 2, 4, or 8,
         * and may not be greater than 1 for variable-width datatypes.
         * For fixed-width datatypes, the width must be a multiple of the
         * alignment size.
         *
         * @param width width for which to compute the alignment
         *
         * @return number of bytes
         */
        public int getAlignmentByteCount(int width)
        { 
            return width; 
        };

        /**
         *  required by the serialization mechanism; should never be used.
         */
        protected FennelNumericType()
        {
            this.ordinal = 0;
            this.bitCount = 0;
            this.fixedByteCount = 0;
        }

        /**
         * Construction.
         */
        public FennelNumericType(int ordinal, int bitCount, int fixedByteCount)
        {
            this.ordinal        = ordinal;
            this.bitCount       = bitCount;
            this.fixedByteCount = fixedByteCount;
        }
    }

    /**
     * describes a signed byte.
     */
    public static class stdINT_8 extends FennelNumericType
        implements java.io.Serializable
    {
        public stdINT_8() { super(
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_INT_8, 0, 1); };
        public FennelAttributeAccessor newAttributeAccessor()
        { 
            return new FennelAttributeAccessor.FennelByteAccessor();
        };
        public int compareValues(FennelTupleDatum d1, FennelTupleDatum d2) {
            return (int) (d2.getByte() - d1.getByte());
        }
    }
    
    /**
     * describes an unsigned byte.
     */
    public static class stdUINT_8 extends FennelNumericType
        implements java.io.Serializable
    {
        public stdUINT_8() { super(
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_8, 0, 1); };
        public FennelAttributeAccessor newAttributeAccessor()
        { 
            return new FennelAttributeAccessor.FennelByteAccessor();
        };
        public int compareValues(FennelTupleDatum d1, FennelTupleDatum d2) {
            return (int) (d2.getUnsignedByte() - d1.getUnsignedByte());
        }
    }

    /**
     * describes a signed short.
     */
    public static class stdINT_16 extends FennelNumericType
        implements java.io.Serializable
    {
        public stdINT_16() { super(
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_INT_16, 0, 2); };
        public FennelAttributeAccessor newAttributeAccessor()
        { 
            return new FennelAttributeAccessor.FennelShortAccessor();
        };
        public int compareValues(FennelTupleDatum d1, FennelTupleDatum d2) {
            return (int) (d2.getShort() - d1.getShort());
        }
    }

    /**
     * describes an unsigned short.
     */
    public static class stdUINT_16 extends FennelNumericType
        implements java.io.Serializable
    {
        public stdUINT_16() { super(
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_16,
            0,
            2); };
        public FennelAttributeAccessor newAttributeAccessor()
        { 
            return new FennelAttributeAccessor.FennelShortAccessor();
        };
        public int compareValues(FennelTupleDatum d1, FennelTupleDatum d2) {
            return d2.getUnsignedShort() - d1.getUnsignedShort();
        }
    }

    /**
     * describes a signed int.
     */
    public static class stdINT_32 extends FennelNumericType
        implements java.io.Serializable
    {
        public stdINT_32() { super(
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_INT_32, 0, 4); };
        public FennelAttributeAccessor newAttributeAccessor()
        { 
            return new FennelAttributeAccessor.FennelIntAccessor();
        };
        public int compareValues(FennelTupleDatum d1, FennelTupleDatum d2) {
            return d2.getInt() - d1.getInt();
        }
    }

    /**
     * describes an unsigned int.
     */
    public static class stdUINT_32 extends FennelNumericType
        implements java.io.Serializable
    {
        public stdUINT_32() { super(
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_32,
            0,
            4); };
        public FennelAttributeAccessor newAttributeAccessor()
        { 
            return new FennelAttributeAccessor.FennelIntAccessor();
        };
        public int compareValues(FennelTupleDatum d1, FennelTupleDatum d2) {
            return (int) (d2.getUnsignedInt() - d1.getUnsignedInt());
        }
    }

    /**
     * describes a signed long.
     */
    public static class stdINT_64 extends FennelNumericType
        implements java.io.Serializable
    {
        public stdINT_64() { super(
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_INT_64, 0, 8); };
        public FennelAttributeAccessor newAttributeAccessor()
        { 
            return new FennelAttributeAccessor.FennelLongAccessor();
        };
        public int compareValues(FennelTupleDatum d1, FennelTupleDatum d2) {
            return (int) (d2.getLong() - d1.getLong());
        }
    }

    /**
     * describes an unsigned long.
     */
    public static class stdUINT_64 extends FennelNumericType
        implements java.io.Serializable
    {
        public stdUINT_64() { super(
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_64,
            0,
            8); };
        public FennelAttributeAccessor newAttributeAccessor()
        { 
            return new FennelAttributeAccessor.FennelLongAccessor();
        };
        public int compareValues(FennelTupleDatum d1, FennelTupleDatum d2) {
            return (int) (d2.getUnsignedLong() - d1.getUnsignedLong());
        }
    }

    /**
     * describes a float.
     */
    public static class stdREAL extends FennelNumericType
        implements java.io.Serializable
    {
        public stdREAL() { super(
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_REAL, 0, 4); };
        public FennelAttributeAccessor newAttributeAccessor()
        { 
            return new FennelAttributeAccessor.FennelIntAccessor();
        };
        public int compareValues(FennelTupleDatum d1, FennelTupleDatum d2) {
            return (int) (d2.getFloat() - d1.getFloat());
        }
    }

    /**
     * describes a double.
     */
    public static class stdDOUBLE extends FennelNumericType
        implements java.io.Serializable
    {
        public stdDOUBLE() { super(
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_DOUBLE, 0, 8); };
        public FennelAttributeAccessor newAttributeAccessor()
        { 
            return new FennelAttributeAccessor.FennelLongAccessor();
        };
        public int compareValues(FennelTupleDatum d1, FennelTupleDatum d2) {
            return (int) (d2.getDouble() - d1.getDouble());
        }
    }

    /**
     * describes a boolean.
     */
    public static class stdBOOL extends FennelNumericType
        implements java.io.Serializable
    {
        public stdBOOL() { super(
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_BOOL, 1, 1); };
        public FennelAttributeAccessor newAttributeAccessor()
        { 
            return new FennelAttributeAccessor.FennelBitAccessor();
        };
        public int compareValues(FennelTupleDatum d1, FennelTupleDatum d2) {
            if (d2.getBoolean() == d1.getBoolean()) { 
                return 0;
            }
            if (d2.getBoolean()) {
                return 1;
            } 
            return -1;
        }
    }

    /**
     * describes a fixed width char array.
     */
    public static class stdCHAR
        implements FennelStoredTypeDescriptor, java.io.Serializable
    {
        public int getOrdinal() {
            return FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_CHAR; }
        public int getBitCount() { return 0; }
        public int getFixedBitCount() { return 0; }
        public int getFixedByteCount() { return 0; }
        public int getMinByteCount(int maxWidth) { return maxWidth; }
        public int getAlignmentByteCount(int width) { return 1; }
        public FennelAttributeAccessor newAttributeAccessor()
        { 
            return new FennelAttributeAccessor.FennelFixedWidthAccessor();
        }
        public int compareValues(FennelTupleDatum d1, FennelTupleDatum d2) {
            int c = d2.getLength() - d1.getLength();
            if (c != 0) {
                return c;
            }
            if (new String(d1.getBytes(), 0, c).equals(
                new String(d2.getBytes(), 0, c))) {
                return 0;
            }
            // arbitrarily mark the first one larger; if this is actually
            // used, compare hashcodes
            return -1;
        }
    }

    /**
     * describes a variable width char array.
     */
    public static class stdVARCHAR implements FennelStoredTypeDescriptor, java.io.Serializable
    {
        public int getOrdinal() {
            return FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_VARCHAR; }
        public int getBitCount() { return 0; }
        public int getFixedBitCount() { return 0; }
        public int getFixedByteCount() { return 0; }
        public int getMinByteCount(int maxWidth) { return 0; }
        public int getAlignmentByteCount(int width) { return 1; }
        public FennelAttributeAccessor newAttributeAccessor()
        { 
            return new FennelAttributeAccessor.FennelVarWidthAccessor();
        }
        public int compareValues(FennelTupleDatum d1, FennelTupleDatum d2) {
            int c = d2.getLength() - d1.getLength();
            if (c != 0) {
                return c;
            }
            if (new String(d1.getBytes(), 0, c).equals(
                new String(d2.getBytes(), 0, c))) {
                return 0;
            }
            // arbitrarily mark the first one larger; if this is actually
            // used, compare hashcodes
            return -1;
        }
    }

    /**
     * describes a fixed width binary array.
     */
    public static class stdBINARY
        implements FennelStoredTypeDescriptor, java.io.Serializable
    {
        public int getOrdinal() {
            return FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_BINARY; }
        public int getBitCount() { return 0; }
        public int getFixedBitCount() { return 0; }
        public int getFixedByteCount() { return 0; }
        public int getMinByteCount(int maxWidth) { return maxWidth; }
        public int getAlignmentByteCount(int width) { return 1; }
        public FennelAttributeAccessor newAttributeAccessor()
        { 
            return new FennelAttributeAccessor.FennelFixedWidthAccessor();
        }
        public int compareValues(FennelTupleDatum d1, FennelTupleDatum d2) {
            int c = d2.getLength() - d1.getLength();
            if (c != 0) {
                return c;
            }
            if (new String(d1.getBytes(), 0, c).equals(
                new String(d2.getBytes(), 0, c))) {
                return 0;
            }
            // arbitrarily mark the first one larger; if this is actually
            // used, compare hashcodes
            return -1;
        }
    }

    /**
     * describes a variable width binary array
     */
    public static class stdVARBINARY
        implements FennelStoredTypeDescriptor, java.io.Serializable
    {
        public int getOrdinal() {
            return FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_VARBINARY;
        }
        public int getBitCount() { return 0; }
        public int getFixedBitCount() { return 0; }
        public int getFixedByteCount() { return 0; }
        public int getMinByteCount(int maxWidth) { return 0; }
        public int getAlignmentByteCount(int width) { return 1; }
        public FennelAttributeAccessor newAttributeAccessor()
        { 
            return new FennelAttributeAccessor.FennelVarWidthAccessor();
        }
        public int compareValues(FennelTupleDatum d1, FennelTupleDatum d2) {
            int c = d2.getLength() - d1.getLength();
            if (c != 0) {
                return c;
            }
            if (new String(d1.getBytes(), 0, c).equals(
                new String(d2.getBytes(), 0, c))) {
                return 0;
            }
            // arbitrarily mark the first one larger; if this is actually
            // used, compare hashcodes
            return -1;
        }
    }
};

// End FennelStandardTypeDescriptor.java
