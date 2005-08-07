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

import org.eigenbase.util.EnumeratedValues;

import java.io.Serializable;

/**
 * FennelStandardTypeDescriptor implements the
 * {@link FennelStandardTypeDescriptor} enumerations as kept in fennel.
 *
 * This must be kept in sync with any changes to fennel's
 * <code>FennelStandardTypeDescriptor.h</code>.
 *
 * @author Mike Bennett
 * @version $Id$
 */
public abstract class FennelStandardTypeDescriptor
    extends EnumeratedValues.BasicValue
    implements FennelStoredTypeDescriptor
{
    public static final int MIN_ORDINAL = 1;
    public static final int INT_8_ORDINAL = 1;
    public static final int UINT_8_ORDINAL = 2;
    public static final int INT_16_ORDINAL = 3;
    public static final int UINT_16_ORDINAL = 4;
    public static final int INT_32_ORDINAL = 5;
    public static final int UINT_32_ORDINAL = 6;
    public static final int INT_64_ORDINAL = 7;
    public static final int UINT_64_ORDINAL = 8;
    public static final int BOOL_ORDINAL = 9;
    public static final int REAL_ORDINAL = 10;
    public static final int DOUBLE_ORDINAL = 11;
    public static final int CHAR_ORDINAL = 12;
    public static final int VARCHAR_ORDINAL = 13;
    public static final int BINARY_ORDINAL = 14;
    public static final int VARBINARY_ORDINAL = 15;
    public static final int EXTENSION_MIN_ORDINAL = 1000;

    /**
     * Describes a signed byte.
     */
    public static final Type_INT_8 INT_8 = new Type_INT_8();

    /**
     * Describes an unsigned signed byte.
     */
    public static final Type_UINT_8 UINT_8 = new Type_UINT_8();

    /**
     * Describes a signed short.
     */
    public static final Type_INT_16 INT_16 = new Type_INT_16();

    /**
     * Describes an unsigned short.
     */
    public static final Type_UINT_16 UINT_16 = new Type_UINT_16();

    /**
     * Describes a signed int.
     */
    public static final Type_INT_32 INT_32 = new Type_INT_32();

    /**
     * Describes an unsigned int.
     */
    public static final Type_UINT_32 UINT_32 = new Type_UINT_32();

    /**
     * Describes a signed long.
     */
    public static final Type_INT_64 INT_64 = new Type_INT_64();

    /**
     * Describes an unsigned long.
     */
    public static final Type_UINT_64 UINT_64 = new Type_UINT_64();

    /**
     * Describes a boolean.
     */
    public static final Type_BOOL BOOL = new Type_BOOL();

    /**
     * Describes a float.
     */
    public static final Type_REAL REAL = new Type_REAL();

    /**
     * Describes a double.
     */
    public static final Type_DOUBLE DOUBLE = new Type_DOUBLE();

    /**
     * Describes a fixed-width character string.
     */
    public static final Type_CHAR CHAR = new Type_CHAR();

    /**
     * Describes a variable-width character string.
     */
    public static final Type_VARCHAR VARCHAR = new Type_VARCHAR();

    /**
     * Describes a fixed-width binary string.
     */
    public static final Type_BINARY BINARY = new Type_BINARY();

    /**
     * Describes a variable-width binary string.
     */
    public static final Type_VARBINARY VARBINARY = new Type_VARBINARY();

    private static final FennelStandardTypeDescriptor[] values = {
        INT_8,
        UINT_8,
        INT_16,
        UINT_16,
        INT_32,
        UINT_32,
        INT_64,
        UINT_64,
        BOOL,
        REAL,
        DOUBLE,
        CHAR,
        VARCHAR,
        BINARY,
        VARBINARY,
    };

    public static final EnumeratedValues enumeration =
        new EnumeratedValues(values);

    private FennelStandardTypeDescriptor(String name, int ordinal)
    {
        super(name, ordinal, null);
    }

    /**
     * Returns the {@link FennelStandardTypeDescriptor} with a given name.
     */
    public static FennelStandardTypeDescriptor get(String name)
    {
        return (FennelStandardTypeDescriptor) enumeration.getValue(name);
    }

    /**
     * Returns the {@link FennelStandardTypeDescriptor} with a given ordinal.
     */
    public static FennelStandardTypeDescriptor forOrdinal(int ordinal)
    {
        return (FennelStandardTypeDescriptor) enumeration.getValue(ordinal);
    }

    /**
     * Returns whether this type is numeric.
     */
    public abstract boolean isNumeric();

    /**
     * Returns whether this is primitive type.
     */
    public boolean isNative()
    {
        return getOrdinal() < DOUBLE_ORDINAL;
    }

    /**
     * Returns whether this ordinal represents a primitive non-boolean type.
     */
    public boolean isNativeNotBool()
    {
        return getOrdinal() <= DOUBLE_ORDINAL &&
            getOrdinal() != BOOL_ORDINAL;
    }

    /**
     * Returns whether this ordinal represents an integral native type.
     */
    public boolean isIntegralNative(int st)
    {
        if (getOrdinal() <= BOOL_ORDINAL) {
            return true;
        }
        return false;
    }

    /**
     * Returns whether this ordinal is an exact numeric.
     */
    public boolean isExact()
    {
        if (getOrdinal() <= UINT_64_ORDINAL) {
            return true;
        }
        return false;
    }

    /**
     * Returns whether this ordinal is an approximate numeric.
     */
    public boolean isApprox()
    {
        if (getOrdinal() == REAL_ORDINAL ||
            getOrdinal() == DOUBLE_ORDINAL) {
            return true;
        }
        return false;
    }

    /**
     * Returns whether this ordinal represents an array.
     */
    public boolean isArray()
    {
        if (getOrdinal() >= CHAR_ORDINAL &&
            getOrdinal() <= VARBINARY_ORDINAL) {
            return true;
        }
        return false;
    }

    /**
     * Returns whether this ordinal represents a variable length array.
     */
    public boolean isVariableLenArray()
    {
        if (getOrdinal() == VARCHAR_ORDINAL ||
            getOrdinal() == VARBINARY_ORDINAL) {
            return true;
        }
        return false;
    }

    /**
     * Returns whether this ordinal represents a fixed length array.
     */
    public boolean isFixedLenArray()
    {
        if (getOrdinal() == CHAR_ORDINAL ||
            getOrdinal() == BINARY_ORDINAL) {
            return true;
        }
        return false;
    }

    /**
     * Returns whether this ordinal represents a text array.
     */
    public boolean isTextArray()
    {
        if (getOrdinal() == CHAR_ORDINAL ||
            getOrdinal() == VARCHAR_ORDINAL) {
            return true;
        }
        return false;
    }

    /**
     * Returns whether this ordinal represent a binary array.
     */
    public boolean isBinaryArray()
    {
        if (getOrdinal() == VARBINARY_ORDINAL ||
            getOrdinal() == BINARY_ORDINAL) {
            return true;
        }
        return false;
    }

    /**
     * Abstract base class for all types.
     */
    private static abstract class FennelType
        extends FennelStandardTypeDescriptor
        implements FennelStoredTypeDescriptor, Serializable
    {
        FennelType(String name, int ordinal)
        {
            super(name, ordinal);
        }

        public int getBitCount()
        {
            return 0;
        }

        public int getFixedByteCount()
        {
            return 0;
        }

        public int getMinByteCount(int maxWidth)
        {
            return 0;
        }

        public int getAlignmentByteCount(int width)
        {
            return 1;
        }

        public boolean isExact()
        {
            return false;
        }

        public boolean isSigned()
        {
            return false;
        }

        public boolean isNumeric()
        {
            return false;
        }
    }

    /**
     * Abstract base class for all numeric types.
     */
    private static abstract class FennelNumericType extends FennelType
    {
        private final int bitCount;
        private final int fixedByteCount;
        private final boolean signed;
        private final boolean exact;

        /**
         * Creates a FennelNumericType.
         */
        protected FennelNumericType(
            String name,
            int ordinal,
            int bitCount,
            int fixedByteCount,
            boolean signed,
            boolean exact)
        {
            super(name, ordinal);
            this.bitCount = bitCount;
            this.fixedByteCount = fixedByteCount;
            this.signed = signed;
            this.exact = exact;
        }

        public int getBitCount()
        {
            return bitCount;
        }

        public int getFixedByteCount()
        {
            return fixedByteCount;
        }

        public int getMinByteCount(int maxWidth)
        {
            return maxWidth;
        }

        public int getAlignmentByteCount(int width)
        {
            return width;
        }

        public boolean isSigned()
        {
            return signed;
        }

        public boolean isExact()
        {
            return exact;
        }

        public boolean isNumeric()
        {
            return true;
        }

        /**
         * Required by the serialization mechanism; should never be used.
         */
        protected FennelNumericType()
        {
            super(null, -1);
            this.bitCount = 0;
            this.fixedByteCount = 0;
            this.signed = false;
            this.exact = true;
        }
    }

    /**
     * Describes a signed byte.
     */
    private static class Type_INT_8 extends FennelNumericType
    {
        Type_INT_8()
        {
            super("s1", INT_8_ORDINAL, 0, 1, true, true);
        }

        public FennelAttributeAccessor newAttributeAccessor()
        {
            return new FennelAttributeAccessor.FennelByteAccessor();
        }
        
        public int compareValues(FennelTupleDatum d1, FennelTupleDatum d2)
        {
            return (int) (d2.getByte() - d1.getByte());
        }
    }

    /**
     * Describes an unsigned byte.
     */
    private static class Type_UINT_8 extends FennelNumericType
    {
        Type_UINT_8()
        {
            super("u1", UINT_8_ORDINAL, 0, 1, false, true);
        }
        
        public FennelAttributeAccessor newAttributeAccessor()
        {
            return new FennelAttributeAccessor.FennelByteAccessor();
        }
        
        public int compareValues(FennelTupleDatum d1, FennelTupleDatum d2)
        {
            return (int) (d2.getUnsignedByte() - d1.getUnsignedByte());
        }
    }

    /**
     * Describes a signed short.
     */
    private static class Type_INT_16 extends FennelNumericType
    {
        Type_INT_16()
        {
            super("s2", INT_16_ORDINAL, 0, 2, true, true);
        }

        public FennelAttributeAccessor newAttributeAccessor()
        {
            return new FennelAttributeAccessor.FennelShortAccessor();
        }

        public int compareValues(FennelTupleDatum d1, FennelTupleDatum d2)
        {
            return (int) (d2.getShort() - d1.getShort());
        }
    }

    /**
     * Describes an unsigned short.
     */
    private static class Type_UINT_16 extends FennelNumericType
    {
        Type_UINT_16()
        {
            super("u2", UINT_16_ORDINAL, 0, 2, false, true);
        }

        public FennelAttributeAccessor newAttributeAccessor()
        {
            return new FennelAttributeAccessor.FennelShortAccessor();
        }

        public int compareValues(FennelTupleDatum d1, FennelTupleDatum d2)
        {
            return d2.getUnsignedShort() - d1.getUnsignedShort();
        }
    }

    /**
     * Describes a signed int.
     */
    private static class Type_INT_32 extends FennelNumericType
    {
        Type_INT_32()
        {
            super("s4", INT_32_ORDINAL, 0, 4, true, true);
        }

        public FennelAttributeAccessor newAttributeAccessor()
        {
            return new FennelAttributeAccessor.FennelIntAccessor();
        }

        public int compareValues(FennelTupleDatum d1, FennelTupleDatum d2)
        {
            return d2.getInt() - d1.getInt();
        }
    }

    /**
     * Describes an unsigned int.
     */
    private static class Type_UINT_32 extends FennelNumericType
    {
        Type_UINT_32()
        {
            super("u4", UINT_32_ORDINAL, 0, 4, false, true);
        }

        public FennelAttributeAccessor newAttributeAccessor()
        {
            return new FennelAttributeAccessor.FennelIntAccessor();
        }

        public int compareValues(FennelTupleDatum d1, FennelTupleDatum d2)
        {
            return (int) (d2.getUnsignedInt() - d1.getUnsignedInt());
        }
    }

    /**
     * Describes a signed long.
     */
    private static class Type_INT_64 extends FennelNumericType
    {
        Type_INT_64()
        {
            super("s8", INT_64_ORDINAL, 0, 8, true, true);
        }

        public FennelAttributeAccessor newAttributeAccessor()
        {
            return new FennelAttributeAccessor.FennelLongAccessor();
        }

        public int compareValues(FennelTupleDatum d1, FennelTupleDatum d2)
        {
            return (int) (d2.getLong() - d1.getLong());
        }
    }

    /**
     * Describes an unsigned long.
     */
    private static class Type_UINT_64 extends FennelNumericType
    {
        Type_UINT_64()
        {
            super("u8", UINT_64_ORDINAL, 0, 8, false, true);
        }

        public FennelAttributeAccessor newAttributeAccessor()
        {
            return new FennelAttributeAccessor.FennelLongAccessor();
        }

        public int compareValues(FennelTupleDatum d1, FennelTupleDatum d2)
        {
            return (int) (d2.getUnsignedLong() - d1.getUnsignedLong());
        }
    }

    /**
     * Describes a float.
     */
    private static class Type_REAL extends FennelNumericType
    {
        Type_REAL()
        {
            super("r", REAL_ORDINAL, 0, 4, true, false);
        }

        public FennelAttributeAccessor newAttributeAccessor()
        {
            return new FennelAttributeAccessor.FennelIntAccessor();
        }

        public int compareValues(FennelTupleDatum d1, FennelTupleDatum d2)
        {
            return (int) (d2.getFloat() - d1.getFloat());
        }
    }

    /**
     * Describes a double.
     */
    private static class Type_DOUBLE extends FennelNumericType
    {
        public Type_DOUBLE()
        {
            super("d", DOUBLE_ORDINAL, 0, 8, true, false);
        }

        public FennelAttributeAccessor newAttributeAccessor()
        {
            return new FennelAttributeAccessor.FennelLongAccessor();
        }

        public int compareValues(FennelTupleDatum d1, FennelTupleDatum d2)
        {
            return (int) (d2.getDouble() - d1.getDouble());
        }
    }

    /**
     * Describes a boolean.
     */
    private static class Type_BOOL extends FennelNumericType
    {
        public Type_BOOL()
        {
            super("bo", BOOL_ORDINAL, 1, 1, false, true);
        }

        public FennelAttributeAccessor newAttributeAccessor()
        {
            return new FennelAttributeAccessor.FennelBitAccessor();
        }

        public int compareValues(FennelTupleDatum d1, FennelTupleDatum d2)
        {
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
     * Describes a fixed-width character string.
     */
    private static class Type_CHAR extends FennelType
    {
        Type_CHAR()
        {
            super("c", CHAR_ORDINAL);
        }

        public int getFixedBitCount()
        {
            return 0;
        }

        public int getMinByteCount(int maxWidth)
        {
            return maxWidth;
        }

        public FennelAttributeAccessor newAttributeAccessor()
        {
            return new FennelAttributeAccessor.FennelFixedWidthAccessor();
        }

        public int compareValues(FennelTupleDatum d1, FennelTupleDatum d2)
        {
            int c = d2.getLength() - d1.getLength();
            if (c != 0) {
                return c;
            }
            final String s1 = new String(d1.getBytes(), 0, c);
            final String s2 = new String(d2.getBytes(), 0, c);
            if (s1.equals(s2)) {
                return 0;
            }
            // arbitrarily mark the first one larger; if this is actually
            // used, compare hashcodes
            return -1;
        }
    }

    /**
     * Describes a variable-width character string.
     */
    private static class Type_VARCHAR extends FennelType
    {
        Type_VARCHAR()
        {
            super("vc", VARCHAR_ORDINAL);
        }

        public int getFixedBitCount()
        {
            return 0;
        }

        public FennelAttributeAccessor newAttributeAccessor()
        {
            return new FennelAttributeAccessor.FennelVarWidthAccessor();
        }

        public int compareValues(FennelTupleDatum d1, FennelTupleDatum d2)
        {
            int c = d2.getLength() - d1.getLength();
            if (c != 0) {
                return c;
            }
            final String s1 = new String(d1.getBytes(), 0, c);
            final String s2 = new String(d2.getBytes(), 0, c);
            if (s1.equals(s2)) {
                return 0;
            }
            // arbitrarily mark the first one larger; if this is actually
            // used, compare hashcodes
            return -1;
        }
    }

    /**
     * Describes a fixed-width binary string.
     */
    private static class Type_BINARY extends FennelType
    {
        Type_BINARY()
        {
            super("b", BINARY_ORDINAL);
        }

        public int getFixedBitCount()
        {
            return 0;
        }

        public int getMinByteCount(int maxWidth)
        {
            return maxWidth;
        }

        public FennelAttributeAccessor newAttributeAccessor()
        {
            return new FennelAttributeAccessor.FennelFixedWidthAccessor();
        }

        public int compareValues(FennelTupleDatum d1, FennelTupleDatum d2)
        {
            int c = d2.getLength() - d1.getLength();
            if (c != 0) {
                return c;
            }
            final String s1 = new String(d1.getBytes(), 0, c);
            final String s2 = new String(d2.getBytes(), 0, c);
            if (s1.equals(s2)) {
                return 0;
            }
            // arbitrarily mark the first one larger; if this is actually
            // used, compare hashcodes
            return -1;
        }
    }

    /**
     * Describes a variable-width binary array.
     */
    private static class Type_VARBINARY extends FennelType
    {
        Type_VARBINARY()
        {
            super("vb", VARBINARY_ORDINAL);
        }

        public int getFixedBitCount()
        {
            return 0;
        }

        public FennelAttributeAccessor newAttributeAccessor()
        {
            return new FennelAttributeAccessor.FennelVarWidthAccessor();
        }

        public int compareValues(FennelTupleDatum d1, FennelTupleDatum d2)
        {
            int c = d2.getLength() - d1.getLength();
            if (c != 0) {
                return c;
            }
            final String s1 = new String(d1.getBytes(), 0, c);
            final String s2 = new String(d2.getBytes(), 0, c);
            if (s1.equals(s2)) {
                return 0;
            }
            // arbitrarily mark the first one larger; if this is actually
            // used, compare hashcodes
            return -1;
        }
    }
}

// End FennelStandardTypeDescriptor.java
