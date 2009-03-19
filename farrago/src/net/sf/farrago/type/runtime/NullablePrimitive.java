/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
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
package net.sf.farrago.type.runtime;

import java.math.*;

import net.sf.farrago.resource.*;

import org.eigenbase.util.*;
import org.eigenbase.util14.*;


/**
 * NullablePrimitive is the abstract superclass for implementations of
 * NullableValue corresponding to Java primitives. These holder classes are
 * declared as static inner classes of NullablePrimitive with names taken from
 * the standard holder classes in java.lang.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class NullablePrimitive
    implements NullableValue,
        AssignableValue
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Name of field storing value.
     */
    public static final String VALUE_FIELD_NAME = "value";

    public static final String TRUE_LITERAL = "TRUE";
    public static final String FALSE_LITERAL = "FALSE";
    public static final String UNKNOWN_LITERAL = "UNKNOWN";

    /**
     * Name of field storing null indicator.
     */
    public static final String NULL_IND_FIELD_NAME = NULL_IND_ACCESSOR_NAME;
    private static final Integer INT_ONE = new Integer(1);
    private static final Integer INT_ZERO = new Integer(0);

    //~ Instance fields --------------------------------------------------------

    /**
     * Whether this value is null.
     */
    public boolean isNull;

    //~ Methods ----------------------------------------------------------------

    // implement NullableValue
    public void setNull(boolean isNull)
    {
        this.isNull = isNull;
    }

    // implement NullableValue
    public boolean isNull()
    {
        return isNull;
    }

    // implement NullableValue
    public Object getNullableData()
    {
        if (isNull) {
            return null;
        }
        try {
            return getClass().getField(VALUE_FIELD_NAME).get(this);
        } catch (Exception ex) {
            throw Util.newInternal(ex);
        }
    }

    // implement AssignableValue
    public void assignFrom(Object obj)
    {
        if (obj == null) {
            setNull(true);
        } else if (obj instanceof Number) {
            setNull(false);
            setNumber((Number) obj);
        } else if (obj instanceof DataValue) {
            if (obj instanceof BytePointer) {
                BytePointer bytePointer = (BytePointer) obj;
                if (bytePointer.isNull()) {
                    setNull(true);
                } else {
                    // TODO jvs 17-Aug-2006:  Use this fastpath for
                    // assignment to NOT NULL primitives also.
                    long n = bytePointer.attemptFastAsciiByteToLong();
                    if (n != Long.MAX_VALUE) {
                        setNull(false);
                        setLong(n);
                        return;
                    }
                }
            }
            DataValue dataValue = (DataValue) obj;
            assignFrom(dataValue.getNullableData());
        } else if (obj instanceof Boolean) {
            setNull(false);
            Boolean b = (Boolean) obj;
            setNumber(b.booleanValue() ? INT_ONE : INT_ZERO);
        } else {
            setNull(false);
            String s = obj.toString();
            Number n;
            try {
                n = new BigDecimal(s.trim());
            } catch (NumberFormatException ex) {
                // NOTE jvs 11-Oct-2005:  leave ex out entirely, because
                // it doesn't contain useful information and causes
                // test diffs due to JVM variance
                throw FarragoResource.instance().AssignFromFailed.ex(
                    s,
                    "NUMERIC",
                    "NumberFormatException");
            }
            setNumber(n);
        }
    }

    /**
     * Assignment from abstract Number object.
     *
     * @param number a new non-null value to be assigned
     */
    protected abstract void setNumber(Number number);

    /**
     * Assignment from non-null long value.
     *
     * @param n long value to assign
     */
    protected abstract void setLong(long n);

    // override Object
    public String toString()
    {
        Object o = getNullableData();
        return (o == null) ? null : o.toString();
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Nullable wrapper for boolean.
     */
    public static final class NullableBoolean
        extends NullablePrimitive
        implements BitReference
    {
        /**
         * Wrapped primitive
         */
        public boolean value;

        // implement AssignableValue for String
        public void assignFrom(Object obj)
        {
            if (obj == null) {
                setNull(true);
            } else if (obj instanceof String) {
                String s = (String) obj;
                s = s.trim();
                if (s.equalsIgnoreCase(TRUE_LITERAL)) {
                    value = true;
                } else if (s.equalsIgnoreCase(FALSE_LITERAL)) {
                    value = false;
                } else if (s.equalsIgnoreCase(UNKNOWN_LITERAL)) {
                    setNull(true);
                } else {
                    super.assignFrom(obj);
                }
            } else {
                super.assignFrom(obj);
            }
        }

        // implement NullablePrimitive
        protected void setNumber(Number number)
        {
            value = (number.longValue() != 0);
        }

        // implement NullablePrimitive
        protected void setLong(long n)
        {
            value = (n != 0);
        }

        // implement BitReference
        public void setBit(boolean bit)
        {
            value = bit;
        }

        // implement BitReference
        public boolean getBit()
        {
            return value;
        }

        /**
         * Implements cast from string to non-nullable boolean Invoked by
         * generated code
         */
        public static final boolean convertString(String s)
        {
            s = s.trim();
            if (s.equalsIgnoreCase(TRUE_LITERAL)) {
                return true;
            } else if (s.equalsIgnoreCase(FALSE_LITERAL)) {
                return false;
            } else {
                throw FarragoResource.instance().AssignFromFailed.ex(
                    s,
                    "BOOLEAN",
                    "Invalid boolean value");
            }
        }

        /**
         * Implements the three-valued-logic version of the AND operator.
         * Invoked by generated code.
         *
         * @param n0 null indictator for arg0
         * @param v0 truth value of arg0 when !n0
         * @param n1 null indicator for arg1
         * @param v1 truth value of arg1 when !n1
         */
        public final void assignFromAnd3VL(
            boolean n0,
            boolean v0,
            boolean n1,
            boolean v1)
        {
            if (n0 && n1) {
                // (UNKNOWN AND UNKNOWN) == UNKNOWN
                isNull = true;
            } else if (n0) {
                if (v1) {
                    // (UNKNOWN AND TRUE) == UNKNOWN
                    isNull = true;
                } else {
                    // (UNKNOWN AND FALSE) == FALSE
                    isNull = false;
                    value = false;
                }
            } else if (n1) {
                if (v0) {
                    // (TRUE AND UNKNOWN) == UNKNOWN
                    isNull = true;
                } else {
                    // (FALSE AND UNKOWN) == FALSE
                    isNull = false;
                    value = false;
                }
            } else {
                // (KNOWN AND KNOWN) == KNOWN
                isNull = false;
                value = v0 && v1;
            }
        }

        /**
         * Implements the three-valued-logic version of the OR operator. Invoked
         * by generated code.
         *
         * @param n0 null indictator for arg0
         * @param v0 truth value of arg0 when !n0
         * @param n1 null indicator for arg1
         * @param v1 truth value of arg1 when !n1
         */
        public final void assignFromOr3VL(
            boolean n0,
            boolean v0,
            boolean n1,
            boolean v1)
        {
            if (n0 && n1) {
                // (UNKNOWN OR UNKNOWN) == UNKNOWN
                isNull = true;
            } else if (n0) {
                if (v1) {
                    // (UNKNOWN OR TRUE) == TRUE
                    isNull = false;
                    value = true;
                } else {
                    // (UNKNOWN OR FALSE) == UKNOWN
                    isNull = true;
                }
            } else if (n1) {
                if (v0) {
                    // (TRUE OR UNKNOWN) == TRUE
                    isNull = false;
                    value = true;
                } else {
                    // (FALSE OR UNKOWN) == UNKNOWN
                    isNull = true;
                }
            } else {
                // (KNOWN OR KNOWN) == KNOWN
                isNull = false;
                value = v0 || v1;
            }
        }
    }

    /**
     * Nullable wrapper for byte.
     */
    public static final class NullableByte
        extends NullablePrimitive
    {
        /**
         * Wrapped primitive
         */
        public byte value;

        // implement NullablePrimitive
        protected void setNumber(Number number)
        {
            if ((number instanceof Float) || (number instanceof Double)) {
                value = (byte) NumberUtil.round(number.doubleValue());
            } else {
                value = number.byteValue();
            }
        }

        // implement NullablePrimitive
        protected void setLong(long n)
        {
            value = (byte) n;
        }
    }

    /**
     * Nullable wrapper for double.
     */
    public static final class NullableDouble
        extends NullablePrimitive
    {
        /**
         * Wrapped primitive
         */
        public double value;

        // implement NullablePrimitive
        protected void setNumber(Number number)
        {
            value = number.doubleValue();
        }

        // implement NullablePrimitive
        protected void setLong(long n)
        {
            value = (double) n;
        }
    }

    /**
     * Nullable wrapper for float.
     */
    public static final class NullableFloat
        extends NullablePrimitive
    {
        /**
         * Wrapped primitive
         */
        public float value;

        // implement NullablePrimitive
        protected void setNumber(Number number)
        {
            value = number.floatValue();
        }

        // implement NullablePrimitive
        protected void setLong(long n)
        {
            value = (float) n;
        }
    }

    /**
     * Nullable wrapper for int.
     */
    public static final class NullableInteger
        extends NullablePrimitive
    {
        /**
         * Wrapped primitive
         */
        public int value;

        // implement NullablePrimitive
        protected void setNumber(Number number)
        {
            if ((number instanceof Float) || (number instanceof Double)) {
                value = (int) NumberUtil.round(number.doubleValue());
            } else {
                value = number.intValue();
            }
        }

        // implement NullablePrimitive
        protected void setLong(long n)
        {
            value = (int) n;
        }
    }

    /**
     * Nullable wrapper for long.
     */
    public static class NullableLong
        extends NullablePrimitive
    {
        /**
         * Wrapped primitive
         */
        public long value;

        // implement NullablePrimitive
        protected void setNumber(Number number)
        {
            if ((number instanceof Float) || (number instanceof Double)) {
                value = (long) NumberUtil.round(number.doubleValue());
            } else {
                value = number.longValue();
            }
        }

        // implement NullablePrimitive
        protected void setLong(long n)
        {
            value = (long) n;
        }

        // override NullablePrimitive
        public void assignFrom(Object o)
        {
            if (o == null) {
                setNull(true);
            } else if (o instanceof SqlDateTimeWithoutTZ) {
                SqlDateTimeWithoutTZ datetime = (SqlDateTimeWithoutTZ) o;
                if (datetime.isNull()) {
                    setNull(true);
                } else {
                    setNull(false);
                    setLong(datetime.value.getTime());
                }
            } else if (o instanceof EncodedSqlInterval) {
                EncodedSqlInterval interval = (EncodedSqlInterval) o;
                if (interval.isNull()) {
                    setNull(true);
                } else {
                    setNull(false);
                    setLong(interval.value);
                }
            } else {
                super.assignFrom(o);
            }
        }
    }

    /**
     * Nullable wrapper for short.
     */
    public static final class NullableShort
        extends NullablePrimitive
    {
        /**
         * Wrapped primitive
         */
        public short value;

        // implement NullablePrimitive
        protected void setNumber(Number number)
        {
            if ((number instanceof Float) || (number instanceof Double)) {
                value = (short) NumberUtil.round(number.doubleValue());
            } else {
                value = number.shortValue();
            }
        }

        // implement NullablePrimitive
        protected void setLong(long n)
        {
            value = (short) n;
        }
    }
}

// End NullablePrimitive.java
