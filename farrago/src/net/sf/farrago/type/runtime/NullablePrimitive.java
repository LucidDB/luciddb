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

package net.sf.farrago.type.runtime;

import net.sf.farrago.resource.*;

import org.eigenbase.util.*;

import java.math.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;


/**
 * NullablePrimitive is the abstract superclass for implementations of
 * NullableValue corresponding to Java primitives.  These holder classes are
 * declared as static inner classes of NullablePrimitive with names taken
 * from the standard holder classes in java.lang.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class NullablePrimitive
    implements NullableValue, AssignableValue
{
    /**
     * Name of field storing value.
     */
    public static final String VALUE_FIELD_NAME = "value";

    /**
     * Name of field storing null indicator.
     */
    public static final String NULL_IND_FIELD_NAME = NULL_IND_ACCESSOR_NAME;

    private static final Integer INT_ONE = new Integer(1);
    private static final Integer INT_ZERO = new Integer(0);

    //~ Instance fields -------------------------------------------------------

    /** Whether this value is null. */
    public boolean isNull;

    //~ Methods ---------------------------------------------------------------

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
        } else if (obj instanceof NullablePrimitive) {
            NullablePrimitive nullable = (NullablePrimitive) obj;
            assignFrom(nullable.getNullableData());
        } else if (obj instanceof Boolean) {
            setNull(false);
            Boolean b = (Boolean) obj;
            setNumber(b.booleanValue() ? INT_ONE : INT_ZERO);
        } else {
            assert(obj instanceof String) : obj.getClass().getName();
            String s = (String) obj;
            Number n;
            try {
                n = new BigDecimal(s);
            } catch (NumberFormatException ex) {
                throw FarragoResource.instance().
                    newAssignFromFailed(s, "NUMERIC", ex.toString());
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
     *
     * @param Class
     */
    public static Class getPrimitiveClass(Class np) throws NoSuchFieldException
    {
        assert NullablePrimitive.class.isAssignableFrom(np) : "parameter to static getPrimitiveClass must be assignable to NullablePrimitive: " + np.toString();
        return np.getField(VALUE_FIELD_NAME).getType();
    }
    //~ Inner Classes ---------------------------------------------------------

    /**
     * Nullable wrapper for boolean.
     */
    public static final class NullableBoolean
        extends NullablePrimitive
            implements BitReference
    {
        /** Wrapped primitive */
        public boolean value;

        // implement NullablePrimitive
        protected void setNumber(Number number)
        {
            value = (number.longValue() != 0);
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
    }

    /**
     * Nullable wrapper for byte.
     */
    public static final class NullableByte extends NullablePrimitive
    {
        /** Wrapped primitive */
        public byte value;

        // implement NullablePrimitive
        protected void setNumber(Number number)
        {
            value = number.byteValue();
        }
    }

    /**
     * Nullable wrapper for double.
     */
    public static final class NullableDouble extends NullablePrimitive
    {
        /** Wrapped primitive */
        public double value;

        // implement NullablePrimitive
        protected void setNumber(Number number)
        {
            value = number.doubleValue();
        }
    }

    /**
     * Nullable wrapper for float.
     */
    public static final class NullableFloat extends NullablePrimitive
    {
        /** Wrapped primitive */
        public float value;

        // implement NullablePrimitive
        protected void setNumber(Number number)
        {
            value = number.floatValue();
        }
    }

    /**
     * Nullable wrapper for int.
     */
    public static final class NullableInteger extends NullablePrimitive
    {
        /** Wrapped primitive */
        public int value;

        // implement NullablePrimitive
        protected void setNumber(Number number)
        {
            value = number.intValue();
        }
    }

    /**
     * Nullable wrapper for long.
     */
    public static class NullableLong extends NullablePrimitive
    {
        /** Wrapped primitive */
        public long value;

        // implement NullablePrimitive
        protected void setNumber(Number number)
        {
            value = number.longValue();
        }
    }


    /**
     * Nullable wrapper for short.
     */
    public static final class NullableShort extends NullablePrimitive
    {
        /** Wrapped primitive */
        public short value;

        // implement NullablePrimitive
        protected void setNumber(Number number)
        {
            value = number.shortValue();
        }
    }
}


// End NullablePrimitive.java
