/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2003 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;
import org.eigenbase.util14.*;


/**
 * Runtime type for decimal values. The usage of decimal values is highly
 * restricted within the Farrago runtime. The operations allowed for decimals
 * are:
 *
 * <ul>
 * <li>A decimal may be casted to or from strings.
 * <li>A decimal may be assigned from another decimal
 * <li>A decimal may be reinterpreted to or from its internal representation, a
 * long value.
 * </ul>
 *
 * The optimizer does not allow other operations to reach to the Farrago
 * runtime. As usual, the method <code>getNullableData</code> returns an
 * external data type, conforming to SQL standards.
 *
 * <p>Note: the code may be inefficient, since it relies on Java libraries 
 * for decimal support and allocates memory on a per row basis.
 *
 * @author jpham
 * @version $Id$
 * @since Dec 21, 2005
 */
public abstract class EncodedSqlDecimal
    implements AssignableValue
{

    //~ Static fields/initializers ---------------------------------------------

    public static final String GET_PRECISION_METHOD_NAME = "getPrecision";
    public static final String GET_SCALE_METHOD_NAME = "getScale";
    public static final String REINTERPRET_METHOD_NAME = "reinterpret";
    public static final String ASSIGN_TO_METHOD_NAME = "assignTo";
    public static final String VALUE_FIELD_NAME = "value";
    public static final String NARROW_CAST_METHOD_NAME = "narrowCast";

    //~ Instance fields --------------------------------------------------------

    private boolean isNull;
    public long value;
    private AssignableDecimal helper;
    private long overflowValue = 0;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a runtime object
     */
    public EncodedSqlDecimal()
    {
        helper = new AssignableDecimal(this);

        // NOTE: overflowValue depends on an abstract method. The use of
        // an abstract method inside a constructor may cause problems for
        // Java interpreters, so save initialization of overflowValue
        // for later.
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return the decimal precision of this number
     */
    protected abstract int getPrecision();

    /**
     * @return the decimal scale of this number
     */
    protected abstract int getScale();

    // implement NullableValue
    public boolean isNull()
    {
        return isNull;
    }

    // implement NullableValue
    public void setNull(boolean b)
    {
        isNull = b;
    }

    // implement AssignableValue
    public void assignFrom(Object obj)
    {
        helper.assignFrom(obj);
    }

    // implement AssignableValue
    public void assignFrom(long l)
    {
        // NOTE: this is used internally and for decimal literals
        setNull(false);
        value = l;
    }

    // implement DataValue
    public Object getNullableData()
    {
        if (isNull()) {
            return null;
        }
        return BigDecimal.valueOf(
                value,
                getScale());
    }

    /**
     * Encodes a long value as an EncodedSqlDecimal, with an optional 
     * overflow check.
     *
     * @param value value to be encoded as an EncodedSqlDecimal
     * @param overflowCheck whether to check for overflow
     */
    public void reinterpret(long value, boolean overflowCheck)
    {
        if (overflowCheck && (getPrecision() < 19)) {
            if (overflowValue == 0) {
                overflowValue = 
                    NumberUtil.getMaxUnscaled(getPrecision()).longValue() + 1;
            }
            if (Math.abs(value) >= overflowValue) {
                throw FarragoResource.instance().Overflow.ex();
            }
        }
        assignFrom(value);
    }

    /**
     * Encodes a long value as an EncodedSqlDecimal without an overflow check.
     * 
     * @param value value to be encoded as an EncodedSqlDecimal
     */
    public void reinterpret(long value)
    {
        reinterpret(value, false);
    }

    /**
     * Encodes a long value as an EncodedSqlDecimal, with an optional 
     * overflow check.
     *
     * @param value value to be encoded as an EncodedSqlDecimal
     * @param overflowCheck whether to check for overflow
     */
    public void reinterpret(
        NullablePrimitive.NullableLong primitive,
        boolean overflowCheck)
    {
        if (primitive.isNull()) {
            setNull(true);
            return;
        }
        reinterpret(primitive.value, overflowCheck);
    }

    /**
     * Encodes a long value as an EncodedSqlDecimal without an overflow check.
     * 
     * @param value value to be encoded as an EncodedSqlDecimal
     */
    public void reinterpret(NullablePrimitive.NullableLong primitive)
    {
        reinterpret(primitive, false);
    }

    /**
     * Assigns the internal value of this decimal to a long variable
     * 
     * @param target the variable to be assigned
     */
    public void assignTo(NullablePrimitive.NullableLong target)
    {
        target.setNull(isNull());
        target.value = this.value;
    }

    // override Object
    public String toString()
    {
        Object o = getNullableData();
        return (o == null) ? null : o.toString();
    }

    /**
     * Narrows an external BigDecimal value (potentially larger than the 
     * database can handle) into a native value. Unsupported values are 
     * replaced with null.
     * 
     * @param o the BigDecimal value
     */
    public void narrowCast(Object o)
    {
        // get the external decimal
        BigDecimal bd = (BigDecimal) o;
        if (bd == null) {
            setNull(true);
            return;
        }
        
        // round it to the correct scale; if it's too large, then 
        // replace it with null
        BigDecimal rounded = NumberUtil.rescaleBigDecimal(bd, getScale());
        if (! NumberUtil.isValidDecimal(rounded)) {
            setNull(true);
            return;
        }
        
        assignFrom(rounded.unscaledValue().longValue());
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Class which assigns a value to an {@link EncodedSqlDecimal}. 
     * Note that EncodedSqlDecimal cannot inherit from NullablePrimitive 
     * because EncodedSqlDecimal supports "NOT NULL" data and 
     * NullablePrimitive tags data as nullable.
     */
    public class AssignableDecimal
        extends NullablePrimitive.NullableLong
    {
        private EncodedSqlDecimal parent;

        /**
         * The maximum LONG value that can be assigned to this decimal. 
         * No overflow check is performed when this value is set to 
         * Long.MAX_VALUE.
         */
        private long longOverflow;

        /**
         * The scale factor applied to integers assigned to this decimal
         */
        private long longScaleFactor;

        public AssignableDecimal(EncodedSqlDecimal parent)
        {
            this.parent = parent;
        }

        public void setNull(boolean b)
        {
            parent.setNull(b);
        }

        protected void setNumber(Number number)
        {
            BigDecimal bd = NumberUtil.toBigDecimal(
                    number,
                    getScale());

            // Check Overflow
            if (NumberUtil.isValidDecimal(bd)) {
                parent.reinterpret(bd.unscaledValue().longValue(), true);
            } else {
                throw FarragoResource.instance().Overflow.ex();
            }
        }

        // implement NullablePrimitive
        protected void setLong(long n)
        {
            // Check for overflow if the number of whole digits is less than 
            // 19, the maximum for a long. Also set the scaling factor used 
            // to convert a long into a decimal of the specified scale.
            if (longOverflow == 0) {
                int precision = parent.getPrecision();
                int scale = parent.getScale();
                int wholeDigits = precision - scale;

                // Scale must be less than 19 to generate a scale factor.
                // The scale factor is 10^scale.
                Util.permAssert(
                    scale < 19, "decimal scale exceeded limit for setLong");
                longScaleFactor = 
                    BigInteger.TEN.pow(parent.getScale()).longValue();

                if (wholeDigits >= 19) {
                    longOverflow = Long.MAX_VALUE;
                } else if (precision == 19) {
                    longOverflow = (Long.MAX_VALUE / longScaleFactor) + 1;
                } else {
                    longOverflow = 
                        BigInteger.TEN.pow(wholeDigits).longValue();
                }
            }
            if (longOverflow != Long.MAX_VALUE 
                && Math.abs(n) >= longOverflow) 
            {
                throw FarragoResource.instance().Overflow.ex();
            }

            reinterpret(n*longScaleFactor);
        }
        
        // implement AssignableValue
        public void assignFrom(Object obj)
        {
            if (obj == null) {
                parent.setNull(true);
            } else if (obj instanceof EncodedSqlDecimal) {
                EncodedSqlDecimal decimal = (EncodedSqlDecimal) obj;
                if ((decimal.getScale() == parent.getScale())
                    && (decimal.getPrecision() == parent.getPrecision())) {
                    parent.setNull(decimal.isNull());
                    parent.value = decimal.value;
                } else {
                    super.assignFrom(decimal.getNullableData());
                }
            } else {
                super.assignFrom(obj);
            }
        }
    }
}

// End EncodedSqlDecimal.java
