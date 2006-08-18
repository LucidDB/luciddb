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
 * FIXME: this code is quite inefficient. First, it relies heavily on 
 * Java libraries for decimal support. As a result it allocates memory 
 * on a per row basis. Second, it is inefficient in other ways. 
 * It relies on AssignableDecimal to assign values and NarrowedDecimal 
 * to squeeze external data values. This should all be optimized, into 
 * simple low-level operations.
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
     * Encodes a long value as an EncodedSqlDecimal.
     *
     * @param value value to be encoded as an EncodedSqlDecimal
     * @param overflowCheck whether to check for overflow
     */
    public void reinterpret(long value, boolean overflowCheck)
    {
        if (overflowCheck && (getPrecision() < 19)) {
            if (overflowValue == 0) {
                overflowValue = BigInteger.TEN.pow(
                        getPrecision()).longValue();
            }
            if (Math.abs(value) >= overflowValue) {
                throw FarragoResource.instance().Overflow.ex();
            }
        }
        assignFrom(value);
    }

    public void reinterpret(long value)
    {
        reinterpret(value, false);
    }

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

    public void reinterpret(NullablePrimitive.NullableLong primitive)
    {
        reinterpret(primitive, false);
    }

    public void assignTo(NullablePrimitive.NullableLong target)
    {
        target.setNull(isNull());
        target.value = this.value;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Class which assigns a value to an {@link EncodedSqlDecimal}.
     */
    public class AssignableDecimal
        extends NullablePrimitive.NullableLong
    {
        EncodedSqlDecimal parent;

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
            BigInteger usv = bd.unscaledValue();
            long usvl = usv.longValue();
            if (usv.equals(BigInteger.valueOf(usvl))) {
                parent.reinterpret(usvl, true);
            } else {
                throw FarragoResource.instance().Overflow.ex();
            }
        }

        // implement NullablePrimitive
        protected void setLong(long n)
        {
            // TODO jvs 18-Aug-2006:  optimize
            setNumber(n);
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

    /**
     * Narrows an external BigDecimal value (potentially larger than the 
     * database can handle) into a native value. Unsupported values are 
     * replaced with null.
     * 
     * @param o the BigDecimal value
     * @param prec the precision of the native type
     * @param scale the scale of the native type
     * 
     * @return a new sql decimal value of the specified precision and scale
     */
    public static EncodedSqlDecimal narrowCast(Object o, int prec, int scale)
    {
        BigDecimal bd = (BigDecimal)o;
        return new NarrowedDecimal(bd, prec, scale);
    }

    private static class NarrowedDecimal extends EncodedSqlDecimal 
        implements NullableValue
    {
        static private BigInteger maxValue = 
            BigInteger.valueOf(Long.MAX_VALUE);
        
        private int prec;
        private int scale;

        public NarrowedDecimal(BigDecimal bd, int prec, int scale) {
            this.prec = prec;
            this.scale = scale;
            if (bd == null) {
                setNull(true);
            } else {
                BigDecimal rounded = 
                    bd.setScale(scale, BigDecimal.ROUND_HALF_UP);
                if (rounded.unscaledValue().compareTo(maxValue) == 1) {
                    setNull(true);
                } else {
                    value = bd.unscaledValue().longValue();
                }
            }
        }
        
        protected int getPrecision() { return prec; }
        protected int getScale() { return scale; }
    }
}

// End EncodedSqlDecimal.java
