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

import java.math.BigDecimal;
import java.math.BigInteger;

import org.eigenbase.util.*;
import org.eigenbase.util14.NumberUtil;

import net.sf.farrago.resource.FarragoResource;

/**
 * Runtime type for decimal values. The usage of decimal values 
 * is highly restricted within the Farrago runtime. The operations 
 * allowed for decimals are:
 * 
 * <ul>
 *   <li>A decimal may be casted to or from strings. 
 *   <li>A decimal may be reinterpreted to or from its
 *       internal representation, a long value.
 * </ul> 
 * 
 * The optimizer does not allow other operations to reach to the 
 * Farrago runtime. This class extends a primitive type so that it 
 * is easily assigned to and from its internal representation. As 
 * usual, the <code>getNullableData</code> method returns an 
 * external data type, conforming to SQL standards.
 *
 * @author jpham
 * @since Dec 21, 2005
 * @version $Id$
 **/
public abstract class EncodedSqlDecimal 
    extends NullablePrimitive.NullableLong
{
    //~ Static fields -------------------------------------------------------

    public static final String GET_PRECISION_METHOD_NAME = "getPrecision";
    public static final String GET_SCALE_METHOD_NAME = "getScale";
    public static final String REINTERPRET_METHOD_NAME = "reinterpret";
    
    //~ Constructors ----------------------------------------------------------

    private long overflowValue = 0;
    
    /**
     * Creates a runtime object
     */
    public EncodedSqlDecimal()
    {
        // NOTE: overflowValue depends on an abstract method. The use of 
        // an abstract method inside a constructor may cause problems for 
        // Java interpreters, so save initialization of overflowValue 
        // for later.
        super();
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * @return the decimal precision of this number
     */
    protected abstract int getPrecision();
    
    /**
     * @return the decimal scale of this number
     */
    protected abstract int getScale();

    protected void setNumber(Number number)
    {
        BigDecimal bd = NumberUtil.toBigDecimal(number, getScale());

        // Check Overflow
        BigInteger usv = bd.unscaledValue();
        long usvl = usv.longValue();
        if (usv.equals(BigInteger.valueOf(usvl))) {
            reinterpret(usvl, true);
        } else {
            throw FarragoResource.instance().Overflow.ex();
        }
    }

    // implement AssignableValue
    public void assignFrom(Object obj) 
    {
        if (obj == null) {
            setNull(true);
        } else if (obj instanceof EncodedSqlDecimal) {
            EncodedSqlDecimal decimal = (EncodedSqlDecimal) obj;
            setNull(decimal.isNull());
            value = decimal.value;
        } else {
            super.assignFrom(obj);
        }
    }
    
    // implement AssignableValue
    public void assignFrom(long l)
    {
        // NOTE: this is a quicker, cheaper version of assignment 
        // for the common case
        setNull(false);
        value = l;
    }
    
    // implement DataValue
    public Object getNullableData()
    {
        if (isNull()) {
            return null;
        }
        return BigDecimal.valueOf(value, getScale());
    }
    
    /**
     * Encodes a long value as an EncodedSqlDecimal.
     * 
     * @param value value to be encoded as an EncodedSqlDecimal
     * @param overflowCheck whether to check for overflow
     * 
     * @return this, an EncodedSqlDecimal whose value has been set
     */
    public void reinterpret(long value, boolean overflowCheck) 
    {
        if (overflowCheck && getPrecision() < 19) {
            if (overflowValue == 0) {
                overflowValue = Util.powerOfTen(getPrecision());
            }
            if (Math.abs(value) >= overflowValue) {
                throw FarragoResource.instance().Overflow.ex();
            }
        }
        assignFrom(value);
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
}

// End EncodedSqlDecimal.java
