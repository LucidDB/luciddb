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

import org.eigenbase.util.*;

import net.sf.farrago.resource.FarragoResource;

/**
 * Runtime type for decimal values.
 *
 *
 * @author jpham
 * @since Dec 21, 2005
 * @version $Id$
 **/
public abstract class EncodedSqlDecimal 
    implements AssignableValue, NullableValue
{
    //~ Static fields -------------------------------------------------------

    public static final String GET_PRECISION_METHOD_NAME = "getPrecision";
    public static final String GET_SCALE_METHOD_NAME = "getScale";
    public static final String REINTERPRET_METHOD_NAME = "reinterpret";
    private static final Integer INT_ONE = new Integer(1);
    private static final Integer INT_ZERO = new Integer(0);
    
    //~ Instance fields -------------------------------------------------------

    private boolean isNull;
    
    /**
     * Scaled integer representation of the decimal
     */
    public long value;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a runtime object
     */
    public EncodedSqlDecimal()
    {
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
    
    public void setNull(boolean isNull)
    {
        this.isNull = isNull;
    }
    
    public boolean isNull()
    {
        return isNull;
    }
    
    // implement AssignableValue
    public void assignFrom(Object obj) 
    {
        if (obj == null) {
            setNull(true);
        } else if (obj instanceof EncodedSqlDecimal) {
            setNull(false);
            EncodedSqlDecimal decimal = (EncodedSqlDecimal) obj;
            value = decimal.value;
        } else {
            // TODO: are we allowed to allocate here?
            setNull(false);
            String s = obj.toString();
            BigDecimal n;
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
            n = n.setScale(getScale(), BigDecimal.ROUND_HALF_UP);
            value = n.unscaledValue().longValue();
        }
    }
    
    // implement AssignableValue
    public void assignFrom(long l)
    {
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
     * Encodes a long value as an EncodedSqlDecimal. Implemented by 
     * setting the value of the current object and returning the 
     * current object. This scheme avoids allocations.
     * 
     * @param value value to be encoded as an EncodedSqlDecimal
     * @return this, an EncodedSqlDecimal whose value has been set
     */
    public EncodedSqlDecimal reinterpret(long value) 
    {
        this.value = value;
        return this;
    }
}

// End EncodedSqlDecimal.java
