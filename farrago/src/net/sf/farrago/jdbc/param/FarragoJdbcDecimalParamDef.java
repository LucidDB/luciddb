/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2005-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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
package net.sf.farrago.jdbc.param;

import java.math.*;

import org.eigenbase.util14.*;


/**
 * FarragoJdbcEngineDecimalParamDef defines a Decimal parameter. This class is
 * JDK 1.4 compatible.
 *
 * @author Angel Chang
 * @version $Id$
 */
class FarragoJdbcDecimalParamDef
    extends FarragoJdbcParamDef
{
    //~ Instance fields --------------------------------------------------------

    final BigInteger maxUnscaled;
    final BigInteger minUnscaled;

    //~ Constructors -----------------------------------------------------------

    FarragoJdbcDecimalParamDef(
        String paramName,
        FarragoParamFieldMetaData paramMetaData)
    {
        super(paramName, paramMetaData);
        maxUnscaled = NumberUtil.getMaxUnscaled(paramMetaData.precision);
        minUnscaled = NumberUtil.getMinUnscaled(paramMetaData.precision);
    }

    //~ Methods ----------------------------------------------------------------

    private BigDecimal getBigDecimal(Object value, int scale)
    {
        BigDecimal bd;
        if (value == null) {
            checkNullable();
            return null;
        } else if (value instanceof Number) {
            bd = NumberUtil.toBigDecimal((Number) value);
        } else if (value instanceof Boolean) {
            bd = new BigDecimal(((Boolean) value).booleanValue() ? 1 : 0);
        } else if (value instanceof String) {
            try {
                bd = new BigDecimal(value.toString().trim());
            } catch (NumberFormatException ex) {
                throw newInvalidFormat(value);
            }
        } else {
            throw newInvalidType(value);
        }
        bd = NumberUtil.rescaleBigDecimal(bd, scale);
        return bd;
    }

    // implement FarragoSessionStmtParamDef
    public Object scrubValue(Object x)
    {
        BigDecimal n = getBigDecimal(x, paramMetaData.scale);
        if (n != null) {
            BigInteger usv = n.unscaledValue();
            checkRange(usv, minUnscaled, maxUnscaled);
        }
        return n;
    }
}

// End FarragoJdbcDecimalParamDef.java
