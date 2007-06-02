/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 Disruptive Tech
// Copyright (C) 2006-2007 LucidEra, Inc.
// Portions Copyright (C) 2006-2007 John V. Sichi
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

import java.sql.*;


/**
 * FarragoJdbcEngineApproxParamDef defines a approximate numeric parameter. This
 * class is JDK 1.4 compatible.
 *
 * @author Angel Chang
 * @version $Id$
 */
class FarragoJdbcApproxParamDef
    extends FarragoJdbcParamDef
{
    //~ Instance fields --------------------------------------------------------

    final double min;
    final double max;

    //~ Constructors -----------------------------------------------------------

    FarragoJdbcApproxParamDef(
        String paramName,
        FarragoParamFieldMetaData paramMetaData)
    {
        super(paramName, paramMetaData);

        switch (paramMetaData.type) {
        case Types.REAL:
            min = -Float.MAX_VALUE;
            max = Float.MAX_VALUE;
            break;
        case Types.FLOAT:
        case Types.DOUBLE:
            min = -Double.MAX_VALUE;
            max = Double.MAX_VALUE;
            break;
        default:
            min = 0;
            max = 0;
            assert (false) : "Approximate paramMetaData expected";
        }
    }

    //~ Methods ----------------------------------------------------------------

    private Double getDouble(Object value)
    {
        if (value instanceof Number) {
            Number n = (Number) value;
            checkRange(
                n.doubleValue(),
                min,
                max);
            return new Double(n.doubleValue());
        } else if (value instanceof Boolean) {
            return (((Boolean) value).booleanValue() ? new Double(1)
                : new Double(0));
        } else if (value instanceof String) {
            try {
                BigDecimal bd = new BigDecimal(value.toString().trim());
                return getDouble(bd);
            } catch (NumberFormatException ex) {
                throw newInvalidFormat(value);
            }
        } else {
            throw newInvalidType(value);
        }
    }

    // implement FarragoSessionStmtParamDef
    public Object scrubValue(Object x)
    {
        if (x == null) {
            checkNullable();
            return null;
        } else {
            return getDouble(x);
        }
    }
}

// End FarragoJdbcApproxParamDef.java
