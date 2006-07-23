/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
// Portions Copyright (C) 2006-2006 John V. Sichi
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

import org.eigenbase.util14.*;


/**
 * FarragoJdbcEngineIntParamDef defines a integer parameter.
 *
 * @author Angel Chang
 * @version $Id$
 */
class FarragoJdbcIntParamDef
    extends FarragoJdbcParamDef
{

    //~ Instance fields --------------------------------------------------------

    final long min;
    final long max;

    //~ Constructors -----------------------------------------------------------

    FarragoJdbcIntParamDef(
        String paramName,
        FarragoParamFieldMetaData paramMetaData)
    {
        super(paramName, paramMetaData);

        switch (paramMetaData.type) {
        case Types.TINYINT:
            min = Byte.MIN_VALUE;
            max = Byte.MAX_VALUE;
            break;
        case Types.SMALLINT:
            min = Short.MIN_VALUE;
            max = Short.MAX_VALUE;
            break;
        case Types.INTEGER:
            min = Integer.MIN_VALUE;
            max = Integer.MAX_VALUE;
            break;
        case Types.BIGINT:
            min = Long.MIN_VALUE;
            max = Long.MAX_VALUE;
            break;
        default:
            min = 0;
            max = 0;
            assert (false) : "Integral paramMetaData expected";
        }
    }

    //~ Methods ----------------------------------------------------------------

    private long getLong(Object value)
    {
        if (value instanceof Number) {
            Number n = (Number) value;
            return NumberUtil.round(n.doubleValue());
        } else if (value instanceof Boolean) {
            return (((Boolean) value).booleanValue() ? 1 : 0);
        } else if (value instanceof String) {
            try {
                BigDecimal bd = new BigDecimal(value.toString().trim());
                return getLong(bd);
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
            long n = getLong(x);
            checkRange(n, min, max);
            return new Long(n);
        }
    }
}

// End FarragoJdbcIntParamDef.java
