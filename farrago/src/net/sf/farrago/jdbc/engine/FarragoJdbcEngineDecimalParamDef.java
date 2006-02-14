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
package net.sf.farrago.jdbc.engine;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.util14.NumberUtil;
import net.sf.farrago.resource.FarragoResource;

/**
 * FarragoJdbcEngineDecimalParamDef defines a Decimal parameter.
 * 
 * @author Angel Chang
 * @version $Id$
 */
class FarragoJdbcEngineDecimalParamDef extends FarragoJdbcEngineParamDef
{
    final BigInteger maxUnscaled;
    final BigInteger minUnscaled;

    FarragoJdbcEngineDecimalParamDef(
        String paramName,
        RelDataType type)
    {
        super(paramName, type);
        maxUnscaled = NumberUtil.getMaxUnscaled(type.getPrecision());
        minUnscaled = NumberUtil.getMinUnscaled(type.getPrecision());
    }

    private BigDecimal getBigDecimal(Object value, int scale)
    {
        BigDecimal bd;
        if (value == null) {
            return null;
        } else if (value instanceof Number) {
            bd = NumberUtil.toBigDecimal((Number) value);
        } else if (value instanceof Boolean) {
            bd = new BigDecimal(((Boolean) value).booleanValue() ? 1 : 0);
        } else {
            try {
                bd = new BigDecimal(value.toString().trim());
            } catch (NumberFormatException ex) {
                throw FarragoResource.instance().ParameterValueIncompatible.ex(
                        value.toString(), type.toString());
            }
        }
        bd = NumberUtil.rescaleBigDecimal(bd, scale);
        return bd;
    }

    // implement FarragoSessionStmtParamDef
    public Object scrubValue(Object x)
    {
        BigDecimal n = getBigDecimal(x, type.getScale());
        if (n != null) {
            BigInteger usv = n.unscaledValue();
            if ((usv.compareTo(maxUnscaled) > 0) ||
                (usv.compareTo(minUnscaled) < 0)) {
                throw FarragoResource.instance().ParameterValueOutOfRange.ex(
                        x.toString(), type.toString());
            }
        }
        return n;
    }
}