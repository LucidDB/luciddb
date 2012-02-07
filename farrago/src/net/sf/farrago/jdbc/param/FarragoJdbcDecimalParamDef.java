/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
