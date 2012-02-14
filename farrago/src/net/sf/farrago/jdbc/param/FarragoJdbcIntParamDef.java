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

import java.sql.*;

import org.eigenbase.util14.*;


/**
 * FarragoJdbcEngineIntParamDef defines a integer parameter. This class is JDK
 * 1.4 compatible.
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
        if (value instanceof Long) {
            // Case "value instanceof Number" below is not sufficient for Long:
            // conversion via double loses precision for values > 2^48. OK for
            // other types, including int and float.
            return ((Long) value).longValue();
        } else if (value instanceof Number) {
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
            switch (paramMetaData.type) {
            case Types.TINYINT:
                return new Byte((byte) n);
            case Types.SMALLINT:
                return new Short((short) n);
            case Types.INTEGER:
                return new Integer((int) n);
            case Types.BIGINT:
                return new Long(n);
            default:
                throw new AssertionError("bad type " + paramMetaData.type);
            }
        }
    }
}

// End FarragoJdbcIntParamDef.java
