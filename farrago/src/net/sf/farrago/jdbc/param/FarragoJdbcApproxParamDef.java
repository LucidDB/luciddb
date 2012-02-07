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

    private Float getFloat(Object value)
    {
        if (value instanceof Number) {
            Number n = (Number) value;
            checkRange(
                n.floatValue(),
                min,
                max);
            return new Float(n.floatValue());
        } else if (value instanceof Boolean) {
            return (((Boolean) value).booleanValue() ? new Float(1)
                : new Float(0));
        } else if (value instanceof String) {
            try {
                BigDecimal bd = new BigDecimal(value.toString().trim());
                return getFloat(bd);
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
            switch (paramMetaData.type) {
            case Types.REAL:
                return getFloat(x);
            case Types.FLOAT:
            case Types.DOUBLE:
                return getDouble(x);
            default:
                throw new AssertionError("bad type " + paramMetaData.type);
            }
        }
    }
}

// End FarragoJdbcApproxParamDef.java
