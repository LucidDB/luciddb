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

import org.eigenbase.util14.*;


/**
 * FarragoJdbcEngineBooleanParamDef defines a boolean parameter. This class is
 * JDK 1.4 compatible.
 *
 * @author Angel Chang
 * @version $Id$
 */
class FarragoJdbcBooleanParamDef
    extends FarragoJdbcParamDef
{
    //~ Constructors -----------------------------------------------------------

    FarragoJdbcBooleanParamDef(
        String paramName,
        FarragoParamFieldMetaData paramMetaData)
    {
        super(paramName, paramMetaData);
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionStmtParamDef
    public Object scrubValue(Object x)
    {
        if (x == null) {
            checkNullable();
            return null;
        } else {
            if (x instanceof Boolean) {
                return x;
            } else if (x instanceof Number) {
                Number n = (Number) x;
                return Boolean.valueOf(n.longValue() != 0);
            } else if (x instanceof String) {
                try {
                    return ConversionUtil.toBoolean((String) x);
                } catch (Exception e) {
                    // Convert string to number, return false if zero
                    try {
                        String str = ((String) x).trim();
                        double d = Double.parseDouble(str);
                        return Boolean.valueOf(d != 0);
                    } catch (NumberFormatException ex) {
                        throw newInvalidFormat(x);
                    }
                }
            } else {
                throw newInvalidType(x);
            }
        }
    }
}

// End FarragoJdbcBooleanParamDef.java
