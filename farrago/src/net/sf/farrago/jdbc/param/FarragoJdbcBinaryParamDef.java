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
 * FarragoJdbcEngineBinaryParamDef defines a binary parameter. Only accepts
 * byte-array values. This class is JDK 1.4 compatible.
 *
 * @author Julian Hyde
 * @version $Id$
 */
class FarragoJdbcBinaryParamDef
    extends FarragoJdbcParamDef
{
    //~ Instance fields --------------------------------------------------------

    private final int maxByteCount;

    //~ Constructors -----------------------------------------------------------

    public FarragoJdbcBinaryParamDef(
        String paramName,
        FarragoParamFieldMetaData paramMetaData)
    {
        super(paramName, paramMetaData);
        maxByteCount = paramMetaData.precision;
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionStmtParamDef
    public Object scrubValue(Object x)
    {
        if (x == null) {
            checkNullable();
            return x;
        }
        if (!(x instanceof byte [])) {
            throw newInvalidType(x);
        }
        final byte [] bytes = (byte []) x;
        if (bytes.length > maxByteCount) {
            throw newValueTooLong(
                ConversionUtil.toStringFromByteArray(bytes, 16));
        }
        return bytes;
    }
}

// End FarragoJdbcBinaryParamDef.java
