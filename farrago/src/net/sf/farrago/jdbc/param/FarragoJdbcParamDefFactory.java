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

import java.sql.*;


/**
 * Factory for {@link FarragoJdbcParamDef} objects.
 *
 * <p>Refactored from FarragoJdbcEngineParamDefFactory.
 *
 * <p>This class is JDK 1.4 compatible.
 *
 * @author Angel Chang
 * @version $Id$
 */
public class FarragoJdbcParamDefFactory
{
    //~ Static fields/initializers ---------------------------------------------

    public static final FarragoJdbcParamDefFactory instance =
        new FarragoJdbcParamDefFactory();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FarragoJdbcParamDefFactory.
     */
    private FarragoJdbcParamDefFactory()
    {
    }

    //~ Methods ----------------------------------------------------------------

    public FarragoJdbcParamDef newParamDef(
        String paramName,
        FarragoParamFieldMetaData paramMetaData,
        boolean useFennelTuple)
    {
        if (useFennelTuple) {
            return newFennelTupleParamDef(paramName, paramMetaData);
        } else {
            return newParamDef(paramName, paramMetaData);
        }
    }

    private static FarragoJdbcParamDef newFennelTupleParamDef(
        String paramName,
        FarragoParamFieldMetaData paramMetaData)
    {
        FarragoJdbcParamDef paramDef = newParamDef(paramName, paramMetaData);
        return new FarragoJdbcFennelTupleParamDef(
            paramName,
            paramMetaData,
            paramDef);
    }

    private static FarragoJdbcParamDef newParamDef(
        String paramName,
        FarragoParamFieldMetaData paramMetaData)
    {
        switch (paramMetaData.type) {
        case Types.BIT:
        case Types.BOOLEAN:
            return new FarragoJdbcBooleanParamDef(paramName, paramMetaData);
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.BIGINT:
            return new FarragoJdbcIntParamDef(paramName, paramMetaData);
        case Types.REAL:
        case Types.FLOAT:
        case Types.DOUBLE:
            return new FarragoJdbcApproxParamDef(paramName, paramMetaData);
        case Types.DECIMAL:
        case Types.NUMERIC:
            return new FarragoJdbcDecimalParamDef(paramName, paramMetaData);
        case Types.CHAR:
        case Types.VARCHAR:
            return new FarragoJdbcStringParamDef(paramName, paramMetaData);
        case Types.BINARY:
        case Types.VARBINARY:
            return new FarragoJdbcBinaryParamDef(paramName, paramMetaData);
        case Types.DATE:
            return new FarragoJdbcDateParamDef(paramName, paramMetaData);
        case Types.TIMESTAMP:
            return new FarragoJdbcTimestampParamDef(paramName, paramMetaData);
        case Types.TIME:
            return new FarragoJdbcTimeParamDef(paramName, paramMetaData);
        default:
            return new FarragoJdbcParamDef(paramName, paramMetaData);
        }
    }
}

// End FarragoJdbcParamDefFactory.java
