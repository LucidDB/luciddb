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
package net.sf.farrago.jdbc.engine;

import java.sql.*;

import net.sf.farrago.jdbc.param.*;
import net.sf.farrago.session.*;

import org.eigenbase.reltype.*;


/**
 * FarragoJdbcEngineParamDefFactory implements {@link
 * FarragoSessionStmtParamDefFactory} for JDBC.
 *
 * @author stephan
 * @version $Id$
 */
public class FarragoJdbcEngineParamDefFactory
    implements FarragoSessionStmtParamDefFactory
{
    //~ Methods ----------------------------------------------------------------

    // Implement FarragoSessionStmtParamDefFactory
    public FarragoSessionStmtParamDef newParamDef(
        String paramName,
        RelDataType type)
    {
        FarragoParamFieldMetaData paramMetaData =
            FarragoParamFieldMetaDataFactory.newParamFieldMetaData(
                type,
                ParameterMetaData.parameterModeIn);

        FarragoJdbcParamDef param =
            FarragoJdbcParamDefFactory.instance.newParamDef(
                paramName,
                paramMetaData,
                false);
        return new FarragoJdbcEngineParamDef(param, type);
    }
}

// End FarragoJdbcEngineParamDefFactory.java
