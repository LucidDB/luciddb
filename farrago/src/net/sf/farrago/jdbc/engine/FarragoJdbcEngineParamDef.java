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

import java.util.*;

import net.sf.farrago.jdbc.param.*;
import net.sf.farrago.session.*;

import org.eigenbase.reltype.*;


/**
 * Enforces constraints on parameters. The constraints are:
 *
 * <ol>
 * <li>Ensures that null values cannot be inserted into not-null columns.
 * <li>Ensures that value is the right type.
 * <li>Ensures that the value is within range. For example, you can't insert a
 * 10001 into a DECIMAL(5) column.
 * </ol>
 *
 * <p>TODO: Actually enfore these constraints.
 *
 * @author Julian Hyde
 * @version $Id$
 */
class FarragoJdbcEngineParamDef
    implements FarragoSessionStmtParamDef
{
    //~ Instance fields --------------------------------------------------------

    final FarragoJdbcParamDef param;
    final RelDataType type;

    //~ Constructors -----------------------------------------------------------

    FarragoJdbcEngineParamDef(FarragoJdbcParamDef param, RelDataType type)
    {
        this.param = param;
        this.type = type;
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionStmtParamDef
    public String getParamName()
    {
        return param.getParamName();
    }

    // implement FarragoSessionStmtParamDef
    public RelDataType getParamType()
    {
        return type;
    }

    // implement FarragoSessionStmtParamDef
    public Object scrubValue(Object x)
    {
        return param.scrubValue(x);
    }

    // implement FarragoSessionStmtParamDef
    public Object scrubValue(Object x, Calendar cal)
    {
        return param.scrubValue(x, cal);
    }
}

// End FarragoJdbcEngineParamDef.java
