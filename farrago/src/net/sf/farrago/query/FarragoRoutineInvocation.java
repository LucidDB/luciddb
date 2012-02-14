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
package net.sf.farrago.query;

import java.util.*;

import net.sf.farrago.cwm.behavioral.*;
import net.sf.farrago.fem.sql2003.*;

import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.*;


/**
 * FarragoRoutineInvocation represents an invocation of a
 * FarragoUserDefinedRoutine.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoRoutineInvocation
{
    //~ Instance fields --------------------------------------------------------

    private final FarragoUserDefinedRoutine routine;
    private final RexNode [] argExprs;
    private final RexNode [] argCastExprs;
    private final Map<String, RexNode> paramNameToArgMap =
        new HashMap<String, RexNode>();
    private final Map<String, RelDataType> paramNameToTypeMap =
        new HashMap<String, RelDataType>();

    //~ Constructors -----------------------------------------------------------

    public FarragoRoutineInvocation(
        FarragoUserDefinedRoutine routine,
        RexNode [] argExprs)
    {
        this.routine = routine;
        this.argExprs = argExprs;

        RelDataType [] paramTypes = routine.getParamTypes();
        argCastExprs = new RexNode[argExprs.length];
        List<String> paramNames = new ArrayList<String>();
        int i = -1;
        for (
            CwmParameter param
            : Util.cast(
                routine.getFemRoutine().getParameter(),
                FemRoutineParameter.class))
        {
            ++i;
            if (param.getKind() == ParameterDirectionKindEnum.PDK_RETURN) {
                break;
            }
            paramNames.add(param.getName());
            RexBuilder rexBuilder =
                routine.getPreparingStmt().getSqlToRelConverter()
                .getRexBuilder();
            RexNode argCast =
                rexBuilder.makeCast(
                    paramTypes[i],
                    argExprs[i]);
            paramNameToArgMap.put(
                param.getName(),
                argCast);
            paramNameToTypeMap.put(
                param.getName(),
                paramTypes[i]);
            argCastExprs[i] = argCast;
        }
    }

    //~ Methods ----------------------------------------------------------------

    public RexNode [] getArgCastExprs()
    {
        return argCastExprs;
    }

    public Map<String, RexNode> getParamNameToArgMap()
    {
        return paramNameToArgMap;
    }

    public Map<String, RelDataType> getParamNameToTypeMap()
    {
        return paramNameToTypeMap;
    }
}

// End FarragoRoutineInvocation.java
