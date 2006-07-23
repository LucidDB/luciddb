/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2005-2005 John V. Sichi
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
package net.sf.farrago.query;

import java.util.*;

import net.sf.farrago.cwm.behavioral.*;
import net.sf.farrago.fem.sql2003.*;

import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


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
        List paramNames = new ArrayList();
        Iterator paramIter = routine.getFemRoutine().getParameter().iterator();
        for (int i = 0; paramIter.hasNext(); ++i) {
            FemRoutineParameter param = (FemRoutineParameter) paramIter.next();
            if (param.getKind() == ParameterDirectionKindEnum.PDK_RETURN) {
                break;
            }
            paramNames.add(param.getName());
            RexBuilder rexBuilder =
                routine.getPreparingStmt().getSqlToRelConverter()
                .getRexBuilder();
            RexNode argCast = rexBuilder.makeCast(
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
