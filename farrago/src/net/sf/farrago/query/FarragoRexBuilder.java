/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/
package net.sf.farrago.query;

import net.sf.farrago.type.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.cwm.behavioral.*;

import org.eigenbase.oj.util.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.reltype.*;

import java.util.*;

/**
 * FarragoRexBuilder refines JavaRexBuilder with Farrago-specific details.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FarragoRexBuilder extends JavaRexBuilder
{
    private final FarragoPreparingStmt preparingStmt;
    
    //~ Constructors ----------------------------------------------------------

    FarragoRexBuilder(FarragoPreparingStmt preparingStmt)
    {
        super(preparingStmt.getFarragoTypeFactory());

        this.preparingStmt = preparingStmt;
    }

    //~ Methods ---------------------------------------------------------------

    // override JavaRexBuilder
    public RexLiteral makeLiteral(String s)
    {
        return makePreciseStringLiteral(s);
    }

    // override RexBuilder
    public RexNode makeCall(
        SqlOperator op,
        RexNode [] exprs)
    {
        if (!(op instanceof FarragoUserDefinedRoutine)) {
            return super.makeCall(op, exprs);
        }

        FarragoUserDefinedRoutine routine = (FarragoUserDefinedRoutine) op;
        FemRoutine femRoutine = routine.getFemRoutine();

        if (femRoutine.getBody() == null) {
            // leave external routines invocations as calls
            return super.makeCall(op, exprs);
        }

        // replace calls to SQL-defined routines by
        // inline expansion of body
        assert(femRoutine.getBody().getLanguage().equals("SQL"));

        Map paramNameToArgMap = new HashMap();
        Map paramNameToTypeMap = new HashMap();
        RelDataType [] paramTypes = routine.getParamTypes();

        List paramNames = new ArrayList();
        Iterator paramIter = femRoutine.getParameter().iterator();
        for (int i = 0; paramIter.hasNext(); ++i) {
            FemRoutineParameter param = (FemRoutineParameter) paramIter.next();
            if (param.getKind() == ParameterDirectionKindEnum.PDK_RETURN) {
                break;
            }
            paramNames.add(param.getName());
            RexNode argCast = makeCast(paramTypes[i], exprs[i]);
            paramNameToArgMap.put(param.getName(), argCast);
            paramNameToTypeMap.put(param.getName(), paramTypes[i]);
        }

        RexNode returnNode = preparingStmt.expandFunction(
            femRoutine.getBody().getBody(),
            paramNameToArgMap,
            paramNameToTypeMap);

        RexNode returnCast = makeCast(routine.getReturnType(), returnNode);
        return returnCast;
    }
}

// End FarragoRexBuilder.java
