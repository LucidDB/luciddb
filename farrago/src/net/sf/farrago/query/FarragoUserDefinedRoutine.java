/*
// $Id$
// Farrago is a relational database management system.
// Copyright (C) 2004-2004 John V. Sichi.
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

import net.sf.farrago.fem.sql2003.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.reltype.*;

/**
 * FarragoUserDefinedRoutine extends {@link SqlFunction} with a
 * repository reference to a specific user-defined routine.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoUserDefinedRoutine extends SqlFunction
{
    private final FemRoutine routine;

    private final RelDataType returnType;
    
    private final RelDataType [] paramTypes;

    public FarragoUserDefinedRoutine(
        FemRoutine routine,
        RelDataType returnType,
        RelDataType [] paramTypes)
    {
        super(
            routine.getName(),
            SqlKind.Function,
            new ReturnTypeInferenceImpl.FixedReturnTypeInference(returnType),
            new ExplicitParamInference(paramTypes),
            new AssignableOperandsTypeChecking(paramTypes),
            SqlFunction.SqlFuncTypeName.User);
        this.routine = routine;
        this.returnType = returnType;
        this.paramTypes = paramTypes;
    }
    
    public FemRoutine getFemRoutine()
    {
        return routine;
    }

    public RelDataType getReturnType()
    {
        return returnType;
    }

    public RelDataType [] getParamTypes()
    {
        return paramTypes;
    }
}

// End FarragoUserDefinedRoutine.java
