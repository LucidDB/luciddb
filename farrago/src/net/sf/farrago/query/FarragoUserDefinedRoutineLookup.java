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

import net.sf.farrago.session.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.cwm.relational.enumerations.*;
import net.sf.farrago.cwm.behavioral.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.reltype.*;
import org.eigenbase.util.*;

import java.util.*;

/**
 * FarragoUserDefinedRoutineLookup implements the {@link SqlOperatorTable}
 * interface by looking up user-defined functions from the repository.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoUserDefinedRoutineLookup implements SqlOperatorTable
{
    private final FarragoSessionStmtValidator stmtValidator;
    
    public FarragoUserDefinedRoutineLookup(
        FarragoSessionStmtValidator stmtValidator)
    {
        this.stmtValidator = stmtValidator;
    }

    // implement SqlOperatorTable
    public List lookupOperatorOverloads(String opName, SqlSyntax syntax)
    {
        if (syntax != SqlSyntax.Function) {
            return Collections.EMPTY_LIST;
        }
        
        // TODO jvs 1-Jan-2005:  schema qualifier; generalize to
        // procedure calls as well
        List list = stmtValidator.findRoutineOverloads(
            null,
            opName,
            ProcedureTypeEnum.FUNCTION);
        List overloads = new ArrayList();
        Iterator iter = list.iterator();
        while (iter.hasNext()) {
            FemRoutine femFunction = (FemRoutine) iter.next();
            SqlFunction sqlFunction = convertFunction(femFunction);
            overloads.add(sqlFunction);
        }
        return overloads;
    }
    
    // implement SqlOperatorTable
    public List getOperatorList()
    {
        // NOTE jvs 1-Jan-2005:  I don't think we'll ever need this.
        throw Util.needToImplement(this);
    }

    private SqlFunction convertFunction(FemRoutine femFunction)
    {
        if (femFunction.getVisibility() == null) {
            // Oops, the referenced routine hasn't been validated yet.
            // Throw a special exception and someone up above will
            // figure out what to do.
            throw new FarragoUnvalidatedDependencyException();
        }

        List parameters = femFunction.getParameter();
        
        // minus one because last param represents return type
        int nParams = parameters.size() - 1;

        RelDataType [] paramTypes = new RelDataType[nParams];
        Iterator paramIter = parameters.iterator();
        RelDataType returnType = null;
        for (int i = 0; paramIter.hasNext(); ++i) {
            FemRoutineParameter param = (FemRoutineParameter)
                paramIter.next();
            RelDataType type =
                stmtValidator.getTypeFactory().createCwmElementType(
                    param);
            if (param.getKind() == ParameterDirectionKindEnum.PDK_RETURN) {
                returnType = type;
            } else {
                paramTypes[i] = type;
            }
        }
            
        SqlFunction sqlFunction = new FarragoUserDefinedRoutine(
            femFunction,
            returnType,
            paramTypes);
        return sqlFunction;
    }
}

// End FarragoUserDefinedRoutineLookup.java
