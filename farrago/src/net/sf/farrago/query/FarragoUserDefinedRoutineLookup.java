/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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

import net.sf.farrago.session.*;
import net.sf.farrago.catalog.*;
import net.sf.farrago.type.*;

import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.cwm.core.*;
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
    
    private final FarragoPreparingStmt preparingStmt;

    private final FemRoutine validatingRoutine;
    
    public FarragoUserDefinedRoutineLookup(
        FarragoSessionStmtValidator stmtValidator,
        FarragoPreparingStmt preparingStmt,
        FemRoutine validatingRoutine)
    {
        this.stmtValidator = stmtValidator;
        this.preparingStmt = preparingStmt;
        this.validatingRoutine = validatingRoutine;
    }

    // implement SqlOperatorTable
    public List lookupOperatorOverloads(
        SqlIdentifier opName,
        SqlFunctionCategory category,
        SqlSyntax syntax)
    {
        if ((preparingStmt != null) && preparingStmt.isExpandingDefinition()) {
            // While expanding view and function bodies, an unqualified name is
            // assumed to be a builtin, because we explicitly qualify
            // everything else when the definition is stored.  We could
            // qualify the builtins with INFORMATION_SCHEMA, but we
            // currently don't.
            if (opName.names.length == 1) {
                return Collections.EMPTY_LIST;
            }
        }
        if (category == SqlFunctionCategory.UserDefinedSpecificFunction) {
            // Look up by specific name instead of invocation name.
            FemRoutine femRoutine = (FemRoutine) stmtValidator.findSchemaObject(
                opName,
                stmtValidator.getRepos().getSql2003Package().getFemRoutine());
            List overloads = new ArrayList();
            if (femRoutine.getType() == ProcedureTypeEnum.FUNCTION) {
                overloads.add(convertRoutine(femRoutine));
            }
            return overloads;
        }
        
        if (syntax != SqlSyntax.Function) {
            return Collections.EMPTY_LIST;
        }
        
        List list = stmtValidator.findRoutineOverloads(
            opName,
            null);
        List overloads = new ArrayList();
        Iterator iter = list.iterator();
        while (iter.hasNext()) {
            FemRoutine femRoutine = (FemRoutine) iter.next();
            if (category == SqlFunctionCategory.UserDefinedFunction) {
                if (femRoutine.getType() != ProcedureTypeEnum.FUNCTION) {
                    continue;
                }
            } else if (category == SqlFunctionCategory.UserDefinedProcedure) {
                if (femRoutine.getType() != ProcedureTypeEnum.PROCEDURE) {
                    continue;
                }
            }
            if (femRoutine.getVisibility() == null) {
                // Oops, the referenced routine hasn't been validated yet.  If
                // requested, throw a special exception and someone up
                // above will figure out what to do.
                if (validatingRoutine == null) {
                    throw new FarragoUnvalidatedDependencyException();
                }
                if (femRoutine != validatingRoutine) {
                    // just skip this one for now; if there's a conflict,
                    // we'll hit it by symmetry
                    continue;
                }
            }
            SqlFunction sqlFunction = convertRoutine(femRoutine);
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

    /**
     * Converts the validated catalog definition of a routine into
     * SqlFunction representation.
     *
     * @param femRoutine catalog definition
     *
     * @return converted function
     */
    public FarragoUserDefinedRoutine convertRoutine(FemRoutine femRoutine)
    {
        int nParams = FarragoCatalogUtil.getRoutineParamCount(femRoutine);
        FarragoTypeFactory typeFactory = stmtValidator.getTypeFactory();

        RelDataType [] paramTypes = new RelDataType[nParams];
        Iterator paramIter = femRoutine.getParameter().iterator();
        RelDataType returnType = null;
        for (int i = 0; paramIter.hasNext(); ++i) {
            FemRoutineParameter param = (FemRoutineParameter)
                paramIter.next();
            RelDataType type = typeFactory.createCwmElementType(param);
            
            if (param.getKind() == ParameterDirectionKindEnum.PDK_RETURN) {
                returnType = type;
            } else {
                paramTypes[i] = type;
            }
        }

        if (returnType == null) {
            // for procedures, we make up a dummy return type to allow
            // invocations to be rewritten as functions
            returnType = typeFactory.createSqlType(SqlTypeName.Integer);
            returnType = typeFactory.createTypeWithNullability(
                returnType, true);
        }

        if (FarragoCatalogUtil.isRoutineConstructor(femRoutine)) {
            // constructors always return NOT NULL
            returnType = typeFactory.createTypeWithNullability(
                returnType, false);
        }
        
        return new FarragoUserDefinedRoutine(
            stmtValidator,
            preparingStmt,
            femRoutine,
            returnType,
            paramTypes);
    }
}

// End FarragoUserDefinedRoutineLookup.java
