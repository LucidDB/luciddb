/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
package net.sf.farrago.jdbc.engine;

import net.sf.farrago.resource.FarragoResource;
import net.sf.farrago.session.FarragoSessionStmtParamDef;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.util.EigenbaseException;

/**
 * Enforces constraints on parameters.
 *
 * The constraints are:<ol>
 *
 * <li>Ensures that null values cannot be inserted into not-null columns.
 *
 * <li>Ensures that value is the right type.
 *
 * <li>Ensures that the value is within range. For example, you can't
 *    insert a 10001 into a DECIMAL(5) column.
 *
 * </ol>
 *
 * <p>TODO: Actually enfore these constraints.
 * 
 * @author Julian Hyde
 * @version $Id$
 */
class FarragoJdbcEngineParamDef implements FarragoSessionStmtParamDef
{
    final RelDataType type;
    final String paramName;

    FarragoJdbcEngineParamDef(String paramName, RelDataType type)
    {
        this.type = type;
        this.paramName = paramName;
    }

    // implement FarragoSessionStmtParamDef
    public String getParamName()
    {
        return paramName;
    }
    
    // implement FarragoSessionStmtParamDef
    public RelDataType getParamType()
    {
        return type;
    }
    
    // implement FarragoSessionStmtParamDef
    public Object scrubValue(Object x)
    {
        return x;
    }

    /**
     * Returns an error that the value is not valid for the desired SQL
     * type.
     */
    protected EigenbaseException newInvalidType(Object x)
    {
        return FarragoResource.instance().ParameterValueIncompatible.ex(
            x.getClass().getName(),
            type.toString());
    }
}