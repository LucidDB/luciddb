/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
