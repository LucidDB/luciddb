/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package org.eigenbase.sql.fun;

import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;


/**
 * Base class for functions such as "USER", "CURRENT_ROLE", and "CURRENT_PATH".
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SqlStringContextVariable
    extends SqlFunction
{
    //~ Constructors -----------------------------------------------------------

    protected SqlStringContextVariable(String name)
    {
        super(
            name,
            SqlKind.Function,
            SqlTypeStrategies.rtiVarchar2000,
            null,
            SqlTypeStrategies.otcNiladic,
            SqlFunctionCategory.System);
    }

    //~ Methods ----------------------------------------------------------------

    public SqlSyntax getSyntax()
    {
        return SqlSyntax.FunctionId;
    }

    // All of the string constants are monotonic.
    public boolean isMonotonic(SqlCall call, SqlValidatorScope scope)
    {
        return true;
    }

    // Plans referencing context variables should never be cached
    public boolean isDynamicFunction()
    {
        return true;
    }
}

// End SqlStringContextVariable.java
