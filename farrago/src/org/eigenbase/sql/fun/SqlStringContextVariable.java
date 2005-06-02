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

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.test.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;

import java.util.*;

/**
 * Abstract base class for functions such as "USER", "CURRENT_ROLE",
 * and "CURRENT_PATH".
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class SqlStringContextVariable extends SqlFunction
{
    protected SqlStringContextVariable(String name)
    {
        super(
            name, SqlKind.Function, ReturnTypeInferenceImpl.useVarchar2000,
            null, OperandsTypeChecking.typeEmpty,
            SqlFunctionCategory.System);
    }

    public OperandsCountDescriptor getOperandsCountDescriptor()
    {
        return OperandsCountDescriptor.niladicCountDescriptor;
    }

    public SqlSyntax getSyntax()
    {
        return SqlSyntax.FunctionId;
    }

    // All of the string constants are monotonic.
    public boolean isMonotonic(SqlCall call, SqlValidatorScope scope)
    {
        return true;
    }
}

// End SqlStringContextVariable.java
