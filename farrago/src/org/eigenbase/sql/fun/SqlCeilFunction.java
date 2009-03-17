/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
 * Support for the CEIL/CEILING builtin function.
 *
 * @author jack
 * @version $Id$
 * @since Apr 13, 2005
 */
public class SqlCeilFunction
    extends SqlFunction
{
    //~ Constructors -----------------------------------------------------------

    public SqlCeilFunction()
    {
        super(
            "CEIL",
            SqlKind.Function,
            SqlTypeStrategies.rtiFirstArgType,
            null,
            SqlTypeStrategies.otcNumeric,
            SqlFunctionCategory.Numeric);
    }

    //~ Methods ----------------------------------------------------------------

    public SqlMonotonicity getMonotonicity(
        SqlCall call,
        SqlValidatorScope scope)
    {
        SqlNode node = (SqlNode) call.operands[0];
        return scope.getMonotonicity(node);
    }
}

// End SqlCeilFunction.java
