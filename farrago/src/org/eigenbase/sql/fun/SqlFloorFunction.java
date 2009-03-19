/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2002-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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
 * Definition of the "FLOOR" builtin SQL function.
 *
 * @author jack
 * @version $Id$
 * @since May 28, 2004
 */
public class SqlFloorFunction
    extends SqlFunction
{
    //~ Constructors -----------------------------------------------------------

    public SqlFloorFunction()
    {
        super(
            "FLOOR",
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
        // Monotonic iff its first argument is, but not strict.
        SqlNode node = (SqlNode) call.operands[0];
        return scope.getMonotonicity(node).unstrict();
    }
}

// End SqlFloorFunction.java
