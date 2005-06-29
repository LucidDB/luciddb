/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
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

package org.eigenbase.sql;

import org.eigenbase.sql.type.*;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.sql.test.SqlOperatorTests;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.validate.SqlValidatorScope;
import org.eigenbase.util.Util;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import openjava.mop.OJClass;


/**
 * Operator which aggregates sets of values into a result.
 *
 * @author jack
 * @version $Id$
 * @since Jun 3, 2005
 */public class SqlRankFunction extends SqlAggFunction
{
    private final RelDataType type = null;

    public SqlRankFunction( String name)
    {
        super( name, SqlKind.Function,
            SqlTypeStrategies.rtiInteger,
            null,
            SqlTypeStrategies.otcNiladic,
            SqlFunctionCategory.Numeric);
    }

    public SqlOperandCountRange getOperandCountRange()
    {
        return SqlOperandCountRange.Zero;
    }

    public RelDataType getReturnType(RelDataTypeFactory typeFactory)
    {
        return type;
    }

    public RelDataType[] getParameterTypes(RelDataTypeFactory typeFactory)
    {
        return new RelDataType [] { type };
    }
    
    public boolean isAggregator()
    {
        return true;
    }

}

// End SqlAggFunction.java
