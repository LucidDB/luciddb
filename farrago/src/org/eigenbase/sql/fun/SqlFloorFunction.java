/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
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
package org.eigenbase.sql.fun;

import org.eigenbase.sql.*;
import org.eigenbase.sql.test.SqlOperatorTests;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.sql.type.OperandsTypeChecking;
import org.eigenbase.sql.type.ReturnTypeInferenceImpl;
import org.eigenbase.sql.validate.SqlValidatorScope;

/**
 * Definition of the "FLOOR" builtin SQL function.
 *
 * @author jack
 * @since May 28, 2004
 * @version $Id$
 **/
public class SqlFloorFunction extends SqlFunction
{
    public SqlFloorFunction()
    {
        super("FLOOR", SqlKind.Function,
            ReturnTypeInferenceImpl.useFirstArgType,
            null,
            OperandsTypeChecking.typeNumeric,
            SqlFunctionCategory.Numeric);

    }

    public void test(SqlTester tester)
    {
        SqlOperatorTests.testFloorFunc(tester);
    }

    public boolean isMonotonic(SqlCall call, SqlValidatorScope scope)
    {
        SqlNode node = (SqlNode)call.operands[0];
        return scope.isMonotonic(node);
    }

}
