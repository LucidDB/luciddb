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
 * The <code>POSITION</code> function.
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class SqlPositionFunction extends SqlFunction
{
    public SqlPositionFunction()
    {
        super("POSITION", SqlKind.Function,
            SqlTypeStrategies.rtiNullableInteger, null,
            SqlTypeStrategies.otcNullableStringX2,
            SqlFunctionCategory.Numeric);
    }
    
    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        writer.print(getName());
        writer.print("(");
        operands[0].unparse(writer, leftPrec, rightPrec);
        writer.print(" IN ");
        operands[1].unparse(writer, leftPrec, rightPrec);
        writer.print(")");
    }

    protected String getSignatureTemplate(final int operandsCount)
    {
        switch (operandsCount) {
        case 2:
            return "{0}({1} IN {2})";
        }
        assert (false);
        return null;
    }

    protected boolean checkArgTypes(
        SqlCall call,
        SqlValidator validator,
        SqlValidatorScope scope,
        boolean throwOnFailure)
    {
        //check that the two operands are of same type.
        RelDataType type0 =
            validator.getValidatedNodeType(call.operands[0]);
        RelDataType type1 =
            validator.getValidatedNodeType(call.operands[1]);
        if (!SqlTypeUtil.inSameFamily(type0, type1)) {
            if (throwOnFailure) {
                throw call.newValidationSignatureError(
                    validator, scope);
            }
            return false;
        }

        return operandsCheckingRule.check(
            validator, scope, call, throwOnFailure);
    }

    public void test(SqlTester tester)
    {
        SqlOperatorTests.testPositionFunc(tester);
    }
}

// End SqlPositionFunction.java
