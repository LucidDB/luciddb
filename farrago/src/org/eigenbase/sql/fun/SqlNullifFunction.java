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
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.test.SqlOperatorTests;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.util.Util;

/**
 * The <code>NULLIF</code> function.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SqlNullifFunction extends SqlFunction
{
    public SqlNullifFunction()
    {
        super("NULLIF", SqlKind.Function, null, null, null,
            SqlFunctionCategory.System);
    }

    // override SqlOperator
    public SqlNode rewriteCall(SqlCall call)
    {
        SqlNode [] operands = call.getOperands();
        SqlParserPos pos = call.getParserPosition();

        if (2 != operands.length) {
            throw Util.newInternal("Invalid arg count: " + call);
        }

        SqlNodeList whenList = new SqlNodeList(pos);
        SqlNodeList thenList = new SqlNodeList(pos);
        whenList.add(operands[1]);
        thenList.add(SqlLiteral.createNull(SqlParserPos.ZERO));
        return SqlStdOperatorTable.caseOperator.createCall(
            operands[0], whenList,
            thenList, operands[0], pos);
    }

    public SqlOperator.OperandsCountDescriptor getOperandsCountDescriptor()
    {
        return new OperandsCountDescriptor(2);
    }

    public void test(SqlTester tester)
    {
        SqlOperatorTests.testNullifFunc(tester);
    }
}

// End SqlNullifFunction.java
