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
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.parser.SqlParserPos;

/**
 * The <code>COALESCE</code> function.
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class SqlCoalesceFunction extends SqlFunction
{
    public SqlCoalesceFunction()
    {
        super("COALESCE", SqlKind.Function, null, null, null,
            SqlFunctionCategory.System);
    }

    // override SqlOperator
    public SqlNode rewriteCall(SqlValidator validator, SqlCall call)
    {
        validateQuantifier(validator, call); // check DISTINCT/ALL

        SqlNode [] operands = call.getOperands();
        SqlParserPos pos = call.getParserPosition();

        SqlNodeList whenList = new SqlNodeList(pos);
        SqlNodeList thenList = new SqlNodeList(pos);

        //todo optimize when know operand is not null.

        for (int i = 0; i + 1 < operands.length; ++i) {
            whenList.add(
                SqlStdOperatorTable.isNotNullOperator.createCall(
                    operands[i], pos));
            thenList.add(operands[i]);
        }
        SqlNode elseExpr = operands[operands.length - 1];
        assert call.getFunctionQuantifier() == null;
        final SqlCall newCall =
            SqlStdOperatorTable.caseOperator.createCall(
                null, whenList, thenList, elseExpr, pos);
        return newCall;
    }

    // REVIEW jvs 1-Jan-2005:  should this be here?  It's
    // not entirely accurate.
    public SqlOperandCountRange getOperandCountRange()
    {
        return SqlOperandCountRange.Two;
    }
}

// End SqlCoalesceFunction.java
