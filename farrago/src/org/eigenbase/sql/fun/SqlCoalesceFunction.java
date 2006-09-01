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
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;


/**
 * The <code>COALESCE</code> function.
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class SqlCoalesceFunction
    extends SqlFunction
{

    //~ Constructors -----------------------------------------------------------

    public SqlCoalesceFunction()
    {
        // NOTE jvs 26-July-2006:  We fill in the type strategies here,
        // but normally they are not used because the validator invokes
        // rewriteCall to convert COALESCE into CASE early.  However,
        // validator rewrite can optionally be disabled, in which case these
        // strategies are used.
        super("COALESCE",
            SqlKind.Function,
            SqlTypeStrategies.rtiLeastRestrictive,
            null,
            SqlTypeStrategies.otcSameVariadic,
            SqlFunctionCategory.System);
    }

    //~ Methods ----------------------------------------------------------------

    // override SqlOperator
    public SqlNode rewriteCall(SqlValidator validator, SqlCall call)
    {
        validateQuantifier(validator, call); // check DISTINCT/ALL

        SqlNode [] operands = call.getOperands();

        if (operands.length == 1) {
            // No CASE needed
            return operands[0];
        }
        
        SqlParserPos pos = call.getParserPosition();

        SqlNodeList whenList = new SqlNodeList(pos);
        SqlNodeList thenList = new SqlNodeList(pos);

        //todo optimize when know operand is not null.

        for (int i = 0; (i + 1) < operands.length; ++i) {
            whenList.add(
                SqlStdOperatorTable.isNotNullOperator.createCall(
                    operands[i],
                    pos));
            thenList.add(operands[i].clone(operands[i].getParserPosition()));
        }
        SqlNode elseExpr = operands[operands.length - 1];
        assert call.getFunctionQuantifier() == null;
        final SqlCall newCall =
            SqlStdOperatorTable.caseOperator.createCall(
                null,
                whenList,
                thenList,
                elseExpr,
                pos);
        return newCall;
    }
}

// End SqlCoalesceFunction.java
