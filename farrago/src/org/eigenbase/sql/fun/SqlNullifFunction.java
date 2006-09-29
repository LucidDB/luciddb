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
 * The <code>NULLIF</code> function.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SqlNullifFunction
    extends SqlFunction
{

    //~ Constructors -----------------------------------------------------------

    public SqlNullifFunction()
    {
        // NOTE jvs 26-July-2006:  We fill in the type strategies here,
        // but normally they are not used because the validator invokes
        // rewriteCall to convert NULLIF into CASE early.  However,
        // validator rewrite can optionally be disabled, in which case these
        // strategies are used.
        super("NULLIF",
            SqlKind.Function,
            SqlTypeStrategies.rtiFirstArgTypeForceNullable,
            null,
            SqlTypeStrategies.otcComparableUnorderedX2,
            SqlFunctionCategory.System);
    }

    //~ Methods ----------------------------------------------------------------

    // override SqlOperator
    public SqlNode rewriteCall(SqlValidator validator, SqlCall call)
    {
        SqlNode [] operands = call.getOperands();
        SqlParserPos pos = call.getParserPosition();

        checkOperandCount(
            validator,
            getOperandTypeChecker(),
            call);
        assert(operands.length == 2);

        SqlNodeList whenList = new SqlNodeList(pos);
        SqlNodeList thenList = new SqlNodeList(pos);
        whenList.add(operands[1]);
        thenList.add(SqlLiteral.createNull(SqlParserPos.ZERO));
        return
            SqlStdOperatorTable.caseOperator.createSwitchedCall(
                pos, operands[0],
                whenList,
                thenList,
                operands[0].clone(operands[0].getParserPosition())
            );
    }
}

// End SqlNullifFunction.java
