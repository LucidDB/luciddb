/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2004-2004 Disruptive Tech
// Copyright (C) 2004-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.*;

import java.util.*;

/**
 * SqlProcedureCallOperator represents the CALL statement.  It
 * takes a single operand which is the real SqlCall.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SqlProcedureCallOperator extends SqlPrefixOperator
{
    public SqlProcedureCallOperator()
    {
        super("CALL", SqlKind.ProcedureCall, 0, null, null, null);
    }

    // override Sqloperator
    public SqlCall rewriteCall(
        SqlCall call)
    {
        // for now, rewrite "CALL f(x)" to "SELECT f(x) FROM VALUES(0)"
        // TODO jvs 18-Jan-2005:  rewrite to SELECT * FROM TABLE f(x)
        // once we support function calls as tables
        SqlStdOperatorTable opTab = SqlStdOperatorTable.instance();
        return opTab.selectOperator.createCall(
            null,
            new SqlNodeList(
                Collections.singletonList(
                    call.getOperands()[0]),
                ParserPosition.ZERO),
            opTab.valuesOperator.createCall(
                opTab.rowConstructor.createCall(
                    SqlLiteral.createExactNumeric("0", ParserPosition.ZERO),
                    ParserPosition.ZERO),
                ParserPosition.ZERO),
            null,
            null,
            null,
            null,
            null,
            ParserPosition.ZERO);
    }
}

// End SqlProcedureCallOperator.java
