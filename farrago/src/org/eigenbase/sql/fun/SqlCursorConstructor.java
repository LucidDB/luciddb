/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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

import org.eigenbase.reltype.*;

/**
 * SqlCursorConstructor defines the non-standard CURSOR(&lt;query&gt;)
 * constructor.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SqlCursorConstructor extends SqlSpecialOperator
{
    //~ Constructors ----------------------------------------------------------

    public SqlCursorConstructor()
    {
        // REVIEW jvs 7-May-2006: Precedence copied from
        // SqlMultisetQueryConstructor
        super("CURSOR", SqlKind.CursorConstructor, 100, false,
            SqlTypeStrategies.rtiFirstArgType,
            null,
            SqlTypeStrategies.otcAny);
    }

    //~ Methods ---------------------------------------------------------------

    public RelDataType deriveType(
        SqlValidator validator, SqlValidatorScope scope, SqlCall call)
    {
        SqlSelect subSelect = (SqlSelect) call.operands[0];
        subSelect.validateExpr(validator, scope);
        SqlValidatorNamespace ns = validator.getNamespace(subSelect);
        return ns.getRowType();
    }

    public void unparse(
        SqlWriter writer,
        SqlNode[] operands,
        int leftPrec,
        int rightPrec) {

        writer.keyword("CURSOR");
        final SqlWriter.Frame frame = writer.startList("(", ")");
        assert operands.length == 1;
        operands[0].unparse(writer, leftPrec, rightPrec);
        writer.endList(frame);
    }
}

// End SqlCursorConstructor.java
