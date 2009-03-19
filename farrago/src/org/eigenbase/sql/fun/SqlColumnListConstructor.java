/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2009 The Eigenbase Project
// Copyright (C) 2006-2009 SQLstream, Inc.
// Copyright (C) 2006-2009 LucidEra, Inc.
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


/**
 * SqlColumnListConstructor defines the non-standard constructor used to pass a
 * COLUMN_LIST parameter to a UDX.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class SqlColumnListConstructor
    extends SqlSpecialOperator
{
    //~ Constructors -----------------------------------------------------------

    public SqlColumnListConstructor()
    {
        super(
            "COLUMN_LIST",
            SqlKind.ColumnListConstructor,
            MaxPrec,
            false,
            SqlTypeStrategies.rtiColumnList,
            null,
            SqlTypeStrategies.otcAny);
    }

    //~ Methods ----------------------------------------------------------------

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        writer.keyword("ROW");
        final SqlWriter.Frame frame = writer.startList("(", ")");
        for (int i = 0; i < operands.length; i++) {
            writer.sep(",");
            operands[0].unparse(writer, leftPrec, rightPrec);
        }
        writer.endList(frame);
    }
}

// End SqlColumnListConstructor.java
