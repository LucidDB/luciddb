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
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * The SQL <code>EXTRACT</code> operator. Extracts a specified field value from
 * a DATETIME or an INTERVAL. E.g.<br>
 * <code>EXTRACT(HOUR FROM INTERVAL '364 23:59:59')</code> returns <code>
 * 23</code>
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class SqlExtractFunction
    extends SqlFunction
{
    //~ Constructors -----------------------------------------------------------

    // SQL2003, Part 2, Section 4.4.3 - extract returns a exact numeric
    // TODO: Return type should be decimal for seconds
    public SqlExtractFunction()
    {
        super(
            "EXTRACT",
            SqlKind.Function,
            SqlTypeStrategies.rtiNullableBigint,
            null,
            SqlTypeStrategies.otcIntervalSameX2,
            SqlFunctionCategory.System);
    }

    //~ Methods ----------------------------------------------------------------

    public String getSignatureTemplate(int operandsCount)
    {
        Util.discard(operandsCount);
        return "{0}({1} FROM {2})";
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        final SqlWriter.Frame frame = writer.startFunCall(getName());
        operands[0].unparse(writer, leftPrec, rightPrec);
        writer.sep("FROM");
        operands[1].unparse(writer, leftPrec, rightPrec);
        writer.endFunCall(frame);
    }
}

// End SqlExtractFunction.java
