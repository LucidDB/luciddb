/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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
import org.eigenbase.sql.validate.SqlMonotonicity;
import org.eigenbase.sql.validate.SqlValidatorScope;


/**
 * A special operator for the subtraction of two DATETIMEs. The format of
 * DATETIME substraction is:<br>
 * <code>"(" &lt;datetime&gt; "-" &lt;datetime&gt; ")" <interval
 * qualifier></code>. This operator is special since it needs to hold the
 * additional interval qualifier specification.
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class SqlDatetimeSubtractionOperator
    extends SqlSpecialOperator
{
    //~ Constructors -----------------------------------------------------------

    public SqlDatetimeSubtractionOperator()
    {
        super(
            "-",
            SqlKind.Minus,
            40,
            true,
            SqlTypeStrategies.rtiNullableThirdArgType,
            SqlTypeStrategies.otiFirstKnown,
            SqlTypeStrategies.otcMinusDateOperator);
    }

    //~ Methods ----------------------------------------------------------------

    public SqlSyntax getSyntax()
    {
        return SqlSyntax.Special;
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        final SqlWriter.Frame frame = writer.startList("(", ")");
        operands[0].unparse(writer, leftPrec, rightPrec);
        writer.sep("-");
        operands[1].unparse(writer, leftPrec, rightPrec);
        writer.endList(frame);
        operands[2].unparse(writer, leftPrec, rightPrec);
    }

    public SqlMonotonicity getMonotonicity(
        SqlCall call,
        SqlValidatorScope scope)
    {
          return SqlStdOperatorTable.minusOperator.getMonotonicity(call, scope);
    }
}

// End SqlDatetimeSubtractionOperator.java
