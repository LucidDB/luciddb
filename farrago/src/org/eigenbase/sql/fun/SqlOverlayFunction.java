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


/**
 * The <code>OVERLAY</code> function.
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class SqlOverlayFunction
    extends SqlFunction
{

    //~ Static fields/initializers ---------------------------------------------

    private static final SqlOperandTypeChecker otcCustom =
        new CompositeOperandTypeChecker(
            CompositeOperandTypeChecker.Composition.OR,
            SqlTypeStrategies.otcStringX2Int,
            SqlTypeStrategies.otcStringX2IntX2);

    //~ Constructors -----------------------------------------------------------

    public SqlOverlayFunction()
    {
        super("OVERLAY",
            SqlKind.Function,
            SqlTypeStrategies.rtiNullableVaryingDyadicStringSumPrecision,
            null,
            otcCustom,
            SqlFunctionCategory.String);
    }

    //~ Methods ----------------------------------------------------------------

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        final SqlWriter.Frame frame = writer.startFunCall(getName());
        operands[0].unparse(writer, leftPrec, rightPrec);
        writer.sep("PLACING");
        operands[1].unparse(writer, leftPrec, rightPrec);
        writer.sep("FROM");
        operands[2].unparse(writer, leftPrec, rightPrec);
        if (4 == operands.length) {
            writer.sep("FOR");
            operands[3].unparse(writer, leftPrec, rightPrec);
        }
        writer.endFunCall(frame);
    }

    public String getSignatureTemplate(final int operandsCount)
    {
        switch (operandsCount) {
        case 3:
            return "{0}({1} PLACING {2} FROM {3})";
        case 4:
            return "{0}({1} PLACING {2} FROM {3} FOR {4})";
        }
        assert (false);
        return null;
    }
}

// End SqlOverlayFunction.java
