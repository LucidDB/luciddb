/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;


/**
 * SqlRowOperator represents the special ROW constructor.
 *
 * <p>TODO: describe usage for row-value construction and row-type construction
 * (SQL supports both).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SqlRowOperator
    extends SqlSpecialOperator
{
    //~ Constructors -----------------------------------------------------------

    public SqlRowOperator()
    {
        super(
            "ROW",
            SqlKind.Row,
            MaxPrec,
            false,
            null,
            SqlTypeStrategies.otiReturnType,
            SqlTypeStrategies.otcVariadic);
    }

    //~ Methods ----------------------------------------------------------------

    // implement SqlOperator
    public SqlSyntax getSyntax()
    {
        // Function syntax would work too.
        return SqlSyntax.Special;
    }

    public RelDataType inferReturnType(
        SqlOperatorBinding opBinding)
    {
        // The type of a ROW(e1,e2) expression is a record with the types
        // {e1type,e2type}.  According to the standard, field names are
        // implementation-defined.
        RelDataType [] argTypes = opBinding.collectOperandTypes();
        final String [] fieldNames = new String[argTypes.length];
        for (int i = 0; i < fieldNames.length; i++) {
            fieldNames[i] = SqlUtil.deriveAliasFromOrdinal(i);
        }
        return opBinding.getTypeFactory().createStructType(
            argTypes,
            fieldNames);
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        SqlUtil.unparseFunctionSyntax(this, writer, operands, true, null);
    }

    // override SqlOperator
    public boolean requiresDecimalExpansion()
    {
        return false;
    }
}

// End SqlRowOperator.java
