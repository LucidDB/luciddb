/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
package net.sf.farrago.rng;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;

/**
 * FarragoRngNextRandomIntOperator defines the SqlOperator for the
 * NEXT_RANDOM_INT pseudo-function.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoRngNextRandomIntOperator extends SqlFunction
{
    public FarragoRngNextRandomIntOperator()
    {
        super(
            "NEXT_RANDOM_INT",
            SqlKind.Other,
            ReturnTypeInferenceImpl.useInteger, 
            null,
            new OperandsTypeChecking.SimpleOperandsTypeChecking(
                new SqlTypeName [][] {
                    SqlTypeName.intTypes, 
                    SqlTypeName.charTypes,
                    SqlTypeName.charTypes
                }), 
            SqlFunctionCategory.System);
    }

    // override SqlOperator
    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        writer.print(name);
        writer.print("(");
        SqlLiteral ceiling = (SqlLiteral) operands[0];
        if (ceiling.intValue() == -1) {
            writer.print("UNBOUNDED ");
        } else {
            writer.print("CEILING ");
            ceiling.unparse(writer, leftPrec, rightPrec);
        }
        writer.print(" FROM ");
        SqlLiteral id = (SqlLiteral) operands[1];
        writer.print(id.getStringValue());
        writer.print(")");
    }
}

// End FarragoRngNextRandomIntOperator.java
