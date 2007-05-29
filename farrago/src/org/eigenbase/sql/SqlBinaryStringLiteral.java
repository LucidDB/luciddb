/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
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
package org.eigenbase.sql;

import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * A binary (or hexadecimal) string literal.
 *
 * <p>The {@link #value} field is a {@link BitString} and {@link #typeName} is
 * {@link SqlTypeName#BINARY}.
 *
 * @author wael
 * @version $Id$
 */
public class SqlBinaryStringLiteral
    extends SqlAbstractStringLiteral
{

    //~ Constructors -----------------------------------------------------------

    protected SqlBinaryStringLiteral(
        BitString val,
        SqlParserPos pos)
    {
        super(val, SqlTypeName.BINARY, pos);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return the underlying BitString
     */
    public BitString getBitString()
    {
        return (BitString) value;
    }

    public SqlNode clone(SqlParserPos pos)
    {
        return new SqlBinaryStringLiteral((BitString) value, pos);
    }

    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        assert value instanceof BitString;
        writer.literal("X'" + ((BitString) value).toHexString() + "'");
    }

    protected SqlAbstractStringLiteral concat1(SqlLiteral [] lits)
    {
        BitString [] args = new BitString[lits.length];
        for (int i = 0; i < lits.length; i++) {
            args[i] = ((SqlBinaryStringLiteral) lits[i]).getBitString();
        }
        return
            new SqlBinaryStringLiteral(
                BitString.concat(args),
                lits[0].getParserPosition());
    }
}

// End SqlBinaryStringLiteral.java
