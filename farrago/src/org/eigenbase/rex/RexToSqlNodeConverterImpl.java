/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2007-2007 The Eigenbase Project
// Copyright (C) 2007-2007 Disruptive Tech
// Copyright (C) 2007-2007 LucidEra, Inc.
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
package org.eigenbase.rex;

import java.util.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * Standard implementation of {@link RexToSqlNodeConverter}.
 *
 * @author Sunny Choi
 * @version $Id$
 */
public class RexToSqlNodeConverterImpl
    implements RexToSqlNodeConverter
{
    //~ Instance fields --------------------------------------------------------

    private final RexSqlConvertletTable convertletTable;

    //~ Constructors -----------------------------------------------------------

    public RexToSqlNodeConverterImpl(RexSqlConvertletTable convertletTable)
    {
        this.convertletTable = convertletTable;
    }

    //~ Methods ----------------------------------------------------------------

    public SqlNode convertCall(RexCall call)
    {
        final RexSqlConvertlet convertlet = convertletTable.get(call);
        if (convertlet != null) {
            return convertlet.convertCall(this, call);
        }

        // No convertlet was suitable.
        throw Util.needToImplement(call);
    }

    public SqlNode convertLiteral(RexLiteral literal)
    {
        // Numeric
        if (SqlTypeFamily.EXACT_NUMERIC.getTypeNames().contains(
                literal.getTypeName()))
        {
            return SqlLiteral.createExactNumeric(
                literal.getValue().toString(),
                SqlParserPos.ZERO);
        }

        if (SqlTypeFamily.APPROXIMATE_NUMERIC.getTypeNames().contains(
                literal.getTypeName()))
        {
            return SqlLiteral.createApproxNumeric(
                literal.getValue().toString(),
                SqlParserPos.ZERO);
        }

        // Timestamp
        if (SqlTypeFamily.TIMESTAMP.getTypeNames().contains(
                literal.getTypeName()))
        {
            return SqlLiteral.createTimestamp(
                (Calendar) literal.getValue(),
                0,
                SqlParserPos.ZERO);
        }

        // Date
        if (SqlTypeFamily.DATE.getTypeNames().contains(
                literal.getTypeName()))
        {
            return SqlLiteral.createDate(
                (Calendar) literal.getValue(),
                SqlParserPos.ZERO);
        }

        // String
        if (SqlTypeFamily.CHARACTER.getTypeNames().contains(
                literal.getTypeName()))
        {
            return SqlLiteral.createCharString(
                ((NlsString) (literal.getValue())).getValue(),
                SqlParserPos.ZERO);
        }
        
        // Boolean
        if (SqlTypeFamily.BOOLEAN.getTypeNames().contains(
                literal.getTypeName()))
        {
            return SqlLiteral.createBoolean(
                (Boolean) literal.getValue(),
                SqlParserPos.ZERO);
        }
        throw Util.unexpected(literal.getTypeName());
    }

    public SqlNode convertInputRef(RexInputRef ref)
    {
        throw Util.needToImplement(ref);
    }
}

// End RexToSqlNodeConverterImpl.java
