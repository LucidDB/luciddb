/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
package org.eigenbase.sql2rel;

import java.math.*;

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;


/**
 * Standard implementation of {@link SqlNodeToRexConverter}.
 *
 * @author jhyde
 * @version $Id$
 * @since 2005/8/4
 */
public class SqlNodeToRexConverterImpl
    implements SqlNodeToRexConverter
{
    //~ Instance fields --------------------------------------------------------

    private final SqlRexConvertletTable convertletTable;

    //~ Constructors -----------------------------------------------------------

    SqlNodeToRexConverterImpl(SqlRexConvertletTable convertletTable)
    {
        this.convertletTable = convertletTable;
    }

    //~ Methods ----------------------------------------------------------------

    public RexNode convertCall(SqlRexContext cx, SqlCall call)
    {
        final SqlRexConvertlet convertlet = convertletTable.get(call);
        if (convertlet != null) {
            return convertlet.convertCall(cx, call);
        }

        // No convertlet was suitable. (Unlikely, because the standard
        // convertlet table has a fall-back for all possible calls.)
        throw Util.needToImplement(call);
    }

    public RexLiteral convertInterval(
        SqlRexContext cx,
        SqlIntervalQualifier intervalQualifier)
    {
        RexBuilder rexBuilder = cx.getRexBuilder();

        return rexBuilder.makeIntervalLiteral(intervalQualifier);
    }

    public RexNode convertLiteral(
        SqlRexContext cx,
        SqlLiteral literal)
    {
        RexBuilder rexBuilder = cx.getRexBuilder();
        RelDataTypeFactory typeFactory = cx.getTypeFactory();
        SqlValidator validator = cx.getValidator();
        final Object value = literal.getValue();
        if (value == null) {
            // Since there is no eq. RexLiteral of SqlLiteral.Unknown we
            // treat it as a cast(null as boolean)
            RelDataType type;
            if (literal.getTypeName() == SqlTypeName.BOOLEAN) {
                type = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
                type = typeFactory.createTypeWithNullability(type, true);
            } else {
                type = validator.getValidatedNodeType(literal);
            }
            return rexBuilder.makeCast(
                type,
                rexBuilder.constantNull());
        }

        BitString bitString;
        switch (literal.getTypeName()) {
        case DECIMAL:

            // exact number
            BigDecimal bd = (BigDecimal) value;
            return rexBuilder.makeExactLiteral(
                bd,
                literal.createSqlType(typeFactory));
        case DOUBLE:

            // approximate type
            // TODO:  preserve fixed-point precision and large integers
            return rexBuilder.makeApproxLiteral((BigDecimal) value);
        case CHAR:
            return rexBuilder.makeCharLiteral((NlsString) value);
        case BOOLEAN:
            return rexBuilder.makeLiteral(((Boolean) value).booleanValue());
        case BINARY:
            bitString = (BitString) value;
            Util.permAssert(
                (bitString.getBitCount() % 8) == 0,
                "incomplete octet");

            // An even number of hexits (e.g. X'ABCD') makes whole number
            // of bytes.
            byte [] bytes = bitString.getAsByteArray();
            return rexBuilder.makeBinaryLiteral(bytes);
        case SYMBOL:
            return rexBuilder.makeFlag(value);
        case TIMESTAMP:
            return rexBuilder.makeTimestampLiteral(
                (Calendar) value,
                ((SqlTimestampLiteral) literal).getPrec());
        case TIME:
            return rexBuilder.makeTimeLiteral(
                (Calendar) value,
                ((SqlTimeLiteral) literal).getPrec());
        case DATE:
            return rexBuilder.makeDateLiteral((Calendar) value);

        case INTERVAL_YEAR_MONTH: {
            SqlIntervalLiteral.IntervalValue intervalValue =
                (SqlIntervalLiteral.IntervalValue) value;
            long l = SqlParserUtil.intervalToMonths(intervalValue);
            return rexBuilder.makeIntervalLiteral(
                l,
                intervalValue.getIntervalQualifier());
        }
        case INTERVAL_DAY_TIME: {
            SqlIntervalLiteral.IntervalValue intervalValue =
                (SqlIntervalLiteral.IntervalValue) value;
            long l = SqlParserUtil.intervalToMillis(intervalValue);
            return rexBuilder.makeIntervalLiteral(
                l,
                intervalValue.getIntervalQualifier());
        }
        default:
            throw Util.unexpected(literal.getTypeName());
        }
    }
}

// End SqlNodeToRexConverterImpl.java
