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

import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.SqlParserUtil;
import org.eigenbase.sql.fun.SqlAvgAggFunction;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.type.IntervalSqlType;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.util.*;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataType;

import java.math.BigDecimal;
import java.util.Calendar;

/**
 * Standard implementation of {@link SqlNodeToRexConverter}.
 *
 * @author jhyde
 * @since 2005/8/4
 * @version $Id$
 */
public class SqlNodeToRexConverterImpl
    implements SqlNodeToRexConverter
{
    private final SqlRexConvertletTable convertletTable;

    SqlNodeToRexConverterImpl(SqlRexConvertletTable convertletTable)
    {
        this.convertletTable = convertletTable;
    }

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
            if (literal.getTypeName() == SqlTypeName.Boolean) {
                type = typeFactory.createSqlType(SqlTypeName.Boolean);
                type = typeFactory.createTypeWithNullability(type, true);
            } else {
                type = validator.getValidatedNodeType(literal);
            }
            return rexBuilder.makeCast(
                type,
                rexBuilder.constantNull());
        }

        BitString bitString;
        switch (literal.getTypeName().getOrdinal()) {
        case SqlTypeName.Decimal_ordinal:

            // exact number
            BigDecimal bd = (BigDecimal) value;
            return rexBuilder.makeExactLiteral(bd);
        case SqlTypeName.Double_ordinal:

            // approximate type
            // TODO:  preserve fixed-point precision and large integers
            return rexBuilder.makeApproxLiteral((BigDecimal) value);
        case SqlTypeName.Char_ordinal:
            return rexBuilder.makeCharLiteral((NlsString) value);
        case SqlTypeName.Boolean_ordinal:
            return rexBuilder.makeLiteral(((Boolean) value).booleanValue());
        case SqlTypeName.Binary_ordinal:
            bitString = (BitString) value;
            Util.permAssert((bitString.getBitCount() % 8) == 0,
                "incomplete octet");
            // An even number of hexits (e.g. X'ABCD') makes whole number
            // of bytes.
            byte [] bytes = bitString.getAsByteArray();
            return rexBuilder.makeBinaryLiteral(bytes);
        case SqlTypeName.Symbol_ordinal:
            return rexBuilder.makeFlag((EnumeratedValues.Value) value);
        case SqlTypeName.Timestamp_ordinal:
            return rexBuilder.makeTimestampLiteral((Calendar) value,
                ((SqlTimestampLiteral) literal).getPrec());
        case SqlTypeName.Time_ordinal:
            return rexBuilder.makeTimeLiteral((Calendar) value,
                ((SqlTimeLiteral) literal).getPrec());
        case SqlTypeName.Date_ordinal:
            return rexBuilder.makeDateLiteral((Calendar) value);
        // TODO: support IntervalYearMonth type of interval.
        case SqlTypeName.IntervalDayTime_ordinal:
            long l = SqlParserUtil.intervalToMillis((SqlIntervalLiteral.IntervalValue) value);
            return rexBuilder.makeIntervalLiteral(l);
        default:
            throw literal.getTypeName().unexpected();
        }
    }

}

// End SqlNodeToRexConverterImpl.java
