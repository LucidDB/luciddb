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

import java.math.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * A numeric SQL literal.
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 18, 2004
 */
public class SqlNumericLiteral
    extends SqlLiteral
{

    //~ Instance fields --------------------------------------------------------

    private Integer prec;
    private Integer scale;
    private boolean isExact;

    //~ Constructors -----------------------------------------------------------

    protected SqlNumericLiteral(
        BigDecimal value,
        Integer prec,
        Integer scale,
        boolean isExact,
        SqlParserPos pos)
    {
        super(value, isExact ? SqlTypeName.Decimal : SqlTypeName.Double,
            pos);
        this.prec = prec;
        this.scale = scale;
        this.isExact = isExact;
    }

    //~ Methods ----------------------------------------------------------------

    public Integer getPrec()
    {
        return prec;
    }

    public Integer getScale()
    {
        return scale;
    }

    public boolean isExact()
    {
        return isExact;
    }

    public SqlNode clone(SqlParserPos pos)
    {
        return
            new SqlNumericLiteral(
                (BigDecimal) value,
                getPrec(),
                getScale(),
                isExact,
                pos);
    }

    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        writer.literal(toValue());
    }

    public String toValue()
    {
        BigDecimal bd = (BigDecimal) value;
        if (isExact) {
            return value.toString();
        }
        return Util.toScientificNotation(bd);
    }

    public RelDataType createSqlType(RelDataTypeFactory typeFactory)
    {
        if (isExact) {
            int scaleValue = scale.intValue();
            if (0 == scaleValue) {
                BigDecimal bd = (BigDecimal) value;
                SqlTypeName result;
                long l = bd.longValue();
                if ((l >= Integer.MIN_VALUE) && (l <= Integer.MAX_VALUE)) {
                    result = SqlTypeName.Integer;
                } else {
                    result = SqlTypeName.Bigint;
                }
                return typeFactory.createSqlType(result);
            }

            //else we have a decimal
            return
                typeFactory.createSqlType(
                    SqlTypeName.Decimal,
                    prec.intValue(),
                    scaleValue);
        }

        // else we have a a float, real or double.  make them all double for
        // now.
        return typeFactory.createSqlType(SqlTypeName.Double);
    }

    public boolean isInteger()
    {
        return (0 == scale.intValue());
    }
}

// End SqlNumericLiteral.java
