/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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
package org.eigenbase.sql.type;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;

/**
 * IntervalSqlType represents a standard SQL datetime interval type.
 *
 * @author wael
 * @version $Id$
 */
public class IntervalSqlType extends AbstractSqlType
{
    private SqlIntervalQualifier intervalQualifier;

    /**
     * Constructs an IntervalSqlType.  This should only be called from a
     * factory method.
     */
    public IntervalSqlType(
        SqlIntervalQualifier intervalQualifier,
        boolean isNullable)
    {
        super(
            intervalQualifier.isYearMonth()
            ? SqlTypeName.IntervalYearMonth
            : SqlTypeName.IntervalDayTime,
            isNullable,
            null);
        this.intervalQualifier = intervalQualifier;
        computeDigest();
    }

    // implement RelDataTypeImpl
    protected void generateTypeString(StringBuffer sb, boolean withDetail)
    {
        sb.append("INTERVAL ");
        sb.append(intervalQualifier.toString());
    }
        
    // implement RelDataType
    public SqlIntervalQualifier getIntervalQualifier()
    {
        return intervalQualifier;
    }

    /**
     * Combines two IntervalTypes and returns the result.
     * E.g. the result of combining <br>
     * <code>INTERVAL DAY TO HOUR</code> <br>
     * with <br>
     * <code>INTERVAL SECOND</code> is <br>
     * <code>INTERVAL DAY TO SECOND</code>
     */
    public IntervalSqlType combine(IntervalSqlType that) {
        assert(this.intervalQualifier.isYearMonth()==
            that.intervalQualifier.isYearMonth());
        boolean  nullable = isNullable || that.isNullable;
        SqlIntervalQualifier.TimeUnit thisStart =
            this.intervalQualifier.getStartUnit();
        SqlIntervalQualifier.TimeUnit thisEnd =
            this.intervalQualifier.getEndUnit();
        SqlIntervalQualifier.TimeUnit thatStart =
            that.intervalQualifier.getStartUnit();
        SqlIntervalQualifier.TimeUnit thatEnd =
            that.intervalQualifier.getEndUnit();

        assert(null!=thisStart);
        assert(null!=thatStart);

        int secondPrec = intervalQualifier.getStartPrecision();
        int fracPrec = Math.max(
            this.intervalQualifier.getFractionalSecondPrecision(),
            that.intervalQualifier.getFractionalSecondPrecision());

        if (thisStart.getOrdinal() > thatStart.
            getOrdinal()) {
            thisEnd = thisStart;
            thisStart = thatStart;
            secondPrec = that.intervalQualifier.getStartPrecision();
        } else if (thisStart.getOrdinal() == thatStart.getOrdinal()) {
            secondPrec = Math.max(secondPrec,
                that.intervalQualifier.getStartPrecision());
        } else  if ((null == thisEnd) || (thisEnd.getOrdinal() < thatStart.
                        getOrdinal())) {
            thisEnd = thatStart;
        }

        if (null!=thatEnd) {
            if ((null==thisEnd) ||
                (thisEnd.getOrdinal() < thatEnd.getOrdinal())) {
                thisEnd = thatEnd;
            }
        }

        return new IntervalSqlType(
            new SqlIntervalQualifier(
                thisStart, secondPrec, thisEnd, fracPrec, null), nullable);
    }

    // implement RelDataType
    public int getPrecision()
    {
        return intervalQualifier.getStartPrecision();
    }
}

// End IntervalSqlType.java
