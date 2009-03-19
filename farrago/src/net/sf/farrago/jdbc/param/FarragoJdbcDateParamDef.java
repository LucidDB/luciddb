/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
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
package net.sf.farrago.jdbc.param;

import java.sql.Timestamp;

import java.util.Calendar;

import org.eigenbase.util14.*;


/**
 * FarragoJdbcEngineDateParamDef defines a date parameter.
 *
 * <p>This class is JDK 1.4 compatible.
 *
 * @author Julian Hyde
 * @version $Id$
 */
class FarragoJdbcDateParamDef
    extends FarragoJdbcParamDef
{
    //~ Constructors -----------------------------------------------------------

    public FarragoJdbcDateParamDef(
        String paramName,
        FarragoParamFieldMetaData paramMetaData)
    {
        super(paramName, paramMetaData);
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionStmtParamDef
    public Object scrubValue(Object x)
    {
        return scrubValue(
            x,
            Calendar.getInstance());
    }

    // implement FarragoSessionStmtParamDef
    // Converts parameters from timezone in calendar into gmt time.
    public Object scrubValue(Object x, Calendar cal)
    {
        if (x == null) {
            checkNullable();
            return null;
        }

        if (x instanceof String) {
            String s = ((String) x).trim();
            ZonelessDate zd = ZonelessDate.parse(s);
            if (zd == null) {
                throw newInvalidFormat(x);
            }
            return zd;
        }

        // Of the subtypes of java.util.Date,
        // only java.sql.Date and java.sql.Timestamp are OK.
        // java.sql.Time is not okay (no date information).
        if ((x instanceof Timestamp) || (x instanceof java.sql.Date)) {
            java.util.Date d = (java.util.Date) x;
            ZonelessDate zd = new ZonelessDate();
            zd.setZonedTime(d.getTime(), DateTimeUtil.getTimeZone(cal));
            return zd;
        }

        // ZonelessDatetime is not required by JDBC, but we allow it because
        // it is a convenient format to serialize values over RMI.
        // We disallow ZonelessTime for the same reasons we disallow
        // java.sql.Time above.
        if ((x instanceof ZonelessTimestamp) || (x instanceof ZonelessDate)) {
            ZonelessDate zd = new ZonelessDate();
            long time = ((ZonelessDatetime) x).getTime();
            zd.setZonedTime(time, DateTimeUtil.getTimeZone(cal));
            return zd;
        }

        throw newInvalidType(x);
    }
}

// End FarragoJdbcDateParamDef.java
