/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
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

import java.sql.Time;
import java.sql.Timestamp;

import java.util.Calendar;

import org.eigenbase.util14.*;


/**
 * FarragoJdbcEngineTimeParamDef defines a time parameter.
 *
 * <p>This class is JDK 1.4 compatible.
 *
 * @author Julian Hyde
 * @version $Id$
 */
class FarragoJdbcTimeParamDef
    extends FarragoJdbcParamDef
{
    //~ Constructors -----------------------------------------------------------

    public FarragoJdbcTimeParamDef(
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
            return x;
        }

        if (x instanceof String) {
            String s = ((String) x).trim();
            ZonelessTime zt = ZonelessTime.parse(s);
            if (zt == null) {
                throw newInvalidFormat(x);
            }
            return zt;
        }

        // Of the subtypes of java.util.Date,
        // only java.sql.Timestamp and java.sql.Time are OK.
        // java.sql.Date is not okay (no time information).
        if ((x instanceof Timestamp) || (x instanceof Time)) {
            java.util.Date timestamp = (java.util.Date) x;
            ZonelessTimestamp zt = new ZonelessTimestamp();
            zt.setZonedTime(timestamp.getTime(), DateTimeUtil.getTimeZone(cal));
            return zt;
        }

        // ZonelessDatetime is not required by JDBC, but we allow it because
        // it is a convenient format to serialize values over RMI.
        // We disallow ZonelessTime for the same reasons we disallow
        // java.sql.Time above.
        if ((x instanceof ZonelessTimestamp) || (x instanceof ZonelessTime)) {
            long time = ((ZonelessDatetime) x).getTime();
            ZonelessTime zt = new ZonelessTime();
            zt.setZonedTime(time, DateTimeUtil.getTimeZone(cal));
            return zt;
        }

        throw newInvalidType(x);
    }
}

// End FarragoJdbcTimeParamDef.java
