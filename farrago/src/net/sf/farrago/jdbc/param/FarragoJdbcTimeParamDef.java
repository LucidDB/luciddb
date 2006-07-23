/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2005-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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
import java.util.TimeZone;


/**
 * FarragoJdbcEngineTimeParamDef defines a time parameter.
 *
 * @author Julian Hyde
 * @version $Id$
 */
class FarragoJdbcTimeParamDef
    extends FarragoJdbcParamDef
{

    //~ Static fields/initializers ---------------------------------------------

    static final TimeZone gmtZone = TimeZone.getTimeZone("GMT");

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
                Calendar.getInstance(gmtZone));
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
            try {
                // TODO: Does this need to take cal into account?
                return Time.valueOf((String) x);
            } catch (IllegalArgumentException e) {
                throw newInvalidFormat(x);
            }
        }

        // Only java.sql.Time, java.sql.Timestamp are all OK.
        // java.sql.Date is not okay (no time information)
        if (!(x instanceof Timestamp) && !(x instanceof Time)) {
            throw newInvalidType(x);
        }

        java.util.Date time = (java.util.Date) x;

        // Make a copy of the calendar
        cal = (Calendar) cal.clone();
        cal.setTime(time);
        final int hour = cal.get(Calendar.HOUR_OF_DAY);
        final int minute = cal.get(Calendar.MINUTE);
        final int second = cal.get(Calendar.SECOND);
        final int millisecond = cal.get(Calendar.MILLISECOND);

        // set date to epoch
        cal.clear();

        // shift to gmt
        cal.setTimeZone(gmtZone);

        // now restore the time part
        cal.set(Calendar.HOUR_OF_DAY, hour);
        cal.set(Calendar.MINUTE, minute);
        cal.set(Calendar.SECOND, second);
        cal.set(Calendar.MILLISECOND, millisecond);

        // convert to a time object
        return new Time(cal.getTimeInMillis());
    }
}

// End FarragoJdbcTimeParamDef.java
