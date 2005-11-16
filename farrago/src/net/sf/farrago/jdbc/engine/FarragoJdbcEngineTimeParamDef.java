/*
// $Id$
// Farrago is an extensible data management system.
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
package net.sf.farrago.jdbc.engine;

import java.sql.Time;
import java.util.Calendar;
import java.util.TimeZone;

import org.eigenbase.reltype.RelDataType;

/**
 * FarragoJdbcEngineTimeParamDef defines a time parameter. Converts parameters 
 * from local time (the JVM's timezone) into system time.
 * 
 * @author Julian Hyde
 * @version $Id$
 */
class FarragoJdbcEngineTimeParamDef extends FarragoJdbcEngineParamDef
{
    static final TimeZone gmtZone = TimeZone.getTimeZone("GMT");
    
    public FarragoJdbcEngineTimeParamDef(
        String paramName,
        RelDataType type)
    {
        super(paramName, type);
    }

    // implement FarragoSessionStmtParamDef
    public Object scrubValue(Object x)
    {
        // java.sql.Date, java.sql.Time, java.sql.Timestamp are all OK.
        if (!(x instanceof java.util.Date)) {
            throw newInvalidType(x);
        }
        java.util.Date time = (java.util.Date) x;

        // create a calendar containing time in locale timezone
        Calendar cal = Calendar.getInstance();
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