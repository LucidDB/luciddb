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

import java.util.Calendar;
import java.util.TimeZone;
import java.sql.Timestamp;

import org.eigenbase.reltype.RelDataType;

/**
 * FarragoJdbcEngineDateParamDef defines a date parameter. Converts parameters 
 * from local time (the JVM's timezone) into system time.
 * 
 * @author Julian Hyde
 * @version $Id$
 */
class FarragoJdbcEngineDateParamDef extends FarragoJdbcEngineParamDef
{
    static final TimeZone gmtZone = TimeZone.getTimeZone("GMT");
    
    public FarragoJdbcEngineDateParamDef(
        String paramName,
        RelDataType type)
    {
        super(paramName, type);
    }

    // implement FarragoSessionStmtParamDef
    public Object scrubValue(Object x)
    {
        if (x == null) {
            return null;
        }

        if (x instanceof String) {
            try {
                return java.sql.Date.valueOf((String) x);
            } catch (IllegalArgumentException e) {
                throw newInvalidFormat(x);
            }            
        }

        // Only java.sql.Date, java.sql.Timestamp are all OK.
        // java.sql.Time is not okay (no date information)
        if (!(x instanceof Timestamp) && !(x instanceof java.sql.Date)) {
            throw newInvalidType(x);
        }

        java.util.Date date = (java.util.Date) x;
        final long millis = date.getTime();
        final long shiftedMillis;

        // Shift time into gmt and truncate to previous midnight.
        // (There's probably a more efficient way of doing this.)
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(millis);

        // Truncate to midnight before we shift into GMT, just in case
        // the untruncated date falls in a different day in GMT.
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        // Shift into gmt and truncate again.
        cal.setTimeZone(gmtZone);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        shiftedMillis = cal.getTimeInMillis();

        return new java.sql.Date(shiftedMillis);
    }
}