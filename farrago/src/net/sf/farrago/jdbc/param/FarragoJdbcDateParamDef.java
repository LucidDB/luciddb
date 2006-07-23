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

import java.sql.Timestamp;

import java.util.Calendar;
import java.util.TimeZone;


/**
 * FarragoJdbcEngineDateParamDef defines a date parameter.
 *
 * @author Julian Hyde
 * @version $Id$
 */
class FarragoJdbcDateParamDef
    extends FarragoJdbcParamDef
{

    //~ Static fields/initializers ---------------------------------------------

    static final TimeZone gmtZone = TimeZone.getTimeZone("GMT");

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
                Calendar.getInstance(gmtZone));
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
            try {
                // TODO: Does this need to take cal into account?
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
        cal = (Calendar) cal.clone();
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

// End FarragoJdbcDateParamDef.java
