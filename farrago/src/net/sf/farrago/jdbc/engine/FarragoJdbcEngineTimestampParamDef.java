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

import java.sql.Timestamp;
import java.util.TimeZone;

import org.eigenbase.reltype.RelDataType;

/**
 * FarragoJdbcEngineTimestampParamDef defines a Timestamp parameter. Converts 
 * parameters from local time (the JVM's timezone) into system time.
 * 
 * @author Julian Hyde
 * @version $Id$
 */
class FarragoJdbcEngineTimestampParamDef extends FarragoJdbcEngineParamDef
{
    static final TimeZone defaultZone = TimeZone.getDefault();

    FarragoJdbcEngineTimestampParamDef(
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
        java.util.Date timestamp = (java.util.Date) x;
        long millis = timestamp.getTime();
        int timeZoneOffset = defaultZone.getOffset(millis);

        // shift the time into gmt
        return new Timestamp(millis + timeZoneOffset);
    }
}