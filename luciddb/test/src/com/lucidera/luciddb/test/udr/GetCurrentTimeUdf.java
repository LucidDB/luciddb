/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 LucidEra, Inc.
// Copyright (C) 2006-2007 The Eigenbase Project
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
package com.lucidera.luciddb.test.udr;

import java.sql.*;
import java.util.*;

/**
 * Returns the current local date/time as a timestamp
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public abstract class GetCurrentTimeUdf
{
    public static Timestamp execute()
    {
        Calendar cal = new GregorianCalendar();
        // TODO: We're ahead by 7 hours always regardless of what time zone is 
        // being set   
        cal.roll(Calendar.HOUR, -7);
        return new Timestamp(cal.getTimeInMillis());
    }
}

// End GetCurrentTimeUdf.java
