/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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
package org.eigenbase.util14;

import java.util.*;


/**
 * BasicDatetime is an interface for dates, times, or timestamps that can be
 * assigned from a long value. The value to be assigned may either be a zoneless
 * time, or it may be a zoned time.
 *
 * <p>A zoneless time is based on milliseconds. It may contain date and/or time
 * components as follows:
 *
 * <pre>
 * The time component = value % milliseconds in a day
 * The date component = value / milliseconds in a day
 * </pre>
 *
 * If a date component is specified, it is relative to the epoch (1970-01-01).
 *
 * <p>A zoned time represents a time that was created in a particular time zone.
 * It may contain date and/or time components that are valid when interpreted
 * relative to a specified time zone, according to a {@link java.util.Calendar
 * Calendar}. Jdbc types, such as {@link java.sql.Date} typically contain zoned
 * times.
 *
 * @author John Pham
 * @version $Id$
 */
public interface BasicDatetime
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Gets the internal value of this datetime
     */
    public long getTime();

    /**
     * Sets this datetime via a zoneless time value. See class comments for more
     * information.
     */
    public void setZonelessTime(long value);

    /**
     * Sets this datetime via a zoned time value. See class comments for more
     * information.
     */
    public void setZonedTime(long value, TimeZone zone);
}

// End BasicDatetime.java
