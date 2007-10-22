/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2007-2007 The Eigenbase Project
// Copyright (C) 2007-2007 Disruptive Tech
// Copyright (C) 2007-2007 LucidEra, Inc.
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
package org.eigenbase.sql.validate;

/**
 * Enumeration of types of monotonicity.
 *
 * @author jhyde
 * @version $Id$
 * @since 2007/9/4
 */
public enum SqlMonotonicity
{
    StrictlyIncreasing,
    Increasing,
    StrictlyDecreasing,
    Decreasing,
    Constant,
    NotMonotonic;

    /**
     * If this is a strict monotonicity
     * (StrictlyIncreasing, StrictlyDecreasing)
     * returns the non-strict equivalent
     * (Increasing, Decreasing).
     *
     * @return non-strict equivalent monotonicity
     */
    public SqlMonotonicity unstrict()
    {
        switch (this) {
        case StrictlyIncreasing:
            return Increasing;
        case StrictlyDecreasing:
            return Decreasing;
        default:
            return this;
        }
    }

    /**
     * Returns the reverse monotonicity.
     *
     * @return reverse monotonicity
     */
    public SqlMonotonicity reverse()
    {
        switch (this) {
        case StrictlyIncreasing:
            return StrictlyDecreasing;
        case Increasing:
            return Decreasing;
        case StrictlyDecreasing:
            return StrictlyIncreasing;
        case Decreasing:
            return Increasing;
        default:
            return this;
        }
    }

    /**
     * Whether values of this monotonicity are decreasing. That is, if a value
     * at a given point in a sequence is X, no point later in the sequence
     * will have a value greater than X.
     *
     * @return whether values are decreasing
     */
    public boolean isDecreasing()
    {
        switch (this) {
        case StrictlyDecreasing:
        case Decreasing:
            return true;
        default:
            return false;
        }
    }

    /**
     * Returns whether values of this monotonicity may ever repeat:
     * true for {@link #NotMonotonic} and {@link #Constant}, false otherwise.
     *
     * <p>If a column is known not to repeat, a sort on that column can make
     * progress before all of the input has been seen.
     *
     * @return whether values repeat
     */
    public boolean mayRepeat()
    {
        switch (this) {
        case NotMonotonic:
        case Constant:
            return true;
        default:
            return false;
        }
    }
}

// End SqlMonotonicity.java
