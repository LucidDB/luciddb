/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2009 The Eigenbase Project
// Copyright (C) 2006-2009 SQLstream, Inc.
// Copyright (C) 2006-2009 LucidEra, Inc.
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
package org.eigenbase.sarg;

import org.eigenbase.reltype.*;


/**
 * SargInterval represents a single contiguous search interval over a scalar
 * domain of a given datatype (including null values). It consists of two
 * endpoints: a lower bound and an upper bound, which may be the same for the
 * case of a single point. The endpoints are represented via instances of {@link
 * SargEndpoint}. An empty interval is represented by setting both bounds to be
 * open with the same value (the null value, but it doesn't really matter).
 *
 * <p>Instances of SargInterval are immutable after construction.
 *
 * <p>For string representation, we use the standard mathematical bracketed
 * bounds pair notation, with round brackets for open bounds and square brackets
 * for closed bounds, e.g.
 *
 * <ul>
 * <li>[3,5] represents all values between 3 and 5 inclusive
 * <li>(3,5] represents all values greater than 3 and less than or equal to 5
 * <li>[3,5) represents all values greatern than or equal to 3 and less than 5
 * <li>(3,5) represents all values between 3 and 5 exclusive
 * <li>(3,+infinity) represents all values greater than 3
 * <li>(-infinity,5] represents all values less than or equal to 5
 * <li>[5,5] represents the single point with coordinate 5
 * </ul>
 *
 * <p>Null values are ordered lower than any non-null value but higher than
 * -infinity. So the interval [null,7) would include the null value and any
 * non-null value less than 7.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SargInterval
    extends SargIntervalBase
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a SargInterval.
     */
    SargInterval(
        SargFactory factory,
        RelDataType dataType)
    {
        super(factory, dataType);
    }

    //~ Methods ----------------------------------------------------------------

    void copyFrom(SargIntervalBase other)
    {
        assert (getDataType() == other.getDataType());
        lowerBound.copyFrom(other.getLowerBound());
        upperBound.copyFrom(other.getUpperBound());
    }

    boolean contains(SargInterval other)
    {
        assert (getDataType() == other.getDataType());
        if (getLowerBound().compareTo(other.getLowerBound()) > 0) {
            return false;
        }
        if (getUpperBound().compareTo(other.getUpperBound()) < 0) {
            return false;
        }
        return true;
    }
}

// End SargInterval.java
