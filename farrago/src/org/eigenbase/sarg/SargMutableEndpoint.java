/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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
import org.eigenbase.sql.type.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.*;

import java.util.*;

/**
 * SargMutableEndpoint exposes methods for modifying a
 * {@link SargEndpoint}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SargMutableEndpoint extends SargEndpoint
{
    /**
     * If rounding was used in converting coordinate to the target type, the
     * sign of roundingCompensation represents the opposite of the rounding
     * direction, otherwise it is 0.
     */
    private int roundingCompensation;

    /**
     * Strictness after adjustment for rounding.
     *
     * @see getStrictness
     */
    private int effectiveStrictness;

    /**
     * @see SargFactory.newEndpoint
     */
    SargMutableEndpoint(SargFactory factory, RelDataType dataType)
    {
        super(factory, dataType);
        effectiveStrictness = 0;
        roundingCompensation = 0;
    }
    
    /**
     * Sets this endpoint to either negative or positive infinity.  An infinite
     * endpoint implies an open bound (negative infinity implies a lower bound,
     * while positive infinity implies an upper bound).
     *
     * @param infinitude either -1 or +1
     */
    public void setInfinity(int infinitude)
    {
        super.setInfinity(infinitude);
        roundingCompensation = 0;
        setEffectiveStrictness();
    }
    
    /**
     * Sets this endpoint to a finite bound.
     *
     * @param boundType upper or lower bound
     *
     * @param coordinate value to set; must match getDataType()
     *
     * @param strict true for an open bound (strictly greater or lower); false
     * for a closed bound
     */
    public void setFinite(
        SargBoundType boundType,
        RexNode coordinate,
        boolean strict)
    {
        super.setFinite(boundType, coordinate, strict);
        setEffectiveStrictness();
    }

    private void setEffectiveStrictness()
    {
        // rounding takes precedence over the strictness flag.
        //  Input        round    strictness    output    effective strictness
        //    >5.9        down            -1       >=6        0
        //    >=5.9       down             0       >=6        0
        //    >6.1          up            -1        >6        1
        //    >=6.1         up             0        >6        1
        //    <6.1          up             1       <=6        0
        //    <=6.1         up             0       <=6        0
        //    <5.9        down             1        <6       -1
        //    <=5.9       down             0        <6       -1
        if (roundingCompensation == 0) {
            effectiveStrictness = strictness;
        } else if (boundType == SargBoundType.LOWER) {
            if (roundingCompensation < 0) {
                effectiveStrictness = 0;
            } else {
                effectiveStrictness = 1;
            }
        } else if (boundType == SargBoundType.UPPER) {
            if (roundingCompensation > 0) {
                effectiveStrictness = 0;
            } else {
                effectiveStrictness = -1;
            }
        }
    }
}

// End SargMutableEndpoint.java
