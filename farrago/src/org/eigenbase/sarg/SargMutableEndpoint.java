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
     * @see SargFactory.newEndpoint
     */
    SargMutableEndpoint(SargFactory factory, RelDataType dataType)
    {
        super(factory, dataType);
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
    }

    /**
     * Sets a finite value for this endpoint.
     *
     * @param boundType boundary type
     *
     * @param coordinate endpoint position
     *
     * @param strict true for strict; false for exact
     */
    public void setFinite(
        SargBoundType boundType,
        RexNode coordinate,
        boolean strict)
    {
        super.setFinite(boundType, coordinate, strict);
    }
}

// End SargMutableEndpoint.java
