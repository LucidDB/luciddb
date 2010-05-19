/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2002 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
package org.eigenbase.rel;

import org.eigenbase.reltype.*;


/**
 * An <code>Aggregation</code> aggregates a set of values into one value.
 *
 * <p>It is used, via a {@link AggregateCall}, in an {@link AggregateRel}
 * relational operator.</p>
 *
 * @author jhyde
 * @version $Id$
 * @since 26 January, 2001
 */
public interface Aggregation
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the parameter types accepted by this Aggregation.
     *
     * @param typeFactory Type factory to create the types
     *
     * @return Array of parameter types
     */
    RelDataType [] getParameterTypes(RelDataTypeFactory typeFactory);

    /**
     * Returns the type of the result yielded by this Aggregation.
     *
     * @param typeFactory Type factory to create the type
     *
     * @return Result type
     */
    RelDataType getReturnType(RelDataTypeFactory typeFactory);

    /**
     * Returns the name of this Aggregation
     *
     * @return name of this aggregation
     */
    String getName();
}

// End Aggregation.java
