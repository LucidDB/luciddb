/*
// $Id$
// Farrago is an extensible data management system.
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
package net.sf.farrago.fennel;

/**
 * FennelDynamicParamId is an opaque type for the 32-bit integers used to
 * uniquely identify dynamic parameters within a {@link FennelStreamGraph}.
 * Fennel dynamic parameters may be used to implement user-level dynamic
 * parameters (represented as question marks in SQL text); they may also be
 * generated internally by the optimizer as part of physical implementation.
 * In the latter case, they are used for out-of-band communication between
 * ExecStreams which cannot be expressed via the usual producer/consumer
 * dataflow mechanisms (e.g. when the streams are not adjacent, or when
 * communication is required from consumer back to producer).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelDynamicParamId
{
    private final int id;
    
    public FennelDynamicParamId(int id)
    {
        this.id = id;
    }

    /**
     * @return the underlying int value
     */
    public int intValue()
    {
        return id;
    }

    // implement Object
    public boolean equals(Object other)
    {
        return ((other instanceof FennelDynamicParamId)
            && (((FennelDynamicParamId) other).intValue() == id));
    }

    // implement Object
    public int hashCode()
    {
        return id;
    }
    
    // implement Object
    public String toString()
    {
        return Integer.toString(id);
    }
}

// End FennelDynamicParamId.java
