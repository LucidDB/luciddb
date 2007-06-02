/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 Disruptive Tech
// Copyright (C) 2006-2007 LucidEra, Inc.
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
package net.sf.farrago.query;

// REVIEW jvs 22-Mar-2006:  use generics to write a Java version
// of Fennel's OpaqueInteger?  Would have to use the boxed type
// underneath.

/**
 * FennelRelParamId is an opaque type representing the reservation of a {@link
 * net.sf.farrago.fennel.FennelDynamicParamId} during query planning. See <a
 * href="http://wiki.eigenbase.org/InternalDynamicParamScoping">the design
 * docs</a> for why this logical ID is needed in addition to
 * FennelDynamicParamId, which is the physical ID. A 64-bit integer is used
 * since a large number of these may be generated and then discarded during
 * query planning. (I hate to think about the impliciations of a planner that
 * would actually exhaust 32 bits, but still, one just can't be too safe.)
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelRelParamId
{
    //~ Instance fields --------------------------------------------------------

    private final long id;

    //~ Constructors -----------------------------------------------------------

    public FennelRelParamId(long id)
    {
        this.id = id;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return the underlying long value
     */
    public long longValue()
    {
        return id;
    }

    // implement Object
    public boolean equals(Object other)
    {
        return ((other instanceof FennelRelParamId)
            && (((FennelRelParamId) other).longValue() == id));
    }

    // implement Object
    public int hashCode()
    {
        return (int) id;
    }

    // implement Object
    public String toString()
    {
        return Long.toString(id);
    }
}

// End FennelRelParamId.java
