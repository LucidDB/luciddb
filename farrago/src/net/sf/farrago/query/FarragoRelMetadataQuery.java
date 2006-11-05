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
package net.sf.farrago.query;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;

/**
 * FarragoRelMetadataQuery defines the relational expression metadata
 * queries specific to Farrago.
 *
 * @author John Sichi
 * @version $Id$
 */
public abstract class FarragoRelMetadataQuery extends RelMetadataQuery
{
    /**
     * Determines whether a physical expression can be restarted.  For leaves,
     * default implementation is true; for non-leaves, default implementation
     * is conjunction of children.
     *
     * @param rel the relational expression
     *
     * @return true if restart is possible
     */
    public static boolean canRestart(RelNode rel)
    {
        return (Boolean) rel.getCluster().getMetadataProvider().getRelMetadata(
            rel,
            "canRestart",
            null);
    }
}

// End FarragoRelMetadataQuery.java
