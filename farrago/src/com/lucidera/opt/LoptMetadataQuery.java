/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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
package com.lucidera.opt;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.rex.*;

/**
 * LoptMetadataQuery defines the relational expression metadata queries
 * which are custom to LucidDB's optimizer.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class LoptMetadataQuery extends RelMetadataQuery
{
    /**
     * Estimates the cost of executing a relational expression, including the
     * cost of its inputs, given a set of filters which will be applied to its
     * output.  The default implementation assumes that the filters cannot be
     * used to reduce the processing cost, and will be evaluated above by a
     * calculator; so the result is the number of rows produced by rel plus the
     * cumulative cost of its inputs.  For expressions such as row scans,
     * more efficient filter processing may be possible.
     *
     * @param rel the relational expression
     *
     * @param filters filters to be applied
     *
     * @return estimated cost, or null if no reliable estimate can
     * be determined
     */
    public static Double getCostWithFilters(RelNode rel, RexNode filters)
    {
        return
            (Double) rel.getCluster().getMetadataProvider().getRelMetadata(
                rel, "getCostWithFilters", new Object[] { filters });
    }
}

// End LoptMetadataQuery.java
