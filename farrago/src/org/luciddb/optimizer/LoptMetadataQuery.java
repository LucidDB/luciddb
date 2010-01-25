/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2009 The Eigenbase Project
// Copyright (C) 2010-2010 SQLstream, Inc.
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
package org.luciddb.optimizer;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.rex.*;


/**
 * LoptMetadataQuery defines the relational expression metadata queries which
 * are custom to LucidDB's optimizer.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class LoptMetadataQuery
    extends RelMetadataQuery
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Estimates the cost of executing a relational expression, including the
     * cost of its inputs, given a set of filters which will be applied to its
     * output. The default implementation assumes that the filters cannot be
     * used to reduce the processing cost, and will be evaluated above by a
     * calculator; so the result is the number of rows produced by rel plus the
     * cumulative cost of its inputs. For expressions such as row scans, more
     * efficient filter processing may be possible.
     *
     * @param rel the relational expression
     * @param filters filters to be applied
     *
     * @return estimated cost, or null if no reliable estimate can be determined
     */
    public static Double getCostWithFilters(RelNode rel, RexNode filters)
    {
        return (Double) rel.getCluster().getMetadataProvider().getRelMetadata(
            rel,
            "getCostWithFilters",
            new Object[] { filters });
    }

    /**
     * Like {@link RelMetadataQuery#getColumnOrigins}, for a given output column
     * of an expression, determines all columns of underlying tables which
     * contribute to result values. The difference is if the column is derived
     * from a complex {@link RelNode}, then null is returned instead.
     *
     * <p>A 'complex RelNode' is a RelNode that we do not push {@link
     * org.eigenbase.rel.rules.SemiJoinRel}s past.
     *
     * @param rel the relational expression
     * @param iOutputColumn 0-based ordinal for output column of interest
     *
     * @return set of origin columns, or null if this information cannot be
     * determined (whereas empty set indicates definitely no origin columns at
     * all) or the column is derived from a complex RelNode.
     */
    public static Set<RelColumnOrigin> getSimpleColumnOrigins(
        RelNode rel,
        int iOutputColumn)
    {
        final Object o =
            rel.getCluster().getMetadataProvider().getRelMetadata(
                rel,
                "getSimpleColumnOrigins",
                new Object[] { iOutputColumn });
        return (Set<RelColumnOrigin>) o;
    }
}

// End LoptMetadataQuery.java
