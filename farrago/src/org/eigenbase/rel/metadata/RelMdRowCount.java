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
package org.eigenbase.rel.metadata;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.rex.*;
import org.eigenbase.util14.*;


/**
 * RelMdRowCount supplies a default implementation of {@link
 * RelMetadataQuery#getRowCount} for the standard logical algebra.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class RelMdRowCount
    extends ReflectiveRelMetadataProvider
{
    //~ Methods ----------------------------------------------------------------

    public Double getRowCount(UnionRelBase rel)
    {
        double nRows = 0.0;

        for (RelNode input : rel.getInputs()) {
            Double partialRowCount = RelMetadataQuery.getRowCount(input);
            if (partialRowCount == null) {
                return null;
            }
            nRows += partialRowCount;
        }
        return nRows;
    }

    public Double getRowCount(FilterRelBase rel)
    {
        return NumberUtil.multiply(
            RelMetadataQuery.getSelectivity(
                rel.getChild(),
                rel.getCondition()),
            RelMetadataQuery.getRowCount(rel.getChild()));
    }

    public Double getRowCount(ProjectRelBase rel)
    {
        return RelMetadataQuery.getRowCount(rel.getChild());
    }

    public Double getRowCount(SortRel rel)
    {
        return RelMetadataQuery.getRowCount(rel.getChild());
    }

    public Double getRowCount(SemiJoinRel rel)
    {
        // create a RexNode representing the selectivity of the
        // semijoin filter and pass it to getSelectivity
        RexNode semiJoinSelectivity =
            RelMdUtil.makeSemiJoinSelectivityRexNode(rel);

        return NumberUtil.multiply(
            RelMetadataQuery.getSelectivity(
                rel.getLeft(),
                semiJoinSelectivity),
            RelMetadataQuery.getRowCount(rel.getLeft()));
    }

    public Double getRowCount(AggregateRelBase rel)
    {
        BitSet groupKey = new BitSet();
        for (int i = 0; i < rel.getGroupCount(); i++) {
            groupKey.set(i);
        }

        // rowcount is the cardinality of the group by columns
        Double distinctRowCount =
            RelMetadataQuery.getDistinctRowCount(
                rel.getChild(),
                groupKey,
                null);
        if (distinctRowCount == null) {
            return RelMetadataQuery.getRowCount(rel.getChild()) / 10;
        } else {
            return distinctRowCount;
        }
    }

    // Catch-all rule when none of the others apply.
    public Double getRowCount(RelNode rel)
    {
        return rel.getRows();
    }
}

// End RelMdRowCount.java
