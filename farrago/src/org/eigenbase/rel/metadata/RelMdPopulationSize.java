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


/**
 * RelMdPopulationSize supplies a default implementation of {@link
 * RelMetadataQuery#getPopulationSize} for the standard logical algebra.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class RelMdPopulationSize
    extends ReflectiveRelMetadataProvider
{
    //~ Constructors -----------------------------------------------------------

    public RelMdPopulationSize()
    {
        // Tell superclass reflection about parameter types expected
        // for various metadata queries.

        // This corresponds to getPopulationSize(rel, BitSet groupKey);
        // note that we don't specify the rel type because we always overload
        // on that.
        mapParameterTypes(
            "getPopulationSize",
            Collections.singletonList((Class) BitSet.class));
    }

    //~ Methods ----------------------------------------------------------------

    public Double getPopulationSize(FilterRelBase rel, BitSet groupKey)
    {
        return RelMetadataQuery.getPopulationSize(
            rel.getChild(),
            groupKey);
    }

    public Double getPopulationSize(SortRel rel, BitSet groupKey)
    {
        return RelMetadataQuery.getPopulationSize(
            rel.getChild(),
            groupKey);
    }

    public Double getPopulationSize(UnionRelBase rel, BitSet groupKey)
    {
        Double population = 0.0;
        for (RelNode input : rel.getInputs()) {
            Double subPop = RelMetadataQuery.getPopulationSize(input, groupKey);
            if (subPop == null) {
                return null;
            }
            population += subPop;
        }
        return population;
    }

    public Double getPopulationSize(JoinRelBase rel, BitSet groupKey)
    {
        return RelMdUtil.getJoinPopulationSize(rel, groupKey);
    }

    public Double getPopulationSize(SemiJoinRel rel, BitSet groupKey)
    {
        return RelMetadataQuery.getPopulationSize(
            rel.getLeft(),
            groupKey);
    }

    public Double getPopulationSize(AggregateRelBase rel, BitSet groupKey)
    {
        BitSet childKey = new BitSet();
        RelMdUtil.setAggChildKeys(groupKey, rel, childKey);
        return RelMetadataQuery.getPopulationSize(
            rel.getChild(),
            childKey);
    }

    public Double getPopulationSize(ValuesRelBase rel, BitSet groupKey)
    {
        // assume half the rows are duplicates
        return rel.getRows() / 2;
    }

    public Double getPopulationSize(ProjectRelBase rel, BitSet groupKey)
    {
        BitSet baseCols = new BitSet();
        BitSet projCols = new BitSet();
        RexNode [] projExprs = rel.getChildExps();
        RelMdUtil.splitCols(projExprs, groupKey, baseCols, projCols);

        Double population =
            RelMetadataQuery.getPopulationSize(
                rel.getChild(),
                baseCols);
        if (population == null) {
            return null;
        }

        // No further computation required if the projection expressions are
        // all column references
        if (projCols.cardinality() == 0) {
            return population;
        }

        for (
            int bit = projCols.nextSetBit(0);
            bit >= 0;
            bit = projCols.nextSetBit(bit + 1))
        {
            Double subRowCount = RelMdUtil.cardOfProjExpr(rel, projExprs[bit]);
            if (subRowCount == null) {
                return null;
            }
            population *= subRowCount;
        }

        // REVIEW zfong 6/22/06 - Broadbase did not have the call to
        // numDistinctVals.  This is needed; otherwise, population can be
        // larger than the number of rows in the RelNode.
        return RelMdUtil.numDistinctVals(
            population,
            RelMetadataQuery.getRowCount(rel));
    }

    // Catch-all rule when none of the others apply.
    public Double getPopulationSize(RelNode rel, BitSet groupKey)
    {
        // if the keys are unique, return the row count; otherwise, we have
        // no further information on which to return any legitimate value

        // REVIEW zfong 4/11/06 - Broadbase code returns the product of each
        // unique key, which would result in the population being larger
        // than the total rows in the relnode
        boolean uniq = RelMdUtil.areColumnsDefinitelyUnique(rel, groupKey);
        if (uniq) {
            return RelMetadataQuery.getRowCount(rel);
        }

        return null;
    }
}

// End RelMdPopulationSize.java
