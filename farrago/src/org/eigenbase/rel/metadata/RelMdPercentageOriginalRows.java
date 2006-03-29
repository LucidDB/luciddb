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
package org.eigenbase.rel.metadata;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;

import java.util.*;

/**
 * RelMdPercentageOriginalRows supplies a default implementation of
 * {@link RelMetadataQuery#getPercentageOriginalRows} for the standard
 * logical algebra.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class RelMdPercentageOriginalRows extends ReflectiveRelMetadataProvider
{
    public Double getPercentageOriginalRows(AggregateRelBase rel)
    {
        // REVIEW jvs 28-Mar-2006: The assumption here seems to be that
        // aggregation does not apply any filtering, so it does not modify the
        // percentage.  That's very much oversimplified.
        
        return RelMetadataQuery.getPercentageOriginalRows(
            rel.getChild());
    }

    public Double getPercentageOriginalRows(UnionRelBase rel)
    {
        double numerator = 0.0;
        double denominator = 0.0;

        // Ignore rel.isDistinct() because it's the same as an aggregate.

        // REVIEW jvs 28-Mar-2006: The original Broadbase formula was broken.
        // It was multiplying percentage into the numerator term rather than
        // than dividing it out of the denominator term, which would be OK if
        // there weren't summation going on.  Probably the cause of the error
        // was the desire to avoid division by zero, which I don't know how to
        // handle so I punt, meaning we return a totally wrong answer in the
        // case where a huge table has been completely filtered away.
        
        for (RelNode input : rel.getInputs()) {
            double rowCount = input.getRows();
            double percentage =
                RelMetadataQuery.getPercentageOriginalRows(input);
            if (percentage != 0.0) {
                denominator += (rowCount / percentage);
                numerator += rowCount;
            }
        }

        return quotientForPercentage(numerator, denominator);
    }
    
    public Double getPercentageOriginalRows(JoinRelBase rel)
    {
        // Assume any single-table filter conditions have already
        // been pushed down.
        
        // REVIEW jvs 28-Mar-2006: As with aggregation, this is
        // oversimplified.
        
        // REVIEW jvs 28-Mar-2006:  need any special casing for SemiJoinRel?
    
        double left =
            RelMetadataQuery.getPercentageOriginalRows(rel.getLeft());
        
        double right =
            RelMetadataQuery.getPercentageOriginalRows(rel.getRight());

        return left*right;
    }

    // Catch-all rule when none of the others apply.
    public Double getPercentageOriginalRows(RelNode rel)
    {
        if (rel.getInputs().length > 1) {
            // No generic formula available for multiple inputs.
            return null;
        }

        if (rel.getInputs().length == 0) {
            // Assume no filtering happening at leaf.
            return 1.0;
        }

        RelNode child = rel.getInputs()[0];

        double childPercentage =
            RelMetadataQuery.getPercentageOriginalRows(child);

        // Compute product of percentage filtering from this rel (assuming any
        // filtering is the effect of single-table filters) with the percentage
        // filtering performed by the child.
        double relPercentage = quotientForPercentage(
            rel.getRows(),
            child.getRows());
        return relPercentage * childPercentage;
    }

    private static double quotientForPercentage(
        double numerator,
        double denominator)
    {
        // may need epsilon instead
        if (denominator == 0.0) {
            // cap at 100%
            return 1.0;
        } else {
            return numerator / denominator;
        }
    }
}

// End RelMdPercentageOriginalRows.java
