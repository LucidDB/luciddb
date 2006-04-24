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
import org.eigenbase.rel.rules.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.fun.*;

import java.util.*;

/**
 * RelMdSelectivity supplies a default implementation of
 * {@link RelMetadataQuery#getSelectivity} for the standard logical algebra.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class RelMdSelectivity extends ReflectiveRelMetadataProvider
{
    public RelMdSelectivity()
    {
        // Tell superclass reflection about parameter types expected
        // for various metadata queries.

        // This corresponds to getSelectivity(rel, RexNode predicate);
        // note that we don't specify the rel type because we always overload
        // on that.
        mapParameterTypes(
            "getSelectivity",
            Collections.singletonList((Class) RexNode.class));
    }
    
    public Double getSelectivity(UnionRelBase rel, RexNode predicate)
    {
        if (rel.getInputs().length == 0 || predicate == null) {
            return 1.0;
        }
        
        double sumRows = 0.0;
        double sumSelectedRows = 0.0;
        for (RelNode input : rel.getInputs()) {
            Double nRows = RelMetadataQuery.getRowCount(input);
            if (nRows == null) {
                return null;
            }
            double sel = RelMetadataQuery.getSelectivity(input, predicate);
            
            sumRows += nRows;
            sumSelectedRows += nRows * sel;
        }
        
        if (sumRows < 1.0) {
            sumRows = 1.0;
        }
        return sumSelectedRows / sumRows;
    }
    
    public Double getSelectivity(SortRel rel, RexNode predicate)
    {
        return RelMetadataQuery.getSelectivity(rel.getChild(), predicate);
    }
    
    public Double getSelectivity(FilterRelBase rel, RexNode predicate)
    {
        // REVIEW zfong 4/12/06 - Broadbase takes the difference between
        // predicate and the rel's condition and only computes the
        // selectivity on those predicates.  By taking the difference, I
        // don't see where the selectivity of the filters associated with the
        // FilterRel would get computed.  So, instead, I'm taking the union
        // of the two, removing redundant filters.
        RexNode unionPreds = RelMdUtil.unionPreds(
            rel.getCluster().getRexBuilder(), predicate, rel.getCondition());
        
        return RelMetadataQuery.getSelectivity(rel.getChild(), unionPreds);
    }
    
    public Double getSelectivity(SemiJoinRel rel, RexNode predicate)
    {
        // create a RexNode representing the selectivity of the
        // semijoin filter and pass it to getSelectivity
        RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
        RexNode newPred = RelMdUtil.makeSemiJoinSelectivityRexNode(
            rel, null);
        if (predicate != null) {
            newPred = rexBuilder.makeCall(
                SqlStdOperatorTable.andOperator, newPred, predicate);
        }
        
        return RelMetadataQuery.getSelectivity(
            rel.getLeft(), newPred);
    }
    
    // Catch-all rule when none of the others apply.  Have not implemented
    // rules for aggregation and projection.
    public Double getSelectivity(RelNode rel, RexNode predicate)
    {
        return RelMdUtil.guessSelectivity(predicate);
    }
}

// End RelMdSelectivity.java
