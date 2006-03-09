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
package com.lucidera.lcs;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.query.*;
import net.sf.farrago.type.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.RexNode;

/**
 * LcsIndexMergeRel is a relation for merging the results of an index scan. 
 * The input to this relation must be a single input and it must be an 
 * LcsIndexSearchRel. The input data consists of unordered rid segments. 
 * The result set produced by this relation will be ordered rid segments.
 *
 * @author John Pham
 * @version $Id$
 */
class LcsIndexMergeRel extends FennelSingleRel
{
    //~ Instance fields -------------------------------------------------------

    final LcsTable lcsTable;
    int ridLimitParamId;
    int consumerSridParamId;
    int segmentLimitParamId;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new LcsIndexMergeRel object.
     *
     * @param indexSearchRel the input to this merge
     */
    public LcsIndexMergeRel(
        LcsTable lcsTable,
        LcsIndexSearchRel indexSearchRel,
        int consumerSridParamId,
        int segmentLimitParamId,
        int ridLimitParamId)
    {
        super(indexSearchRel.getCluster(), indexSearchRel);
        this.lcsTable = lcsTable;
        
        // These two parameters are used when there's an AND 
        // downstream(as consumer)
        this.consumerSridParamId = consumerSridParamId;
        this.segmentLimitParamId = segmentLimitParamId;

        // This parameter is used when there's a "chopper"
        // upstream(as producer)
        this.ridLimitParamId = ridLimitParamId;
    }

    //~ Methods ---------------------------------------------------------------

    // implement Cloneable
    public Object clone()
    {
        return new LcsIndexMergeRel(
            lcsTable,
            (LcsIndexSearchRel) RelOptUtil.clone(getChild()),
            consumerSridParamId,
            segmentLimitParamId,
            ridLimitParamId);
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        // TODO:  the real thing(sorter costing + merge cost)
        return planner.makeTinyCost();   
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        //
        // First obtain the child stream
        //
        FemExecutionStreamDef search = 
            implementor.visitFennelChild((FennelRel) getChild());
        
        //
        // Chop the tuples so they fit in memory when expanded
        //
        FemLbmChopperStreamDef chopper = repos.newFemLbmChopperStreamDef();
        chopper.setRidLimitParamId(ridLimitParamId);
        implementor.addDataFlowFromProducerToConsumer(search, chopper);
        
        //
        // Sort the stream
        //
        FemSortingStreamDef sorter = 
            lcsTable.getIndexGuide().newBitmapSorter();
        implementor.addDataFlowFromProducerToConsumer(chopper, sorter);

        //
        // Merge the results
        //
        FemLbmUnionStreamDef merge = repos.newFemLbmUnionStreamDef();

        merge.setConsumerSridParamId(consumerSridParamId);
        merge.setSegmentLimitParamId(segmentLimitParamId);
        
        merge.setRidLimitParamId(ridLimitParamId);

        implementor.addDataFlowFromProducerToConsumer(sorter, merge);
        
        return merge;
    }
}

// End LcsIndexMergeRel.java
