/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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

import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * An aggregate on bitmap data
 *
 * @author John Pham
 * @version $Id$
 */
public class LcsIndexAggRel
    extends FennelAggRel
{
    //~ Constructors -----------------------------------------------------------

    public LcsIndexAggRel(
        RelOptCluster cluster,
        RelNode child,
        int groupCount,
        Call [] aggCalls)
    {
        super(cluster, child, groupCount, aggCalls);
    }

    //~ Methods ----------------------------------------------------------------

    // implement AbstractRelNode
    public LcsIndexAggRel clone()
    {
        LcsIndexAggRel clone =
            new LcsIndexAggRel(
                getCluster(),
                getChild(),
                groupCount,
                aggCalls);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FemLbmSortedAggStreamDef aggStream =
            repos.newFemLbmSortedAggStreamDef();
        defineAggStream(aggStream);
        implementor.addDataFlowFromProducerToConsumer(
            implementor.visitFennelChild((FennelRel) getChild()),
            aggStream);

        return aggStream;
    }
}

// End LcsSortedAggRel.java
