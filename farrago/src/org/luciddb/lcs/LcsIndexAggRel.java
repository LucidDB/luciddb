/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package org.luciddb.lcs;

import java.util.*;

import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fennel.rel.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * An aggregate on bitmap data.
 *
 * @author John Pham
 * @version $Id$
 */
public class LcsIndexAggRel
    extends FennelAggRel
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an LcsIndexAggRel.
     *
     * @param cluster
     * @param child
     * @param groupCount
     * @param aggCalls
     */
    public LcsIndexAggRel(
        RelOptCluster cluster,
        RelNode child,
        int groupCount,
        List<AggregateCall> aggCalls)
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
        FennelRelUtil.defineAggStream(aggCalls, groupCount, repos, aggStream);
        implementor.addDataFlowFromProducerToConsumer(
            implementor.visitFennelChild((FennelRel) getChild(), 0),
            aggStream);

        return aggStream;
    }
}

// End LcsIndexAggRel.java
