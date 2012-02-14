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
package net.sf.farrago.fennel.rel;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * FennelAggRel represents the Fennel implementation of aggregation.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelAggRel
    extends AggregateRelBase
    implements FennelRel
{
    //~ Instance fields --------------------------------------------------------

    protected final FarragoRepos repos;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FennelAggRel.
     *
     * @param cluster Cluster
     * @param child Child
     * @param groupCount Size of grouping key
     * @param aggCalls Collection of calls to aggregate functions
     */
    public FennelAggRel(
        RelOptCluster cluster,
        RelNode child,
        int groupCount,
        List<AggregateCall> aggCalls)
    {
        super(
            cluster,
            new RelTraitSet(FennelRel.FENNEL_EXEC_CONVENTION),
            child,
            groupCount,
            aggCalls);
        repos = FennelRelUtil.getRepos(this);
    }

    //~ Methods ----------------------------------------------------------------

    public FennelAggRel clone()
    {
        FennelAggRel clone =
            new FennelAggRel(
                getCluster(),
                getChild().clone(),
                groupCount,
                aggCalls);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement FennelRel
    public RelFieldCollation [] getCollations()
    {
        // TODO jvs 5-Oct-2005: if group keys are already sorted,
        // non-hash agg will preserve them; full-table agg is
        // trivially sorted (only one row of output)
        return RelFieldCollation.emptyCollationArray;
    }

    // implement FennelRel
    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        return implementor.visitChild(
            this,
            0,
            getChild());
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FemSortedAggStreamDef aggStream = repos.newFemSortedAggStreamDef();
        FennelRelUtil.defineAggStream(aggCalls, groupCount, repos, aggStream);
        implementor.addDataFlowFromProducerToConsumer(
            implementor.visitFennelChild((FennelRel) getChild(), 0),
            aggStream);

        return aggStream;
    }
}

// End FennelAggRel.java
