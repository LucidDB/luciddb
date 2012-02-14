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
 * LhxAggRel represents hash aggregation.
 *
 * @author Rushan Chen
 * @version $Id$
 */
public class LhxAggRel
    extends AggregateRelBase
    implements FennelRel
{
    //~ Instance fields --------------------------------------------------------

    /**
     * row count of the input
     */
    long numInputRows;

    /**
     * cardinality of the group by key
     */
    long cndGroupByKey;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a LhxAggRel.
     *
     * @param cluster Cluster
     * @param child Child
     * @param groupCount Size of grouping key
     * @param aggCalls Collection of calls to aggregate functions
     * @param numInputRows Row count of the input
     * @param cndGroupByKey Cardinality of the grouping key
     */
    public LhxAggRel(
        RelOptCluster cluster,
        RelNode child,
        int groupCount,
        List<AggregateCall> aggCalls,
        long numInputRows,
        long cndGroupByKey)
    {
        super(
            cluster,
            new RelTraitSet(FennelRel.FENNEL_EXEC_CONVENTION),
            child,
            groupCount,
            aggCalls);
        this.numInputRows = numInputRows;
        this.cndGroupByKey = cndGroupByKey;
    }

    //~ Methods ----------------------------------------------------------------

    public LhxAggRel clone()
    {
        LhxAggRel clone =
            new LhxAggRel(
                getCluster(),
                getChild().clone(),
                groupCount,
                aggCalls,
                numInputRows,
                cndGroupByKey);
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
        final FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemLhxAggStreamDef aggStream = repos.newFemLhxAggStreamDef();
        FennelRelUtil.defineAggStream(aggCalls, groupCount, repos, aggStream);
        aggStream.setNumRows(numInputRows);
        aggStream.setCndGroupByKeys(cndGroupByKey);

        implementor.addDataFlowFromProducerToConsumer(
            implementor.visitFennelChild((FennelRel) getChild(), 0),
            aggStream);

        return aggStream;
    }
}

// End LhxAggRel.java
