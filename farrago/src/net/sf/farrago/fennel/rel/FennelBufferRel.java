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

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;


/**
 * FennelBufferRel represents the Fennel implementation of a buffering stream.
 * product.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class FennelBufferRel
    extends FennelSingleRel
{
    //~ Instance fields --------------------------------------------------------

    protected boolean inMemory;
    protected boolean multiPass;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FennelBufferRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param child child input
     * @param inMemory true if the buffering needs to be done only in memory
     * @param multiPass true if the buffer output will be read multiple times
     */
    public FennelBufferRel(
        RelOptCluster cluster,
        RelNode child,
        boolean inMemory,
        boolean multiPass)
    {
        super(cluster, child);
        this.inMemory = inMemory;
        this.multiPass = multiPass;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return true if the buffering needs to be done only in memory
     */
    boolean isInMemory()
    {
        return inMemory;
    }

    // implement Cloneable
    public FennelBufferRel clone()
    {
        FennelBufferRel clone =
            new FennelBufferRel(
                getCluster(),
                getChild().clone(),
                inMemory,
                multiPass);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        double rowCount = RelMetadataQuery.getRowCount(getChild());

        // NOTE zfong 9/6/06 - For now, I've arbitrarily set the I/O factor
        // to 1/10 the rowcount, which means that there are 10 rows per
        // page
        return planner.makeCost(
            rowCount,
            rowCount,
            rowCount / 10);
    }

    // implement RelNode
    public double getRows()
    {
        return RelMetadataQuery.getRowCount(getChild());
    }

    // override RelNode
    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String[] { "child", "inMemory", "multiPass" },
            new Object[] { inMemory, multiPass });
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemBufferingTupleStreamDef streamDef =
            repos.newFemBufferingTupleStreamDef();

        FemExecutionStreamDef childInput =
            implementor.visitFennelChild((FennelRel) getChild(), 0);
        implementor.addDataFlowFromProducerToConsumer(
            childInput,
            streamDef);
        streamDef.setInMemory(inMemory);
        streamDef.setMultipass(multiPass);
        return streamDef;
    }
}

// End FennelBufferRel.java
