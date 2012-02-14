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
import org.eigenbase.reltype.*;


/**
 * FennelMergeRel represents the Fennel implementation of UNION ALL.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FennelMergeRel
    extends FennelMultipleRel
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FennelMergeRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param inputs array of inputs
     */
    FennelMergeRel(
        RelOptCluster cluster,
        RelNode [] inputs)
    {
        super(cluster, inputs);
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelNode
    public double getRows()
    {
        return UnionRelBase.estimateRowCount(this);
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        return planner.makeCost(
            RelMetadataQuery.getRowCount(this),
            0.0,
            0.0);
    }

    // implement RelNode
    protected RelDataType deriveRowType()
    {
        // input to merge is supposed to be homogeneous, so just take
        // the first input type
        return inputs[0].getRowType();
    }

    // implement RelNode
    public FennelMergeRel clone()
    {
        FennelMergeRel clone =
            new FennelMergeRel(
                getCluster(),
                RelOptUtil.clone(inputs));
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement RelNode
    public void explain(RelOptPlanWriter pw)
    {
        String [] names = new String[inputs.length];

        for (int i = 0; i < inputs.length; i++) {
            names[i] = "child#" + i;
        }
        pw.explain(
            this,
            names,
            new Object[] {});
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);

        FemMergeStreamDef mergeStream = repos.newFemMergeStreamDef();
        mergeStream.setSequential(false);
        mergeStream.setPrePullInputs(false);

        for (int i = 0; i < inputs.length; i++) {
            FemExecutionStreamDef inputStream =
                implementor.visitFennelChild((FennelRel) inputs[i], i);
            implementor.addDataFlowFromProducerToConsumer(
                inputStream,
                mergeStream);
        }

        return mergeStream;
    }
}

// End FennelMergeRel.java
