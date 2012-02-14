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
 * FennelBernoulliSamplingRel implements Bernoulli-style table sampling using a
 * generic Fennel XO.
 *
 * @author Stephan Zuercher
 */
public class FennelBernoulliSamplingRel
    extends FennelSingleRel
{
    //~ Instance fields --------------------------------------------------------

    private final RelOptSamplingParameters samplingParams;

    //~ Constructors -----------------------------------------------------------

    public FennelBernoulliSamplingRel(
        RelOptCluster cluster,
        RelNode child,
        RelOptSamplingParameters samplingParams)
    {
        super(cluster, child);

        this.samplingParams = samplingParams;

        assert (samplingParams.isBernoulli());
    }

    //~ Methods ----------------------------------------------------------------

    public RelNode clone()
    {
        FennelBernoulliSamplingRel clone =
            new FennelBernoulliSamplingRel(
                getCluster(),
                getChild().clone(),
                samplingParams);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement RelNode
    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String[] { "child", "rate", "repeatableSeed" },
            new Object[] {
                samplingParams.getSamplingPercentage(),
                samplingParams.isRepeatable()
                ? samplingParams.getRepeatableSeed()
                : "-"
            });
    }

    // implement RelNode
    public double getRows()
    {
        double rows = RelMetadataQuery.getRowCount(getChild());

        return rows * (double) samplingParams.getSamplingPercentage();
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemBernoulliSamplingStreamDef streamDef =
            repos.newFemBernoulliSamplingStreamDef();

        FemExecutionStreamDef childInput =
            implementor.visitFennelChild((FennelRel) getChild(), 0);

        implementor.addDataFlowFromProducerToConsumer(
            childInput,
            streamDef);

        streamDef.setOutputDesc(
            FennelRelUtil.createTupleDescriptorFromRowType(
                repos,
                getCluster().getRexBuilder().getTypeFactory(),
                getRowType()));
        streamDef.setSamplingRate(samplingParams.getSamplingPercentage());
        streamDef.setRepeatable(samplingParams.isRepeatable());
        streamDef.setRepeatableSeed(samplingParams.getRepeatableSeed());

        return streamDef;
    }
}

// End FennelBernoulliSamplingRel.java
