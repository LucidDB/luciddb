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

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * FennelBernoulliSamplingRule converts a {@link SamplingRel} into a {@link
 * FennelBernoulliSamplingRel}, regardless of whether the original SamplingRel
 * specified Bernoulli or system sampling. By default Farrago doesn't not
 * support system sampling.
 *
 * @author Stephan Zuercher
 */
public class FennelBernoulliSamplingRule
    extends RelOptRule
{
    public static final FennelBernoulliSamplingRule instance =
        new FennelBernoulliSamplingRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FennelBernoulliSamplingRule.
     */
    private FennelBernoulliSamplingRule()
    {
        super(new RelOptRuleOperand(SamplingRel.class, ANY));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelRel.FENNEL_EXEC_CONVENTION;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        SamplingRel origRel = (SamplingRel) call.rels[0];

        RelOptSamplingParameters origParams = origRel.getSamplingParameters();

        RelOptSamplingParameters params =
            new RelOptSamplingParameters(
                true,
                origParams.getSamplingPercentage(),
                origParams.isRepeatable(),
                origParams.getRepeatableSeed());

        FennelBernoulliSamplingRel samplingRel =
            new FennelBernoulliSamplingRel(
                origRel.getCluster(),
                origRel.getChild(),
                params);

        call.transformTo(samplingRel);
    }
}

// End FennelBernoulliSamplingRule.java
