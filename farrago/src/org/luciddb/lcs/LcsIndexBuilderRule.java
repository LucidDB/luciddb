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

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;

/**
 * LcsModificationRule is a rule for converting an abstract {@link
 * FarragoIndexBuilderRel} into a corresponding {@link LcsIndexBuilderRel}.
 *
 * <p>TODO: this rule was copied from FtrsIndexBuilderRule; consider
 * generalizing it.
 *
 * @author John Pham
 * @version $Id$
 */
class LcsIndexBuilderRule
    extends RelOptRule
{
    public static final LcsIndexBuilderRule instance =
        new LcsIndexBuilderRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a LcsIndexBuilderRule.
     */
    private LcsIndexBuilderRule()
    {
        super(
            new RelOptRuleOperand(
                FarragoIndexBuilderRel.class,
                ANY));
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
        FarragoIndexBuilderRel builderRel =
            (FarragoIndexBuilderRel) call.rels[0];

        if (!(builderRel.getTable() instanceof LcsTable)) {
            return;
        }

        RelNode inputRel = builderRel.getChild();
        RelNode fennelInput =
            mergeTraitsAndConvert(
                call.rels[0].getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION,
                inputRel);
        if (fennelInput == null) {
            return;
        }

        LcsIndexBuilderRel lcsRel =
            new LcsIndexBuilderRel(
                builderRel.getCluster(),
                fennelInput,
                builderRel.getIndex());

        call.transformTo(lcsRel);
    }
}

// End LcsIndexBuilderRule.java
