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
 * FennelCollectRule is a rule to implement a call made with the {@link
 * org.eigenbase.sql.fun.SqlMultisetValueConstructor}
 *
 * @author Wael Chatila
 * @version $Id$
 * @since Dec 11, 2004
 */
public class FennelCollectRule
    extends RelOptRule
{
    public static final FennelCollectRule instance =
        new FennelCollectRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FennelCollectRule.
     */
    private FennelCollectRule()
    {
        super(
            new RelOptRuleOperand(
                CollectRel.class,
                ANY));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelRel.FENNEL_EXEC_CONVENTION;
    }

    public void onMatch(RelOptRuleCall call)
    {
        CollectRel collectRel = (CollectRel) call.rels[0];
        RelNode relInput = collectRel.getChild();
        RelNode fennelInput =
            mergeTraitsAndConvert(
                collectRel.getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION,
                relInput);
        if (fennelInput == null) {
            return;
        }

        FennelPullCollectRel fennelCollectRel =
            new FennelPullCollectRel(
                collectRel.getCluster(),
                fennelInput,
                collectRel.getFieldName());
        call.transformTo(fennelCollectRel);
    }
}

// End FennelCollectRule.java
