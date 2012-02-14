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
 * FennelUncollectRule is a rule to implement a call with the {@link
 * org.eigenbase.sql.fun.SqlStdOperatorTable#unnestOperator}
 *
 * @author Wael Chatila
 * @version $Id$
 * @since Dec 12, 2004
 */
public class FennelUncollectRule
    extends RelOptRule
{
    public static final FennelUncollectRule instance =
        new FennelUncollectRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FennelUncollectRule.
     */
    private FennelUncollectRule()
    {
        super(
            new RelOptRuleOperand(
                UncollectRel.class,
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
        UncollectRel uncollectRel = (UncollectRel) call.rels[0];
        RelNode relInput = uncollectRel.getChild();
        RelNode fennelInput =
            mergeTraitsAndConvert(
                uncollectRel.getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION,
                relInput);
        if (fennelInput == null) {
            return;
        }

        FennelPullUncollectRel fennelUncollectRel =
            new FennelPullUncollectRel(
                uncollectRel.getCluster(),
                fennelInput);
        call.transformTo(fennelUncollectRel);
    }
}

// End FennelUncollectRule.java
