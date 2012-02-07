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
package net.sf.farrago.query;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * FarragoJavaUdxRule is a rule for transforming an abstract {@link
 * TableFunctionRel} into a {@link FarragoJavaUdxRel}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJavaUdxRule
    extends RelOptRule
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * The singleton instance.
     */
    public static final FarragoJavaUdxRule instance = new FarragoJavaUdxRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoJavaUdxRule object.
     */
    public FarragoJavaUdxRule()
    {
        super(new RelOptRuleOperand(TableFunctionRel.class, ANY));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return CallingConvention.ITERATOR;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        TableFunctionRel callRel = (TableFunctionRel) call.rels[0];
        final RelNode [] inputs = callRel.getInputs().clone();

        for (int i = 0; i < inputs.length; i++) {
            RelNode input = inputs[i];
            final RelTraitSet traits = RelOptUtil.clone(input.getTraits());

            // copy over other traits
            for (int j = 0; j < callRel.getTraits().size(); j++) {
                RelTrait trait = callRel.getTraits().getTrait(j);
                if (trait.getTraitDef()
                    != CallingConventionTraitDef.instance)
                {
                    if (traits.getTrait(trait.getTraitDef()) != null) {
                        traits.setTrait(trait.getTraitDef(), trait);
                    } else {
                        traits.addTrait(trait);
                    }
                }
            }
            inputs[i] =
                mergeTraitsAndConvert(
                    traits,
                    CallingConvention.ITERATOR,
                    input);
        }
        FarragoJavaUdxRel javaTableFunctionRel =
            new FarragoJavaUdxRel(
                callRel.getCluster(),
                callRel.getCall(),
                callRel.getRowType(),
                null,
                inputs);
        javaTableFunctionRel.setColumnMappings(callRel.getColumnMappings());
        call.transformTo(javaTableFunctionRel);
    }
}

// End FarragoJavaUdxRule.java
