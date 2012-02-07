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
import org.eigenbase.rel.convert.*;
import org.eigenbase.relopt.*;


/**
 * FennelValuesRule provides an implementation for {@link ValuesRel} in terms of
 * {@link FennelValuesRel}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelValuesRule
    extends ConverterRule
{
    public static final FennelValuesRule instance =
        new FennelValuesRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FennelValuesRule.
     */
    private FennelValuesRule()
    {
        super(
            ValuesRel.class,
            CallingConvention.NONE,
            FennelRel.FENNEL_EXEC_CONVENTION,
            "FennelValuesRule");
    }

    //~ Methods ----------------------------------------------------------------

    // implement ConverterRule
    public RelNode convert(RelNode rel)
    {
        ValuesRel valuesRel = (ValuesRel) rel;
        RelTraitSet traitSet = valuesRel.getTraits();
        FennelValuesRel fennelRel =
            new FennelValuesRel(
                valuesRel.getCluster(),
                valuesRel.getRowType(),
                valuesRel.getTuples());
        // copy over the other traits
        for (int i = 0; i < traitSet.size(); i++) {
            RelTrait trait = traitSet.getTrait(i);
            if (trait.getTraitDef() != getTraitDef()) {
                if (fennelRel.getTraits().getTrait(trait.getTraitDef())
                    != null)
                {
                    fennelRel.getTraits().setTrait(trait.getTraitDef(), trait);
                } else {
                    fennelRel.getTraits().addTrait(trait);
                }
            }
        }
        return fennelRel;
    }
}

// End FennelValuesRule.java
