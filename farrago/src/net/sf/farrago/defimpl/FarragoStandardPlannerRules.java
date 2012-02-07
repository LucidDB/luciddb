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
package net.sf.farrago.defimpl;

import net.sf.farrago.fem.config.*;
import net.sf.farrago.fennel.rel.*;
import net.sf.farrago.query.*;
import net.sf.farrago.session.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;


/**
 * Registers standard rules needed by most planner implementations.
 *
 * @author John Sichi
 * @version $Id$
 */
public class FarragoStandardPlannerRules
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Adds a set of default rules to a planner.
     *
     * @param planner Planner
     * @param fennelEnabled Whether fennel is enabled.
     * @param calcVM Flavor of calculator being used.
     */
    public static void addDefaultRules(
        FarragoSessionPlanner planner,
        boolean fennelEnabled,
        CalcVirtualMachine calcVM)
    {
        planner.addRule(RemoveDistinctRule.instance);
        planner.addRule(RemoveDistinctAggregateRule.instance);
        planner.addRule(ExtractJoinFilterRule.instance);
        planner.addRule(UnionToDistinctRule.instance);
        planner.addRule(UnionEliminatorRule.instance);

        // for set operations, we coerce names to match so that
        // Java implementations can pass row objects through without
        // copying
        planner.addRule(new CoerceInputsRule(UnionRel.class, true));
        planner.addRule(new CoerceInputsRule(IntersectRel.class, true));
        planner.addRule(new CoerceInputsRule(MinusRel.class, true));

        // for DML, name coercion isn't helpful
        planner.addRule(
            new CoerceInputsRule(TableModificationRel.class, false));
        planner.addRule(SwapJoinRule.instance);
        planner.addRule(RemoveTrivialProjectRule.instance);
        planner.addRule(RemoveTrivialCalcRule.instance);
        planner.addRule(FarragoJavaUdxRule.instance);

        planner.addRule(IterRules.HomogeneousUnionToIteratorRule.instance);
        planner.addRule(IterRules.OneRowToIteratorRule.instance);

        planner.addRule(ReduceDecimalsRule.instance);

        planner.addRule(FarragoReduceExpressionsRule.filterInstance);
        planner.addRule(FarragoReduceExpressionsRule.projectInstance);
        planner.addRule(FarragoReduceExpressionsRule.joinInstance);
        planner.addRule(FarragoReduceExpressionsRule.calcInstance);
        planner.addRule(FarragoReduceValuesRule.filterInstance);
        planner.addRule(FarragoReduceValuesRule.projectInstance);
        planner.addRule(FarragoReduceValuesRule.projectFilterInstance);

        planner.addRule(ReduceAggregatesRule.instance);

        // NOTE zfong 9/27/06: PullUpProjectsAboveJoinRule has not been
        // added because together with PushProjectPastJoinRule, it causes
        // Volcano to go into an infinite loop

        planner.addRule(PushFilterPastJoinRule.instance);
        planner.addRule(PushFilterPastSetOpRule.instance);
        planner.addRule(MergeFilterRule.instance);
        planner.addRule(PushFilterPastProjectRule.instance);
        planner.addRule(PushProjectPastFilterRule.instance);
        planner.addRule(PushProjectPastJoinRule.instance);
        planner.addRule(PushProjectPastSetOpRule.instance);
        planner.addRule(MergeProjectRule.instance);

        if (fennelEnabled) {
            planner.addRule(FennelSortRule.instance);
            planner.addRule(FennelDistinctSortRule.instance);
            planner.addRule(FennelRenameRule.instance);
            planner.addRule(FennelCartesianJoinRule.instance);
            planner.addRule(FennelOneRowRule.instance);
            planner.addRule(FennelValuesRule.instance);
            planner.addRule(FennelEmptyRule.instance);
            planner.addRule(FennelAggRule.instance);
            planner.addRule(FennelReshapeRule.instance);
        }

        // Add the rule to introduce FennelCalcRel's only if the fennel
        // calculator is enabled.
        if (calcVM.equals(CalcVirtualMachineEnum.CALCVM_FENNEL)) {
            // use Fennel for calculating expressions
            assert fennelEnabled;
            FarragoDefaultPlanner.addFennelCalcRules(planner, false);

            // REVIEW jvs 13-Nov-2005: I put FennelUnionRule here instead of in
            // fennelEnabled block above because I want to be able to test both
            // implementations, and currently the only way to control that is
            // via the calc parameter.  Probably need a more general parameter
            // controlling all rels in case of overlap, not just calc.
            planner.addRule(FennelUnionRule.instance);
        }

        if (calcVM.equals(CalcVirtualMachineEnum.CALCVM_JAVA)
            || calcVM.equals(CalcVirtualMachineEnum.CALCVM_AUTO))
        {
            // use Java code generation for calculating expressions
            planner.addRule(IterRules.IterCalcRule.instance);
        }

        if (calcVM.equals(CalcVirtualMachineEnum.CALCVM_AUTO)
            && fennelEnabled)
        {
            // add rule for pure calculator usage plus rule for
            // decomposing rels into mixed Java/Fennel impl
            FarragoDefaultPlanner.addFennelCalcRules(planner, true);

            // see REVIEW 13-Nov-2005 comment above
            planner.addRule(FennelUnionRule.instance);
        }

        if (fennelEnabled) {
            FennelToIteratorConverter.register(planner);
            IteratorToFennelConverter.register(planner);
        }
    }
}

// End FarragoStandardPlannerRules.java
