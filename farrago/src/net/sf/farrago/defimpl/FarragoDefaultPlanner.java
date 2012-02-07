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
import org.eigenbase.relopt.volcano.*;

// TODO jvs 3-May-2006:  Rename this to FarragoDefaultVolcanoPlanner


/**
 * FarragoDefaultPlanner extends {@link VolcanoPlanner} to request
 * Farrago-specific optimizations.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoDefaultPlanner
    extends VolcanoPlanner
    implements FarragoSessionPlanner
{
    //~ Instance fields --------------------------------------------------------

    private FarragoPreparingStmt stmt;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoDefaultPlanner object.
     *
     * @param stmt statement on whose behalf this planner operates
     */
    public FarragoDefaultPlanner(FarragoSessionPreparingStmt stmt)
    {
        this.stmt = (FarragoPreparingStmt) stmt;

        // Yon Cassius has a lean and hungry look.
        ambitious = true;

        // Create a new CallingConvention trait definition that will store
        // the graph of possible conversions and handle the creation of
        // converters.
        addRelTraitDef(CallingConventionTraitDef.instance);

        // NOTE: don't call IterConverterRel.init and friends; their presence
        // just confuses the optimizer, and we explicitly supply all the
        // conversion rules we need
        RelOptUtil.registerAbstractRels(this);

        addRule(AbstractConverter.ExpandConversionRule.instance);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Initializes Farrago-specific rules for this planner.
     */
    public void init()
    {
        final boolean fennelEnabled = stmt.getRepos().isFennelEnabled();
        final CalcVirtualMachine calcVM =
            stmt.getRepos().getCurrentConfig().getCalcVirtualMachine();
        addStandardRules(this, fennelEnabled, calcVM);
    }

    /**
     * Adds a set of standard rules to a planner.
     *
     * @param planner Planner
     * @param fennelEnabled Whether fennel is enabled.
     * @param calcVM Flavor of calculator being used.
     */
    public static void addStandardRules(
        FarragoSessionPlanner planner,
        boolean fennelEnabled,
        CalcVirtualMachine calcVM)
    {
        FarragoStandardPlannerRules.addDefaultRules(
            planner,
            fennelEnabled,
            calcVM);
        planner.addRule(FarragoMultisetSplitterRule.instance);
        if (fennelEnabled) {
            planner.addRule(FennelCollectRule.instance);
            planner.addRule(FennelUncollectRule.instance);
            planner.addRule(FennelCorrelatorRule.instance);
        }
    }

    // NOTE jvs 22-Mar-2007: separate method from
    // FarragoStandardPlannerRules.addStandardRules to avoid direct dependency
    // on com.disruptivetech from there
    public static void addFennelCalcRules(
        FarragoSessionPlanner planner,
        boolean auto)
    {
        planner.addRule(FennelCalcRule.instance);
        if (auto) {
            planner.addRule(FarragoAutoCalcRule.instance);
        }
    }

    // implement FarragoSessionPlanner
    public FarragoSessionPreparingStmt getPreparingStmt()
    {
        return stmt;
    }

    // implement FarragoSessionPlanner
    public void beginMedPluginRegistration(String serverClassName)
    {
        // don't care
    }

    // implement FarragoSessionPlanner
    public void endMedPluginRegistration()
    {
        // don't care
    }

    // override VolcanoPlanner
    public JavaRelImplementor getJavaRelImplementor(RelNode rel)
    {
        return stmt.getRelImplementor(rel.getCluster().getRexBuilder());
    }
}

// End FarragoDefaultPlanner.java
