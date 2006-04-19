/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package net.sf.farrago.defimpl;

import com.disruptivetech.farrago.rel.*;

import com.disruptivetech.farrago.volcano.*;

import net.sf.farrago.query.*;
import net.sf.farrago.session.*;
import net.sf.farrago.fem.config.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;

/**
 * FarragoDefaultPlanner extends {@link VolcanoPlanner} to request
 * Farrago-specific optimizations.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoDefaultPlanner extends VolcanoPlanner
    implements FarragoSessionPlanner
{
    //~ Instance fields -------------------------------------------------------

    private FarragoPreparingStmt stmt;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoDefaultPlanner object.
     *
     * @param stmt statement on whose behalf this planner operates
     */
    protected FarragoDefaultPlanner(FarragoSessionPreparingStmt stmt)
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

        addRule(new AbstractConverter.ExpandConversionRule());
    }

    //~ Methods ---------------------------------------------------------------

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
        planner.addRule(new RemoveDistinctRule());
        planner.addRule(RemoveDistinctAggregateRule.instance);
        planner.addRule(ExtractJoinFilterRule.instance);
        planner.addRule(new UnionToDistinctRule());
        planner.addRule(new UnionEliminatorRule());
        // for set operations, we coerce names to match so that
        // Java implementations can pass row objects through without
        // copying
        planner.addRule(
            new CoerceInputsRule(UnionRel.class, true));
        planner.addRule(
            new CoerceInputsRule(IntersectRel.class, true));
        planner.addRule(
            new CoerceInputsRule(MinusRel.class, true));
        // for DML, name coercion isn't helpful
        planner.addRule(
            new CoerceInputsRule(TableModificationRel.class, false));
        planner.addRule(new SwapJoinRule());
        planner.addRule(new RemoveTrivialProjectRule());
        planner.addRule(new FarragoMultisetSplitterRule());
        planner.addRule(FarragoJavaUdxRule.instance);

        planner.addRule(new IterRules.HomogeneousUnionToIteratorRule());
        planner.addRule(new IterRules.OneRowToIteratorRule());

        planner.addRule(
            new ReduceDecimalsRule(CalcRel.class));
        
        planner.addRule(new PushFilterRule());
        
        if (fennelEnabled) {
            planner.addRule(new FennelSortRule());
            planner.addRule(new FennelCollectRule());
            planner.addRule(new FennelUncollectRule());
            // TODO jvs 9-Apr-2006:  Eliminate FennelDistinctSortRule
            // entirely; it has been generalized to FennelAggRule.
            planner.addRule(new FennelDistinctSortRule());
            planner.addRule(new FennelRenameRule());
            planner.addRule(new FennelCartesianJoinRule());
            planner.addRule(new FennelCorrelatorRule());
            planner.addRule(new FennelOneRowRule());
            planner.addRule(new FennelAggRule());
        }

        // Add the rule to introduce FennelCalcRel's only if the fennel
        // calculator is enabled.
        if (calcVM.equals(CalcVirtualMachineEnum.CALCVM_FENNEL)) {
            // use Fennel for calculating expressions
            assert fennelEnabled;
            planner.addRule(FennelCalcRule.instance);

            // REVIEW jvs 13-Nov-2005: I put FennelUnionRule here instead of in
            // fennelEnabled block above because I want to be able to test
            // both implementations, and currently the only way to control
            // that is via the calc parameter.  Probably need a more
            // general parameter controlling all rels in case of
            // overlap, not just calc.
            planner.addRule(FennelUnionRule.instance);
        }

        if (calcVM.equals(CalcVirtualMachineEnum.CALCVM_JAVA)
                || calcVM.equals(CalcVirtualMachineEnum.CALCVM_AUTO)) {
            // use Java code generation for calculating expressions
            planner.addRule(IterRules.IterCalcRule.instance);
        }

        if (calcVM.equals(CalcVirtualMachineEnum.CALCVM_AUTO) && fennelEnabled) {
            // add rule for pure calculator usage plus rule for
            // decomposing rels into mixed Java/Fennel impl
            planner.addRule(FennelCalcRule.instance);
            planner.addRule(FarragoAutoCalcRule.instance);

            // see REVIEW 13-Nov-2005 comment above
            planner.addRule(FennelUnionRule.instance);
        }

        if (fennelEnabled) {
            FennelToIteratorConverter.register(planner);
            IteratorToFennelConverter.register(planner);
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
