/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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

import org.eigenbase.oj.*;
import org.eigenbase.oj.rel.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.relopt.*;
import org.eigenbase.util.*;


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
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Initializes Farrago-specific rules for this planner.
     */
    public void init()
    {
        boolean fennelEnabled = stmt.getRepos().isFennelEnabled();

        // Create a new CallingConvention trait definition that will store
        // the graph of possible conversions and handle the creation of
        // converters.
        addRelTraitDef(CallingConventionTraitDef.instance);

        // NOTE: don't call IterConverterRel.init and friends; their presence
        // just confuses the optimizer, and we explicitly supply all the
        // conversion rules we need
        RelOptUtil.registerAbstractRels(this);

        addRule(new AbstractConverter.ExpandConversionRule());
        addRule(new RemoveDistinctRule());
        addRule(new UnionToDistinctRule());
        addRule(new UnionEliminatorRule());
        addRule(new CoerceInputsRule(UnionRel.class));
        addRule(new CoerceInputsRule(TableModificationRel.class));
        addRule(new SwapJoinRule());
        addRule(new RemoveTrivialProjectRule());

        addRule(new IterRules.HomogeneousUnionToIteratorRule());
        addRule(new IterRules.OneRowToIteratorRule());

        if (fennelEnabled) {
            addRule(new FennelSortRule());
            addRule(new FennelCollectRule());
            addRule(new FennelUncollectRule());
            addRule(new FennelDistinctSortRule());
            addRule(
                new FennelRenameRule(FennelPullRel.FENNEL_PULL_CONVENTION,
                    "FennelPullRenameRule"));
            addRule(new FennelCartesianJoinRule());
            addRule(new FennelCorrelatorRule());
            addRule(new FennelOneRowRule());
        }

        // Add the rule to introduce FennelCalcRel's only if the fennel
        // calculator is enabled.
        final CalcVirtualMachine calcVM =
            stmt.getRepos().getCurrentConfig().getCalcVirtualMachine();
        if (calcVM.equals(CalcVirtualMachineEnum.CALCVM_FENNEL)) {
            // use Fennel for calculating expressions
            assert (fennelEnabled);
            addRule(FennelCalcRule.instance);
        }

        if (calcVM.equals(CalcVirtualMachineEnum.CALCVM_JAVA)
                || calcVM.equals(CalcVirtualMachineEnum.CALCVM_AUTO)) {
            // use Java code generation for calculating expressions
            addRule(IterRules.IterCalcRule.instance);

            // TODO jvs 6-May-2004:  these should be redundant now, but when
            // I remove them, some queries fail.  Find out why.
            addRule(IterRules.ProjectToIteratorRule.instance);
            addRule(IterRules.ProjectedFilterToIteratorRule.instance);
        }

        if (calcVM.equals(CalcVirtualMachineEnum.CALCVM_AUTO) && fennelEnabled) {
            // add rule for pure calculator usage plus rule for
            // decomposing rels into mixed Java/Fennel impl
            addRule(FennelCalcRule.instance);
            addRule(FarragoAutoCalcRule.instance);
        }

        if (fennelEnabled) {
            FennelToIteratorConverter.register(this);
            IteratorToFennelConverter.register(this);
        }
    }

    public FarragoSessionPreparingStmt getPreparingStmt()
    {
        return stmt;
    }

    // override VolcanoPlanner
    public JavaRelImplementor getJavaRelImplementor(RelNode rel)
    {
        return stmt.getRelImplementor(rel.getCluster().rexBuilder);
    }
}


// End FarragoDefaultPlanner.java
