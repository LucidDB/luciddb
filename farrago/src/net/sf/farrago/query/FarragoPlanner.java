/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/
package net.sf.farrago.query;

import com.disruptivetech.farrago.rel.*;

// FIXME jvs 25-Aug-2004:  This is just a temporary circular dependency
// until the required session plugin infrastructure is available.
import com.disruptivetech.farrago.volcano.*;

import net.sf.farrago.fem.config.CalcVirtualMachine;
import net.sf.farrago.fem.config.CalcVirtualMachineEnum;

import org.eigenbase.oj.*;
import org.eigenbase.oj.rel.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.relopt.*;
import org.eigenbase.util.*;


/**
 * FarragoPlanner extends {@link VolcanoPlanner} to request Farrago-specific
 * optimizations.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoPlanner extends VolcanoPlanner
{
    //~ Instance fields -------------------------------------------------------

    private FarragoPreparingStmt stmt;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoPlanner object.
     *
     * @param stmt statement on whose behalf this planner operates
     */
    public FarragoPlanner(FarragoPreparingStmt stmt)
    {
        this.stmt = stmt;

        // Yon Cassius has a lean and hungry look.
        ambitious = true;
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Initialize Farrago-specific rules for this planner.
     */
    public void init()
    {
        boolean fennelEnabled = stmt.getRepos().isFennelEnabled();

        // Only register calling conventions we're interested in.  Eventually
        // we probably want to expand this set once the various converters are
        // accurately costed.  For now, this guarantees determinism in unit
        // tests.
        addCallingConvention(CallingConvention.NONE);
        addCallingConvention(CallingConvention.ITERATOR);
        addCallingConvention(CallingConvention.RESULT_SET);
        addCallingConvention(FennelPullRel.FENNEL_PULL_CONVENTION);

        // NOTE: don't call IterConverterRel.init and friends; their presence
        // just confuses the optimizer, and we explicitly supply all the
        // conversion rules we need
        registerAbstractRels();

        addRule(new AbstractConverter.ExpandConversionRule());
        addRule(new RemoveDistinctRule());
        addRule(new UnionToDistinctRule());
        addRule(new CoerceInputsRule(UnionRel.class));
        addRule(new CoerceInputsRule(TableModificationRel.class));
        addRule(new SwapJoinRule());

        addRule(new IterRules.HomogeneousUnionToIteratorRule());
        addRule(new IterRules.OneRowToIteratorRule());

        if (fennelEnabled) {
            addRule(new FennelSortRule());
            addRule(new FennelDistinctSortRule());
            addRule(
                new FennelRenameRule(FennelPullRel.FENNEL_PULL_CONVENTION,
                    "FennelPullRenameRule"));
            addRule(new FennelCartesianJoinRule());
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
            IteratorToFennelConverter.register(this, this.stmt);
        }
    }

    /**
     * @return the FarragoPreparingStmt on whose behalf planning
     * is being performed
     */
    public FarragoPreparingStmt getPreparingStmt()
    {
        return stmt;
    }

    // override VolcanoPlanner
    public JavaRelImplementor getJavaRelImplementor(RelNode rel)
    {
        return stmt.getRelImplementor(rel.getCluster().rexBuilder);
    }
}


// End FarragoPlanner.java
