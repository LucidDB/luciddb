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

import net.sf.saffron.util.*;
import net.sf.saffron.oj.*;
import net.sf.saffron.oj.rel.*;
import net.sf.saffron.oj.convert.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.rel.convert.*;
import net.sf.farrago.fem.config.CalcVirtualMachine;
import net.sf.farrago.fem.config.CalcVirtualMachineEnum;

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

        addRule(new OJPlannerFactory.HomogeneousUnionToIteratorRule());
        addRule(new OJPlannerFactory.OneRowToIteratorRule());

        addRule(new FennelSortRule());
        addRule(new FennelDistinctSortRule());
        addRule(
            new FennelRenameRule(
                FennelPullRel.FENNEL_PULL_CONVENTION,
                "FennelPullRenameRule"));
        addRule(new FennelCartesianJoinRule());

        // Add the rule to introduce FennelCalcRel's only if the fennel
        // calculator is enabled.
        final CalcVirtualMachine calcVM =
                stmt.getCatalog().getCurrentConfig().getCalcVirtualMachine();
        if (calcVM.equals(CalcVirtualMachineEnum.CALCVM_FENNEL)) {
            // use only Fennel for calculating expressions
            addRule(FennelCalcRule.instance);
        } else if (calcVM.equals(CalcVirtualMachineEnum.CALCVM_JAVA)) {
            // use only Java code generation for calculating expressions
            addRule(OJPlannerFactory.IterCalcRule.instance);
            
            // TODO jvs 6-May-2004:  these should be redundant now, but when
            // I remove them, some queries fail.  Find out why.
            addRule(OJPlannerFactory.ProjectToIteratorRule.instance);
            addRule(OJPlannerFactory.ProjectedFilterToIteratorRule.instance);
        } else {
            assert(calcVM.equals(CalcVirtualMachineEnum.CALCVM_AUTO));
            throw Util.needToImplement(calcVM);
        }

        FennelToIteratorConverter.register(this);
        IteratorToFennelConverter.register(this, this.stmt);
    }

    /**
     * @return the FarragoPreparingStmt on whose behalf planning
     * is being performed
     */
    public FarragoPreparingStmt getPreparingStmt()
    {
        return stmt;
    }
}


// End FarragoPlanner.java
