/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2003-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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

import net.sf.farrago.fem.config.*;
import net.sf.farrago.query.*;
import net.sf.farrago.session.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;

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

        addRule(new AbstractConverter.ExpandConversionRule());
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
        planner.addRule(new FarragoMultisetSplitterRule());
        if (fennelEnabled) {
            planner.addRule(new FennelCollectRule());
            planner.addRule(new FennelUncollectRule());
            planner.addRule(new FennelCorrelatorRule());
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
