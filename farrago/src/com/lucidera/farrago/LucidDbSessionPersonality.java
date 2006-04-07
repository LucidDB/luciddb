/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
package com.lucidera.farrago;

import com.lucidera.lcs.*;
import com.lucidera.opt.*;

import net.sf.farrago.session.*;
import net.sf.farrago.db.*;
import net.sf.farrago.defimpl.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.resgen.*;
import org.eigenbase.resource.*;

/**
 * Customizes Farrago session personality with LucidDB behavior.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LucidDbSessionPersonality extends FarragoDefaultSessionPersonality
{
    protected LucidDbSessionPersonality(FarragoDbSession session)
    {
        super(session);
    }

    // implement FarragoSessionPersonality
    public String getDefaultLocalDataServerName(
        FarragoSessionStmtValidator stmtValidator)
    {
        return "SYS_COLUMN_STORE_DATA_SERVER";
    }

    public boolean supportsFeature(ResourceDefinition feature)
    {
        // TODO jvs 20-Nov-2005: better infrastructure once there
        // are enough feature overrides to justify it

        // LucidDB doesn't yet support transactions.
        if (feature == EigenbaseResource.instance().SQLFeature_E151) {
            return false;
        }
        
        // LucidDB doesn't yet support EXCEPT.
        if (feature == EigenbaseResource.instance().SQLFeature_E071_03) {
            return false;
        }
        
        // LucidDB doesn't yet support INTERSECT.
        if (feature == EigenbaseResource.instance().SQLFeature_F302) {
            return false;
        }
        
        return super.supportsFeature(feature);
    }
    
    public FarragoSessionPlanner newPlanner(
        FarragoSessionPreparingStmt stmt,
        boolean init)
    {
        FarragoSessionPlanner planner = super.newPlanner(stmt, init);
        planner.addRule(new PushSemiJoinPastFilterRule());
        planner.addRule(new OptimizeJoinRule());
        
        addConvertMultiJoinRules(planner);
 
        planner.removeRule(SwapJoinRule.instance);
        return planner;
    }
    
    /**
     * Adds the different permutations of patterns that trigger 
     * ConvertMultiJoinRule.  We enumerate over the patterns rather than
     * matching on arbitrary RelNodes to speed up Volcano pattern matching.
     * 
     * @param planner planner that rules will be added to
     */
    private void addConvertMultiJoinRules(FarragoSessionPlanner planner)
    {
        planner.addRule(new ConvertMultiJoinRule(
            new RelOptRuleOperand(
                JoinRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(MultiJoinRel.class, null),
                    new RelOptRuleOperand(MultiJoinRel.class, null)
                    }), "MJ, MJ"));
        planner.addRule(new ConvertMultiJoinRule(
            new RelOptRuleOperand(
                JoinRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(MultiJoinRel.class, null),
                    new RelOptRuleOperand(LcsRowScanRel.class, null)
                    }), "MJ, RS"));
        planner.addRule(new ConvertMultiJoinRule(
            new RelOptRuleOperand(
                JoinRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(MultiJoinRel.class, null),
                    new RelOptRuleOperand(FilterRel.class,
                        new RelOptRuleOperand [] {
                            new RelOptRuleOperand(LcsRowScanRel.class, null)
                    })}), "MJ, FRS"));
        planner.addRule(new ConvertMultiJoinRule(
            new RelOptRuleOperand(
                JoinRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(LcsRowScanRel.class, null),
                    new RelOptRuleOperand(MultiJoinRel.class, null)
                    }), "RS, MJ"));
        planner.addRule(new ConvertMultiJoinRule(
            new RelOptRuleOperand(
                JoinRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(LcsRowScanRel.class, null),
                    new RelOptRuleOperand(LcsRowScanRel.class, null)
                    }), "RS, RS"));
        planner.addRule(new ConvertMultiJoinRule(
            new RelOptRuleOperand(
                JoinRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(LcsRowScanRel.class, null),
                    new RelOptRuleOperand(FilterRel.class,
                        new RelOptRuleOperand [] {
                            new RelOptRuleOperand(LcsRowScanRel.class, null)
                    })}), "RS, FRS"));
        planner.addRule(new ConvertMultiJoinRule(
            new RelOptRuleOperand(
                JoinRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(FilterRel.class,
                        new RelOptRuleOperand [] {
                            new RelOptRuleOperand(LcsRowScanRel.class, null)}),
                    new RelOptRuleOperand(MultiJoinRel.class, null)
                    }), "FRS, MJ"));
        planner.addRule(new ConvertMultiJoinRule(
            new RelOptRuleOperand(
                JoinRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(FilterRel.class,
                        new RelOptRuleOperand [] {
                            new RelOptRuleOperand(LcsRowScanRel.class, null)}),
                    new RelOptRuleOperand(LcsRowScanRel.class, null)
                    }), "FRS, RS"));
        planner.addRule(new ConvertMultiJoinRule(
            new RelOptRuleOperand(
                JoinRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(FilterRel.class,
                        new RelOptRuleOperand [] {
                            new RelOptRuleOperand(LcsRowScanRel.class, null)}),
                    new RelOptRuleOperand(FilterRel.class,
                        new RelOptRuleOperand [] {
                            new RelOptRuleOperand(LcsRowScanRel.class, null)})
                }), "FRS, FRS"));
    }
}

// End LucidDbSessionPersonality.java
