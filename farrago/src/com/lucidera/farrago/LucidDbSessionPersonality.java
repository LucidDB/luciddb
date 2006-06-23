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

// TODO jvs 9-Apr-2006:  eliminate this once we stop depending on
// Fennel calc, or make it dynamic for GPL version only.
import com.disruptivetech.farrago.rel.*;

import net.sf.farrago.session.*;
import net.sf.farrago.db.*;
import net.sf.farrago.defimpl.*;
import net.sf.farrago.query.*;

import net.sf.farrago.fem.config.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.relopt.hep.*;
import org.eigenbase.oj.rel.*;
import org.eigenbase.resgen.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;

import java.util.*;

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
    
    // implement FarragoSessionPersonality
    public SqlOperatorTable getSqlOperatorTable(
        FarragoSessionPreparingStmt preparingStmt)
    {
        return LucidDbOperatorTable.ldbInstance();
    }

    public boolean supportsFeature(ResourceDefinition feature)
    {
        // TODO jvs 20-Nov-2005: better infrastructure once there
        // are enough feature overrides to justify it

        // LucidDB doesn't yet support transactions.
        if (feature == EigenbaseResource.instance().SQLFeature_E151) {
            return false;
        }
                
        return super.supportsFeature(feature);
    }

    // implement FarragoSessionPersonality
    public FarragoSessionPlanner newPlanner(
        FarragoSessionPreparingStmt stmt,
        boolean init)
    {
        return newHepPlanner(stmt);
    }

    // implement FarragoSessionPersonality
    public void registerRelMetadataProviders(ChainedRelMetadataProvider chain)
    {
        chain.addProvider(
            new LoptMetadataProvider(database.getSystemRepos()));
    }

    private FarragoSessionPlanner newHepPlanner(
        final FarragoSessionPreparingStmt stmt)
    {
        final boolean fennelEnabled = stmt.getRepos().isFennelEnabled();
        final CalcVirtualMachine calcVM =
            stmt.getRepos().getCurrentConfig().getCalcVirtualMachine();

        Collection<RelOptRule> medPluginRules = new LinkedHashSet<RelOptRule>();
        
        HepProgram program = createHepProgram(
            fennelEnabled,
            calcVM,
            medPluginRules);
        FarragoSessionPlanner planner = new LucidDbPlanner(
            program,
            stmt,
            medPluginRules);

        // TODO jvs 9-Apr-2006: Get rid of !fennelEnabled configuration
        // altogether once there are packaged Windows binary builds available.

        planner.addRelTraitDef(CallingConventionTraitDef.instance);
        RelOptUtil.registerAbstractRels(planner);

        // TODO jvs 9-Apr-2006: Need to break this up, since we don't want to
        // be depending on rules from non-LucidEra yellow zones.
        FarragoDefaultPlanner.addStandardRules(
            planner,
            fennelEnabled,
            calcVM);
        
        planner.addRule(new PushSemiJoinPastFilterRule());
        planner.addRule(new PushSemiJoinPastJoinRule());
        planner.addRule(new ConvertMultiJoinRule());
        planner.addRule(
            new LoptOptimizeJoinRule(
                new RelOptRuleOperand(
                    ProjectRel.class,
                    new RelOptRuleOperand [] {
                        new RelOptRuleOperand(MultiJoinRel.class, null)
                }),
                "with project"));
        planner.addRule(
            new LoptOptimizeJoinRule(
                new RelOptRuleOperand(MultiJoinRel.class, null),
                "without project"));
        planner.addRule(new CoerceInputsRule(LcsTableMergeRel.class, false));

        planner.removeRule(SwapJoinRule.instance);
        return planner;
    }

    private HepProgram createHepProgram(
        boolean fennelEnabled,
        CalcVirtualMachine calcVM,
        Collection<RelOptRule> medPluginRules)
    {
        HepProgramBuilder builder = new HepProgramBuilder();

        // The very first step is to implement index joins on catalog
        // tables.  The reason we do this here is so that we don't
        // disturb the carefully hand-coded joins in the catalog views.
        // TODO:  loosen up once we make sure OptimizeJoinRule does
        // as well or better than the hand-coding.
        builder.addRuleByDescription("MedMdrJoinRule");

        // Eliminate AGG(DISTINCT x) now, because this transformation
        // may introduce new joins which need to be optimized further on.
        builder.addRuleInstance(RemoveDistinctAggregateRule.instance);
        
        // Eliminate reducible constant expression.  TODO jvs 26-May-2006: do
        // this again later wherever more such expressions may be reintroduced.
        builder.addRuleClass(FarragoReduceExpressionsRule.class);
        
        // Now, pull join conditions out of joins, leaving behind Cartesian
        // products.  Why?  Because PushFilterRule doesn't start from
        // join conditions, only filters.  It will push them right back
        // into and possibly through the join.
        builder.addRuleInstance(ExtractJoinFilterRule.instance);
            
        // Need to fire delete and merge rules before any projection rules
        // since they modify the projection
        builder.addRuleInstance(new LcsTableDeleteRule());
        builder.addRuleInstance(new LcsTableMergeRule());

        // Remove trivial projects so tables referenced in selects in the
        // from clause can be optimized with the rest of the query
        builder.addRuleInstance(new RemoveTrivialProjectRule());

        // Push filters down.
        builder.addRuleInstance(new PushFilterRule());

        // Convert 2-way joins to n-way joins.  Do the conversion bottom-up
        // so once a join is converted to a MultiJoinRel, you're ensured that
        // all of its children have been converted to MultiJoinRels
        builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        builder.addRuleInstance(new ConvertMultiJoinRule());
        
        // Optimize join order; this will spit back out all 2-way joins and
        // semijoins.  Note that the match order is still bottom-up, so we
        // can optimize lower-level joins before their ancestors.  That allows
        // ancestors to have better cost info to work with (well, eventually).
        // First try to apply the rule matching against a projection, so we
        // can preserve the original projection.  Then, try matching without
        // the projection to handle the cases where the projection has been
        // trivially removed.  We want to try and preserve the original
        // projection so we can push projections past joins.
        builder.addRuleInstance(
            new LoptOptimizeJoinRule(
                new RelOptRuleOperand(
                    ProjectRel.class,
                    new RelOptRuleOperand [] {
                        new RelOptRuleOperand(MultiJoinRel.class, null)
                }),
                "with project"));
        builder.addRuleInstance(
            new LoptOptimizeJoinRule(
                new RelOptRuleOperand(MultiJoinRel.class, null),
                "without project"));
        builder.addMatchOrder(HepMatchOrder.ARBITRARY);
        
        // Convert filters to bitmap index searches and boolean operators.  We
        // didn't do this earlier to make join costing easier.
        builder.addRuleByDescription("LcsIndexAccessRule");
        
        // Push semijoins down to tables.  (The join part is a NOP for now,
        // but once we start taking more kinds of join factors, it won't be.)
        builder.addGroupBegin();
        builder.addRuleInstance(new PushSemiJoinPastFilterRule());
        builder.addRuleInstance(new PushSemiJoinPastJoinRule());
        builder.addGroupEnd();

        // Convert semijoins to physical index access.
        builder.addRuleClass(LcsIndexSemiJoinRule.class);
 
        // Remove any semijoins that couldn't be converted
        builder.addRuleInstance(new RemoveSemiJoinRule());

        // Apply PushProjectPastJoinRule while there are no physical joinrels
        // since the rule only matches on JoinRel.
        builder.addRuleInstance(new RemoveTrivialProjectRule());
        builder.addRuleInstance(new PushProjectPastJoinRule(
            LucidDbOperatorTable.ldbInstance().getSpecialOperators()));
        
        // Push project past filter so that we can reduce the number
        // of clustered indexes accessed by row scan.  There are two rule
        // patterns because the second is needed to handle the case where
        // the projection has been trivially removed but we still need to
        // pull special columns referenced in filters into a new projection
        builder.addRuleInstance(new RemoveTrivialProjectRule());
        builder.addRuleInstance(new PushProjectPastFilterRule(
            new RelOptRuleOperand(
                ProjectRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(FilterRel.class, null)
                }),
            LucidDbOperatorTable.ldbInstance().getSpecialOperators(),
            "with project"));
        builder.addRuleInstance(new PushProjectPastFilterRule(
            new RelOptRuleOperand(FilterRel.class, null),
            LucidDbOperatorTable.ldbInstance().getSpecialOperators(),
            "without project"));
        builder.addRuleInstance(new RemoveTrivialProjectRule());

        // Apply physical projection to row scans, eliminating access
        // to clustered indexes we don't need.
        builder.addRuleInstance(new LcsTableProjectionRule());

        // Eliminate UNION DISTINCT and trivial UNION.
        // REVIEW:  some of this may need to happen earlier as well.
        builder.addRuleInstance(new UnionToDistinctRule());
        builder.addRuleInstance(new UnionEliminatorRule());

        // Eliminate redundant SELECT DISTINCT.
        builder.addRuleInstance(new RemoveDistinctRule());

        // We're getting close to physical implementation.  First, insert
        // type coercions for expressions which require it.
        builder.addRuleClass(CoerceInputsRule.class);

        // Run any SQL/MED plugin rules.  For FTRS, this includes index joins.
        // LCS rules are included here too; the ones we've already executed
        // explicitly should be nops now, but some, like LcsTableAppendRule and
        // LcsIndexBuilderRule, we actually need to run.  (Note that
        // LcsTableAppendRule relies on CoerceInputsRule above.)
        builder.addRuleCollection(medPluginRules);

        // Use hash semi join if possible.
        builder.addRuleInstance(new LhxSemiJoinRule());
        
        // Use hash join wherever possible.
        builder.addRuleInstance(new LhxJoinRule());
        
        // Use hash join to implement set op: Intersect.
        builder.addRuleInstance(new LhxIntersectRule());        

        // Use hash join to implement set op: Except(minus).
        builder.addRuleInstance(new LhxMinusRule());        
        
        // Extract join conditions again so that FennelCartesianJoinRule can do
        // its job.  Need to do this before converting filters to calcs, but
        // after other join strategies such as hash join have been attempted,
        // because they rely on the join condition being part of the join.
        builder.addRuleInstance(ExtractJoinFilterRule.instance);

        // Replace AVG with SUM/COUNT (need to do this BEFORE calc conversion
        // and decimal reduction).
        builder.addRuleInstance(ReduceAggregatesRule.instance);
        
        // Use hash aggregation wherever possible.
        builder.addRuleInstance(new LhxAggRule());

        // Convert remaining filters and projects to logical calculators,
        // merging adjacent ones.
        builder.addGroupBegin();
        builder.addRuleInstance(FilterToCalcRule.instance);
        builder.addRuleInstance(ProjectToCalcRule.instance);
        builder.addRuleInstance(MergeCalcRule.instance);
        builder.addGroupEnd();

        // Replace the DECIMAL datatype with primitive ints.
        builder.addRuleInstance(new ReduceDecimalsRule());

        // The rest of these are all physical implementation rules
        // which are safe to apply simultaneously.
        builder.addGroupBegin();

        // Implement calls to UDX's.
        builder.addRuleInstance(FarragoJavaUdxRule.instance);

        if (fennelEnabled) {
            builder.addRuleInstance(new FennelSortRule());
            builder.addRuleInstance(new FennelRenameRule());
            builder.addRuleInstance(new FennelCartesianJoinRule());
            builder.addRuleInstance(new FennelAggRule());
            builder.addRuleInstance(new FennelValuesRule());
            // Requires CoerceInputsRule.
            builder.addRuleInstance(FennelUnionRule.instance);
        } else {
            builder.addRuleInstance(
                new IterRules.HomogeneousUnionToIteratorRule());
        }

        if (calcVM.equals(CalcVirtualMachineEnum.CALCVM_FENNEL)) {
            // use Fennel for calculating expressions
            assert(fennelEnabled);
            builder.addRuleInstance(FennelCalcRule.instance);
            builder.addRuleInstance(new FennelOneRowRule());
        } else if (calcVM.equals(CalcVirtualMachineEnum.CALCVM_JAVA)) {
            // use Java code generation for calculating expressions
            builder.addRuleInstance(IterRules.IterCalcRule.instance);
            builder.addRuleInstance(new IterRules.OneRowToIteratorRule());
        }

        // Finish main physical implementation group.
        builder.addGroupEnd();

        // If automatic calculator selection is enabled (the default),
        // figure out what to do with CalcRels.
        if (calcVM.equals(CalcVirtualMachineEnum.CALCVM_AUTO)) {
            // First, attempt to choose calculators such that converters are
            // minimized
            builder.addConverters(false);
            // Split remaining expressions into Fennel part and Java part
            builder.addRuleInstance(FarragoAutoCalcRule.instance);
            // Convert expressions, giving preference to Java
            builder.addRuleInstance(new IterRules.OneRowToIteratorRule());
            builder.addRuleInstance(IterRules.IterCalcRule.instance);
            builder.addRuleInstance(FennelCalcRule.instance);
        }
        
        // Finally, add generic converters as necessary.
        builder.addConverters(true);
        
        return builder.createProgram();
    }

    // TODO jvs 9-Apr-2006:  Move this to com.lucidera.opt once
    // naming convention has been decided there.
    private static class LucidDbPlanner
        extends HepPlanner
        implements FarragoSessionPlanner
    {
        private final FarragoSessionPreparingStmt stmt;
        private final Collection<RelOptRule> medPluginRules;
        private boolean inPluginRegistration;
        
        LucidDbPlanner(
            HepProgram program,
            FarragoSessionPreparingStmt stmt,
            Collection<RelOptRule> medPluginRules)
        {
            super(program);
            this.stmt = stmt;
            this.medPluginRules = medPluginRules;
        }

        // implement FarragoSessionPlanner
        public FarragoSessionPreparingStmt getPreparingStmt()
        {
            return stmt;
        }

        // implement FarragoSessionPlanner
        public void beginMedPluginRegistration(String serverClassName)
        {
            inPluginRegistration = true;
        }

        // implement FarragoSessionPlanner
        public void endMedPluginRegistration()
        {
            inPluginRegistration = false;
        }
        
        // implement RelOptPlanner
        public JavaRelImplementor getJavaRelImplementor(RelNode rel)
        {
            return stmt.getRelImplementor(
                rel.getCluster().getRexBuilder());
        }
        
        // implement RelOptPlanner
        public boolean addRule(RelOptRule rule)
        {
            if (inPluginRegistration) {
                medPluginRules.add(rule);
            }
            return super.addRule(rule);
        }
    }
}

// End LucidDbSessionPersonality.java
