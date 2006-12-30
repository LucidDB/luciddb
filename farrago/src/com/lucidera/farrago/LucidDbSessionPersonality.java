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

// TODO jvs 9-Apr-2006:  eliminate this once we stop depending on
// Fennel calc, or make it dynamic for GPL version only.

import com.disruptivetech.farrago.rel.*;

import com.lucidera.lcs.*;
import com.lucidera.opt.*;
import com.lucidera.runtime.*;
import com.lucidera.type.*;

import java.io.*;
import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.db.*;
import net.sf.farrago.defimpl.*;
import net.sf.farrago.fem.config.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.query.*;
import net.sf.farrago.session.*;
import net.sf.farrago.util.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.relopt.hep.*;
import org.eigenbase.reltype.*;
import org.eigenbase.resgen.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;


/**
 * Customizes Farrago session personality with LucidDB behavior.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LucidDbSessionPersonality
    extends FarragoDefaultSessionPersonality
{
    //~ Static fields ----------------------------------------------------------

    public static final String LOG_DIR = "logDir";
    public static final String[] LOG_DIR_DEFAULT = 
        { "log", "testlog", "trace" };
    public static final String ETL_PROCESS_ID = "etlProcessId";
    public static final String ETL_PROCESS_ID_DEFAULT = null;
    public static final String ETL_ACTION_ID = "etlActionId";
    public static final String ETL_ACTION_ID_DEFAULT = null;
    public static final String ERROR_MAX = "errorMax";
    public static final String ERROR_MAX_DEFAULT = "0";
    public static final String ERROR_LOG_MAX = "errorLogMax";
    public static final String ERROR_LOG_MAX_DEFAULT = null;
    public static final String LAST_UPSERT_ROWS_INSERTED =
        "lastUpsertRowsInserted";
    public static final String LAST_UPSERT_ROWS_INSERTED_DEFAULT = null;
    
    //~ Constructors -----------------------------------------------------------

    protected LucidDbSessionPersonality(FarragoDbSession session)
    {
        super(session);
        paramValidator.registerDirectoryParam(LOG_DIR, false);
        paramValidator.registerStringParam(ETL_PROCESS_ID, true);
        paramValidator.registerStringParam(ETL_ACTION_ID, true);
        paramValidator.registerIntParam(
            ERROR_MAX, true, 0, Integer.MAX_VALUE);
        paramValidator.registerIntParam(
            ERROR_LOG_MAX, true, 0, Integer.MAX_VALUE);
        paramValidator.registerLongParam(
            LAST_UPSERT_ROWS_INSERTED, true, 0, Long.MAX_VALUE); 
    }

    //~ Methods ----------------------------------------------------------------

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

        EigenbaseResource featureResource = EigenbaseResource.instance();

        // LucidDB doesn't yet support transactions.
        if (feature == featureResource.SQLFeature_E151) {
            return false;
        }

        // LucidDB doesn't support UPDATE
        if (feature == featureResource.SQLFeature_E101_03) {
            return false;
        }

        // but LucidDB does support MERGE (unlike vanilla Farrago)
        if (feature == featureResource.SQLFeature_F312) {
            return true;
        }

        // LucidDB doesn't support ORDER BY DESC...yet
        if (feature == featureResource.SQLConformance_OrderByDesc){
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
        chain.addProvider(new LoptMetadataProvider(database.getSystemRepos()));
    }

    private FarragoSessionPlanner newHepPlanner(
        final FarragoSessionPreparingStmt stmt)
    {
        final boolean fennelEnabled = stmt.getRepos().isFennelEnabled();
        final CalcVirtualMachine calcVM =
            stmt.getRepos().getCurrentConfig().getCalcVirtualMachine();

        Collection<RelOptRule> medPluginRules = new LinkedHashSet<RelOptRule>();

        HepProgram program =
            createHepProgram(
                fennelEnabled,
                calcVM,
                medPluginRules);
        FarragoSessionPlanner planner =
            new LucidDbPlanner(
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
        planner.addRule(new LoptOptimizeJoinRule());
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
        
        // Convert ProjectRels underneath an insert into RenameRels before
        // applying any merge projection rules.  Otherwise, we end up losing
        // column information used in error reporting during inserts.
        if (fennelEnabled) {
            builder.addRuleInstance(new FennelInsertRenameRule());
        }
        
        // Execute rules that are needed to do proper join optimization:
        // 1) Push down filters so they're closest to the RelNode they apply to.
        //    This also needs to be done before the pull project rules because
        //    filters need to be pushed into joins in order for the pull up
        //    project rules to properly determine whether projects can be pulled
        //    up.
        // 2) Pull up projects above joins to maximize the number of join
        //    factors.
        // 3) Push the projects back down so row scans are projected and also so
        //    we can determine which fields are projected above each join.
        // 4) Push down filters a second time to push filters past any projects
        //    that were pushed down.
        // 5) Convert the join inputs into MultiJoinRels and also pull projects
        //    back up, but only the ones above joins so we preserve projects
        //    on top of row scans but maximize the number of join factors.
        // 6) Optimize join ordering.
   
        // Push down filters
        applyPushDownFilterRules(builder);
        
        // Pull up projects
        builder.addGroupBegin();      
        builder.addRuleInstance(new RemoveTrivialProjectRule());
        builder.addRuleInstance(
            PullUpProjectsAboveJoinRule.instanceTwoProjectChildren);
        builder.addRuleInstance(
            PullUpProjectsAboveJoinRule.instanceLeftProjectChild);
        builder.addRuleInstance(
            PullUpProjectsAboveJoinRule.instanceRightProjectChild);
        // push filter past project to move the project up in the tree
        builder.addRuleInstance(new PushFilterPastProjectRule());
        // merge any projects we pull up
        builder.addRuleInstance(new MergeProjectRule(true));
        builder.addGroupEnd();
        
        // Push the projects back down
        applyPushDownProjectRules(builder);
        
        // Push filters down again after pulling and pushing projects
        applyPushDownFilterRules(builder);
        
        // Merge any projects that are now on top of one another as a result
        // of pushing filters.  This ensures that the subprogram below fires
        // the 3 rules described in lockstep fashion on only the nodes related
        // to joins.
        builder.addRuleInstance(new MergeProjectRule(true));

        // Convert 2-way joins to n-way joins.  Do the conversion bottom-up
        // so once a join is converted to a MultiJoinRel, you're ensured that
        // all of its children have been converted to MultiJoinRels.  At the
        // same time, pull up projects that are on top of MultiJoinRels so the
        // projects are above their parent joins.  Since we're pulling up
        // projects, we need to also merge any projects we generate as a
        // result of the pullup.
        //
        // These three rules are applied within a subprogram so they can be
        // applied one after the other in lockstep fashion.
        HepProgramBuilder subprogramBuilder = new HepProgramBuilder();
        subprogramBuilder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        subprogramBuilder.addMatchLimit(1);       
        subprogramBuilder.addRuleInstance(new ConvertMultiJoinRule());
        subprogramBuilder.addRuleInstance(
            PullUpProjectsOnTopOfMultiJoinRule.instanceTwoProjectChildren);
        subprogramBuilder.addRuleInstance(
            PullUpProjectsOnTopOfMultiJoinRule.instanceLeftProjectChild);
        subprogramBuilder.addRuleInstance(
            PullUpProjectsOnTopOfMultiJoinRule.instanceRightProjectChild);
        subprogramBuilder.addRuleInstance(new MergeProjectRule(true));
        builder.addSubprogram(subprogramBuilder.createProgram());
        
        // Push projection information in the remaining projections that sit
        // on top of MultiJoinRels into the MultiJoinRels.  These aren't
        // handled by PullUpProjectsOnTopOfMultiJoinRule because these
        // projects are not beneath joins.
        builder.addRuleInstance(new PushProjectIntoMultiJoinRule());
        
        // Optimize join order; this will spit back out all 2-way joins and
        // semijoins.  Note that the match order is bottom-up, so we
        // can optimize lower-level joins before their ancestors.  That allows
        // ancestors to have better cost info to work with (well, eventually).
        builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        builder.addRuleInstance(new LoptOptimizeJoinRule()); 
        builder.addMatchOrder(HepMatchOrder.ARBITRARY);

        // Convert filters to bitmap index searches and boolean operators.  We
        // didn't do this earlier to make join costing easier.
        builder.addRuleByDescription("LcsIndexAccessRule");

        // Push semijoins down to tables.  (The join part is a NOP for now,
        // but once we start taking more kinds of join factors, it won't be.)
        builder.addGroupBegin();
        builder.addRuleInstance(new PushSemiJoinPastFilterRule());
        builder.addRuleInstance(new PushSemiJoinPastProjectRule());
        builder.addRuleInstance(new PushSemiJoinPastJoinRule());
        builder.addGroupEnd();

        // Convert semijoins to physical index access.
        builder.addRuleClass(LcsIndexSemiJoinRule.class);

        // TODO zfong 10/27/06 - This rule is currently a no-op because we
        // won't generate a semijoin if it can't be converted to physical
        // RelNodes.  But it's currently left in place in case of bugs.
        // In the future, change it to a rule that converts the leftover
        // SemiJoinRel to a pattern that can be processed by LhxSemiJoinRule so
        // we instead use hash semijoins to process the semijoin rather than
        // removing the semijoin, which could result in an incorrect query
        // result.
        builder.addRuleInstance(new RemoveSemiJoinRule());
    
        // Now that we've finished join ordering optimization, have converted
        // filters where possible, and have converted semijoins, push projects
        // back down.
        applyPushDownProjectRules(builder);

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

        // Bitmap aggregation is favored
        builder.addRuleInstance(LcsIndexAggRule.instanceRenameRowScan);
        builder.addRuleInstance(LcsIndexAggRule.instanceRenameNormalizer);

        // Prefer hash aggregation over the standard Fennel aggregation.
        // Apply aggregation rules before the calc rules below so we can
        // call metadata queries on logical RelNodes.
        builder.addRuleInstance(new LhxAggRule());

        // Handle trivial renames now so that they don't get
        // implemented as calculators.
        if (fennelEnabled) {
            builder.addRuleInstance(new FennelRenameRule());
        }

        // Convert remaining filters and projects to logical calculators,
        // merging adjacent ones.
        builder.addGroupBegin();
        builder.addRuleInstance(FilterToCalcRule.instance);
        builder.addRuleInstance(ProjectToCalcRule.instance);
        builder.addRuleInstance(MergeCalcRule.instance);
        builder.addGroupEnd();
        
        // First, try to use ReshapeRel for calcs before firing the other
        // physical calc conversion rules.  Fire this rule before
        // ReduceDecimalsRule so we avoid decimal reinterprets that can
        // be handled by Reshape
        if (fennelEnabled) {
            builder.addRuleInstance(new FennelReshapeRule());
        }
        
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
        
        // If FennelCartesianJoinRule swapped its join inputs and added a
        // new CalcRel on top of the new cartesian join, we may need to
        // merge the calc with other calcs
        builder.addRuleInstance(MergeCalcRule.instance);

        if (calcVM.equals(CalcVirtualMachineEnum.CALCVM_FENNEL)) {
            // use Fennel for calculating expressions
            assert (fennelEnabled);
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

        // After calculator relations are resolved, decorate Java calc rels
        builder.addRuleInstance(LoptIterCalcRule.lcsAppendInstance);
        builder.addRuleInstance(LoptIterCalcRule.tableAccessInstance);
        builder.addRuleInstance(LoptIterCalcRule.lcsMergeInstance);
        builder.addRuleInstance(LoptIterCalcRule.lcsDeleteInstance);
        builder.addRuleInstance(LoptIterCalcRule.jdbcQueryInstance);
        builder.addRuleInstance(LoptIterCalcRule.javaUdxInstance);
        builder.addRuleInstance(LoptIterCalcRule.hashJoinInstance);
        builder.addRuleInstance(LoptIterCalcRule.defaultInstance);

        return builder.createProgram();
    }
    
    /**
     * Applies rules that push filters past various RelNodes.
     * 
     * @param builder HEP program builder
     */
    private void applyPushDownFilterRules(HepProgramBuilder builder)
    {
        builder.addGroupBegin();
        builder.addRuleInstance(new PushFilterPastSetOpRule());
        builder.addRuleInstance(new PushFilterPastProjectRule());
        builder.addRuleInstance(
            new PushFilterPastJoinRule(
                new RelOptRuleOperand(
                    FilterRel.class,
                    new RelOptRuleOperand[] {
                        new RelOptRuleOperand(JoinRel.class, null)
                    }),
                "with filter above join"));
        builder.addRuleInstance(
            new PushFilterPastJoinRule(
                new RelOptRuleOperand(JoinRel.class, null),
                "without filter above join"));      
        // merge filters
        builder.addRuleInstance(new MergeFilterRule());
        builder.addGroupEnd();     
    }
    
    /**
     * Applies rules that push projects past various RelNodes.
     * 
     * @param builder HEP program builder
     */
    private void applyPushDownProjectRules(HepProgramBuilder builder)
    {
        builder.addGroupBegin();
        builder.addRuleInstance(new RemoveTrivialProjectRule());
        builder.addRuleInstance(
            new PushProjectPastSetOpRule(
                LucidDbOperatorTable.ldbInstance().getSpecialOperators()));
        builder.addRuleInstance(
            new PushProjectPastJoinRule(
                LucidDbOperatorTable.ldbInstance().getSpecialOperators()));
        // Rules to push projects past filters.  There are two rule
        // patterns because the second is needed to handle the case where
        // the projection has been trivially removed but we still need to
        // pull special columns referenced in filters into a new projection.
        builder.addRuleInstance(
            new PushProjectPastFilterRule(
                new RelOptRuleOperand(
                    ProjectRel.class,
                    new RelOptRuleOperand[] {
                        new RelOptRuleOperand(FilterRel.class, null)
                    }),
                LucidDbOperatorTable.ldbInstance().getSpecialOperators(),
                "with project"));
        builder.addRuleInstance(
            new PushProjectPastFilterRule(
                new RelOptRuleOperand(FilterRel.class, null),
                LucidDbOperatorTable.ldbInstance().getSpecialOperators(),
                "without project"));
        builder.addRuleInstance(new MergeProjectRule(true));
        builder.addGroupEnd();
    }

    //~ Inner Classes ----------------------------------------------------------

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

    // override FarragoDefaultSessionPersonality
    public void loadDefaultSessionVariables(
        FarragoSessionVariables variables)
    {
        super.loadDefaultSessionVariables(variables);
        // try to pick a good default variable for log directory. this would 
        // not be used in practice, but could be helpful during development.
        String homeDirPath = FarragoProperties.instance().homeDir.get();
        File homeDir = new File(homeDirPath);
        assert (homeDir.exists() && homeDir.isDirectory());
        String logDirPath = homeDirPath;
        for (String subDirPath : LOG_DIR_DEFAULT) {
            File logDir = new File(homeDir, subDirPath);
            if (logDir.exists()) {
                logDirPath = logDir.getPath();
                break;
            }
        }
        variables.setDefault(LOG_DIR, logDirPath);
        variables.setDefault(ETL_PROCESS_ID, ETL_PROCESS_ID_DEFAULT);
        variables.setDefault(ETL_ACTION_ID, ETL_ACTION_ID_DEFAULT);
        variables.setDefault(ERROR_MAX, ERROR_MAX_DEFAULT);
        variables.setDefault(ERROR_LOG_MAX, ERROR_LOG_MAX_DEFAULT);
        variables.setDefault(
            LAST_UPSERT_ROWS_INSERTED,
            LAST_UPSERT_ROWS_INSERTED_DEFAULT);
    }

    // override FarragoDefaultSessionPersonality
    public FarragoSessionRuntimeContext newRuntimeContext(
        FarragoSessionRuntimeParams params)
    {
        return new LucidDbRuntimeContext(params);
    }
    
    // override FarragoDefaultSessionPersonality
    public FarragoSessionPreparingStmt newPreparingStmt(
        FarragoSessionStmtContext stmtContext,
        FarragoSessionStmtValidator stmtValidator)
    {
        String sql = (stmtContext == null) ? "?" : stmtContext.getSql();
        LucidDbPreparingStmt stmt =
            new LucidDbPreparingStmt(stmtValidator, sql);
        initPreparingStmt(stmt);
        return stmt;
    }

    // implement FarragoSessionPersonality
    public RelDataTypeFactory newTypeFactory(
        FarragoRepos repos)
    {
        return new LucidDbTypeFactory(repos);
    }
    
    // implement FarragoSessionPersonality
    public void updateRowCounts(
        FarragoSession session,
        List<String> tableName,
        long insertedRowCount,
        long deletedRowCount,
        long updateRowCount,
        TableModificationRel.Operation tableModOp)
    {
        FarragoSessionStmtValidator stmtValidator = session.newStmtValidator();
        FarragoRepos repos = session.getRepos();
        boolean rollback = false;
        try {
            repos.beginReposTxn(true);
            rollback = true;
        
            // get the current rowcounts
            assert(tableName.size() == 3);
            SqlIdentifier qualifiedName =
                new SqlIdentifier(
                    tableName.toArray(new String[tableName.size()]),
                    new SqlParserPos(0,0));
            FemAbstractColumnSet columnSet =
                stmtValidator.findSchemaObject(
                    qualifiedName,
                    FemAbstractColumnSet.class);       
            long currRowCount = columnSet.getRowCount();
            long currDeletedRowCount = columnSet.getDeletedRowCount();
               
            // update the rowcounts based on the operation
            if (tableModOp == TableModificationRel.Operation.INSERT) {
                currRowCount += insertedRowCount;
            } else if (tableModOp == TableModificationRel.Operation.DELETE) {
                currRowCount -= deletedRowCount;
                currDeletedRowCount += deletedRowCount;
            } else if (tableModOp == TableModificationRel.Operation.MERGE) {
                long newRowCount = insertedRowCount - deletedRowCount;
                currRowCount += newRowCount;
                currDeletedRowCount += deletedRowCount;
                session.getSessionVariables().setLong(
                    LAST_UPSERT_ROWS_INSERTED,
                    newRowCount);
            } else {
                assert(false);
            }
 
            // update the catalog; don't let the rowcount go below zero; it
            // may go below zero if a crash occurred in the middle of a prior
            // update
            if (currRowCount < 0) {
                currRowCount = 0;
            }
            columnSet.setRowCount(currRowCount);
            assert(currDeletedRowCount >= 0);
            columnSet.setDeletedRowCount(currDeletedRowCount);
            
            rollback = false;
            repos.endReposTxn(false);
        } finally {
            if (rollback) {
                repos.endReposTxn(true);
            }
            stmtValidator.closeAllocation();
        }
    }
    
    // implement FarragoSessionPersonality
    public void resetRowCounts(FemAbstractColumnSet table)
    {
        FarragoCatalogUtil.resetRowCounts(table);
    }
}

// End LucidDbSessionPersonality.java
