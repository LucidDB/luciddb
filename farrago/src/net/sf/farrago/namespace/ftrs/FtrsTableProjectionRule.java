/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
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
package net.sf.farrago.namespace.ftrs;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fennel.rel.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.PushProjector;
import org.eigenbase.relopt.*;
import org.eigenbase.rel.rules.PushProjector;

/**
 * FtrsTableProjectionRule implements the rule for pushing a Projection into a
 * FtrsIndexScanRel.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FtrsTableProjectionRule
    extends MedAbstractFennelProjectionRule
{
    public static final FtrsTableProjectionRule instance =
        new FtrsTableProjectionRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FtrsTableProjectionRule.
     */
    private FtrsTableProjectionRule()
    {
        super(
            new RelOptRuleOperand(
                ProjectRel.class,
                new RelOptRuleOperand(FtrsIndexScanRel.class, ANY)));
    }

    //~ Methods ----------------------------------------------------------------
    protected boolean equalTraitSets(RelTraitSet rts1, RelTraitSet rts2)
    {
        if (rts1.size() != rts2.size()) {
            return false;
        }
        for (int i = 0; i < rts1.size(); i++) {
            RelTrait rt1 = rts1.getTrait(i);
            RelTrait rt2 = rts2.getTrait(rt1.getTraitDef());
            if (rt1 != rt2) {
                return false;
            }
        }
        return true;
    }

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelRel.FENNEL_EXEC_CONVENTION;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        ProjectRel origProject = (ProjectRel) call.rels[0];
        if (!origProject.isBoxed()) {
            return;
        }

        FtrsIndexScanRel origScan = (FtrsIndexScanRel) call.rels[1];
        if (origScan.projectedColumns != null) {
            return;
        }

        // determine which columns can be projected from the scan, pulling
        // out references from expressions, if necessary
        List<Integer> projectedColumnList = new ArrayList<Integer>();
        List<ProjectRel> newProjList = new ArrayList<ProjectRel>();
        boolean needRename =
            createProjectionList(
                origScan,
                origProject,
                projectedColumnList,
                PushProjector.ExprCondition.FALSE,
                null,
                newProjList);

        // empty list indicates that nothing can be projected
        if (projectedColumnList.size() == 0) {
            return;
        }
        ProjectRel newProject;
        if (newProjList.isEmpty()) {
            newProject = null;
        } else {
            newProject = newProjList.get(0);
        }

        // Generate a potential scan for each available index covering the
        // desired projection.  Leave it up to the optimizer to select one
        // based on cost, since sort order and I/O may be in competition.
        final FarragoRepos repos = FennelRelUtil.getRepos(origScan);

        Integer [] projectedColumns =
            projectedColumnList.toArray(
                new Integer[projectedColumnList.size()]);

        // Make sure indexes are considered in a deterministic order, since
        // they aren't returned in one from the repository.  Also, causes us
        // to examine simpler (fewer covered columns) indexes first.
        TreeSet<FemLocalIndex> indexes =
            new TreeSet<FemLocalIndex>(new IndexLengthComparator());
        indexes.addAll(
            FarragoCatalogUtil.getTableIndexes(
                repos,
                origScan.ftrsTable.getCwmColumnSet()));
        for (FemLocalIndex index : indexes) {
            if (origScan.isOrderPreserving && !index.equals(origScan.index)) {
                // can't switch indexes if original scan order needs to be
                // preserved
                continue;
            }

            if (!testIndexCoverage(
                    origScan.ftrsTable.getIndexGuide(),
                    index,
                    projectedColumns))
            {
                continue;
            }

            // REVIEW:  should cluster be from origProject or origScan?
            RelNode projectedScan =
                new FtrsIndexScanRel(
                    origProject.getCluster(),
                    origScan.ftrsTable,
                    index,
                    origScan.getConnection(),
                    projectedColumns,
                    origScan.isOrderPreserving);

            // copy over other traits
            for (int i = 0; i < origScan.getTraits().size(); i++) {
                RelTrait trait = origScan.getTraits().getTrait(i);
                if (trait.getTraitDef()
                    != CallingConventionTraitDef.instance)
                {
                    if (projectedScan.getTraits().getTrait(trait.getTraitDef())
                        != null)
                    {
                        projectedScan.getTraits().setTrait(
                            trait.getTraitDef(),
                            trait);
                    } else {
                        projectedScan.getTraits().addTrait(trait);
                    }
                }
            }

            // create new RelNodes to replace the existing ones, either
            // removing or replacing the ProjectRel and recreating the row scan
            // to read only projected columns
            RelNode modRelNode =
                createNewRelNode(
                    projectedScan,
                    origProject,
                    needRename,
                    newProject);

            // change traits, just in case there are differences between
            // the non CC traits of origProject and origScan
            if (modRelNode != projectedScan) {
                // copy over non CC traits if necessary
                for (int i = 0; i < projectedScan.getTraits().size(); i++) {
                    RelTrait trait = projectedScan.getTraits().getTrait(i);
                    if (trait.getTraitDef()
                        != CallingConventionTraitDef.instance
                        && null == modRelNode.getTraits().getTrait(
                            trait.getTraitDef()))
                    {
                        modRelNode.getTraits().addTrait(trait);
                    }
                }
                // we only want to change traits if the CCs match and
                // the other traits do not, otherwise we cause an
                // AbstractConverter to be created which causes problems
                // because the subsets will be merged by the transformTo
                // call at the end of this method.
                if (!equalTraitSets(
                    origProject.getTraits(),
                    modRelNode.getTraits())
                    && !projectedScan.getTraits().equals(
                        modRelNode.getTraits()))
                {
                    modRelNode =
                        call.getPlanner().changeTraits(
                            modRelNode,
                            projectedScan.getTraits());
                }
            }
            call.transformTo(modRelNode);
        }
    }

    private boolean testIndexCoverage(
        FtrsIndexGuide indexGuide,
        FemLocalIndex index,
        Integer [] projection)
    {
        if (index.isInvalid()) {
            return false;
        }
        if (index.isClustered()) {
            // clustered index guarantees coverage
            return true;
        }
        Integer [] indexProjection =
            indexGuide.getUnclusteredCoverageArray(index);
        return Arrays.asList(indexProjection).containsAll(
            Arrays.asList(projection));
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class IndexLengthComparator
        implements Comparator<FemLocalIndex>
    {
        public int compare(FemLocalIndex o1, FemLocalIndex o2)
        {
            int c =
                o1.getIndexedFeature().size() - o2.getIndexedFeature().size();
            if (c != 0) {
                return c;
            }

            return o1.getStorageId().compareTo(o2.getStorageId());
        }
    }
}

// End FtrsTableProjectionRule.java
