/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
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
package net.sf.farrago.namespace.ftrs;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.query.*;
import net.sf.farrago.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexNode;
import org.eigenbase.util.*;


/**
 * FtrsTableProjectionRule implements the rule for pushing a Projection into
 * a FtrsIndexScanRel.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FtrsTableProjectionRule extends RelOptRule
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FtrsTableProjectionRule object.
     */
    public FtrsTableProjectionRule()
    {
        super(new RelOptRuleOperand(
                ProjectRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(FtrsIndexScanRel.class, null)
                }));
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelPullRel.FENNEL_PULL_CONVENTION;
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
            // TODO:  fold existing projection?
            return;
        }

        // REVIEW:  what about AnonFields?
        // TODO:  rather than failing, split into parts that can be
        // pushed down and parts that can't
        int n = origProject.getChildExps().length;
        Integer [] projectedColumns = new Integer[n];
        RelDataType rowType = origScan.getRowType();
        RelDataType projType = origProject.getRowType();
        RelDataTypeField [] projFields = projType.getFields();
        String [] fieldNames = new String[n];
        boolean needRename = false;
        for (int i = 0; i < n; ++i) {
            RexNode exp = origProject.getChildExps()[i];
            if (!(exp instanceof RexInputRef)) {
                return;
            }
            RexInputRef fieldAccess = (RexInputRef) exp;
            String projFieldName = projFields[i].getName();
            fieldNames[i] = projFieldName;
            String origFieldName =
                rowType.getFields()[fieldAccess.index].getName();
            if (!projFieldName.equals(origFieldName)) {
                needRename = true;
            }
            projectedColumns[i] = new Integer(fieldAccess.index);
        }

        // Generate a potential scan for each available index covering the
        // desired projection.  Leave it up to the optimizer to select one
        // based on cost, since sort order and I/O may be in competition.
        final FarragoRepos repos = FennelRelUtil.getRepos(origScan);

        Iterator iter = FarragoCatalogUtil.getTableIndexes(
            repos, origScan.ftrsTable.getCwmColumnSet()).iterator();
        while (iter.hasNext()) {
            FemLocalIndex index = (FemLocalIndex) iter.next();

            if (origScan.isOrderPreserving && !index.equals(origScan.index)) {
                // can't switch indexes if original scan order needs to be
                // preserved
                continue;
            }

            if (!testIndexCoverage(
                    origScan.ftrsTable.getIndexGuide(),
                    index, projectedColumns)) {
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

            if (needRename) {
                // Replace calling convention with FENNEL_PULL_CONVENTION
                RelTraitSet traits =
                    (RelTraitSet)origProject.getTraits().clone();
                traits.setTrait(
                    CallingConventionTraitDef.instance,
                    FennelPullRel.FENNEL_PULL_CONVENTION);

                projectedScan =
                    new FennelRenameRel(
                        origProject.getCluster(),
                        projectedScan,
                        fieldNames,
                        traits);
            }

            call.transformTo(projectedScan);
        }
    }

    private boolean testIndexCoverage(
        FtrsIndexGuide indexGuide,
        FemLocalIndex index,
        Integer [] projection)
    {
        if (index.isClustered()) {
            // clustered index guarantees coverage
            return true;
        }
        Integer [] indexProjection =
            indexGuide.getUnclusteredCoverageArray(index);
        return Arrays.asList(indexProjection).containsAll(
            Arrays.asList(projection));
    }
}


// End FtrsTableProjectionRule.java
