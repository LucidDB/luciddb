/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2010 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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
package org.luciddb.lcs;

import org.luciddb.session.*;

import java.util.*;

import net.sf.farrago.fem.med.*;
import net.sf.farrago.namespace.impl.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.PushProjector;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;


/**
 * LcsTableProjectionRule implements the rule for pushing a Projection into a
 * LcsRowScanRel.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class LcsTableProjectionRule
    extends MedAbstractFennelProjectionRule
{
    public static final LcsTableProjectionRule instance =
        new LcsTableProjectionRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a LcsTableProjectionRule.
     */
    private LcsTableProjectionRule()
    {
        super(
            new RelOptRuleOperand(
                ProjectRel.class,
                new RelOptRuleOperand(LcsRowScanRelBase.class, ANY)));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        ProjectRel origProject = (ProjectRel) call.rels[0];
        if (!origProject.isBoxed()) {
            return;
        }

        LcsRowScanRelBase origScan = (LcsRowScanRelBase) call.rels[1];
        if (origScan.projectedColumns != null) {
            return;
        }

        // determine which columns can be projected from the scan, pulling
        // out references from expressions, if necessary
        List<Integer> projectedColumnList = new ArrayList<Integer>();
        List<ProjectRel> newProjList = new ArrayList<ProjectRel>();

        // create a rid expression to be used in the case where no fields
        // are being projected
        RexNode defaultExpr =
            LucidDbSpecialOperators.makeRidExpr(
                origScan.getCluster().getRexBuilder(),
                origScan);
        boolean needRename =
            createProjectionList(
                origScan,
                origProject,
                projectedColumnList,
                new PushProjector.OperatorExprCondition(
                    LucidDbOperatorTable.ldbInstance().getSpecialOperators()),
                defaultExpr,
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

        // Find all the clustered indexes that reference columns in
        // the projection list.  If the index references any column
        // in the projection, then it needs to be used in the scan.
        // Put clusters corresponding to residual columns first in the
        // list.
        List<FemLocalIndex> indexList =
            origScan.getIndexGuide().createResidualClusterList(
                origScan.residualColumns);

        // Test which of the remaining clustered indexes are needed to cover
        // the projectedColumns.
        Integer [] projectedColumns =
            projectedColumnList.toArray(
                new Integer[projectedColumnList.size()]);

        for (FemLocalIndex index : origScan.clusteredIndexes) {
            if (indexList.contains(index)) {
                continue;
            }
            if (!origScan.getIndexGuide().testIndexCoverage(
                    index,
                    projectedColumns))
            {
                continue;
            }
            indexList.add(index);
        }

        // If no clusters need to be read, read from the cluster with the
        // fewest pages; if more than one has the fewest pages, then
        // pick based on alphabetical order of the cluster name.
        if (indexList.size() == 0) {
            indexList.add(
                LcsIndexOptimizer.getIndexWithMinDiskPages(origScan));
        }

        // REVIEW:  should cluster be from origProject or origScan?
        RelNode projectedScan;
        if (origScan instanceof LcsRowScanRel) {
            projectedScan =
                new LcsRowScanRel(
                    origProject.getCluster(),
                    origScan.getInputs(),
                    origScan.lcsTable,
                    indexList,
                    origScan.getConnection(),
                    projectedColumns,
                    origScan.isFullScan,
                    origScan.residualColumns,
                    origScan.inputSelectivity);
        } else {
            projectedScan =
                new LcsSamplingRowScanRel(
                    origProject.getCluster(),
                    origScan.getInputs(),
                    origScan.lcsTable,
                    indexList,
                    origScan.getConnection(),
                    projectedColumns,
                    ((LcsSamplingRowScanRel) origScan).samplingParams);
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

        call.transformTo(modRelNode);
    }

    /**
     * Maps a projection expression to its underlying field reference. Also
     * handles special columns, mapping them to their special column ids.
     *
     * @param exp expression to be mapped
     * @param origFieldName returns field name corresponding to the field
     * reference
     * @param rowType row from which the field reference originated
     *
     * @return ordinal representing the projection element
     */
    protected Integer mapProjCol(
        RexNode exp,
        List<String> origFieldName,
        RelDataType rowType)
    {
        Integer projIndex = null;

        if (exp instanceof RexCall) {
            RexCall call = (RexCall) exp;
            SqlOperator op = call.getOperator();
            if (LucidDbOperatorTable.ldbInstance().isSpecialOperator(op)) {
                // make sure the special operator's argument references
                // at least one column from the row
                BitSet exprArgs = new BitSet(rowType.getFieldCount());
                exp.accept(new RelOptUtil.InputFinder(exprArgs));
                if (exprArgs.cardinality() > 0) {
                    projIndex =
                        new Integer(
                            LucidDbOperatorTable.ldbInstance()
                                                .getSpecialOpColumnId(
                                                    op));
                    origFieldName.add(
                        LucidDbOperatorTable.ldbInstance().getSpecialOpName(
                            op));
                }
            }
        }
        if (exp instanceof RexInputRef) {
            projIndex = mapFieldRef(exp, origFieldName, rowType);
        }

        return projIndex;
    }
}

// End LcsTableProjectionRule.java
