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
package com.lucidera.lcs;

import java.util.*;

import com.lucidera.farrago.*;
import com.lucidera.query.*;

import net.sf.farrago.fem.med.*;
import net.sf.farrago.namespace.impl.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;

/**
 * LcsTableProjectionRule implements the rule for pushing a Projection into
 * a LcsRowScanRel.
 * 
 * @author Zelaine Fong
 * @version $Id$
 */
public class LcsTableProjectionRule extends MedAbstractFennelProjectionRule
{
    //  ~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FtrsTableProjectionRule object.
     */
    public LcsTableProjectionRule()
    {
        super(new RelOptRuleOperand(
                ProjectRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(LcsRowScanRel.class, null)
                }));
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        origProject = (ProjectRel) call.rels[0];
        if (!origProject.isBoxed()) {
            return;
        }

        LcsRowScanRel origScan = (LcsRowScanRel) call.rels[1];
        if (origScan.projectedColumns != null) {
            // TODO:  fold existing projection?
            return;
        }

        boolean needRename = createProjectionList(origScan);
        if (numProjectedCols == 0) {
            // create a rid expression to be used in the case where no fields
            // are being projected
            RexNode defaultExpr = LucidDbSpecialOperators.makeRidExpr(
                origScan.getCluster().getRexBuilder(), origScan);
                
            // there are expressions in the projection; we need to split
            // the projection to first project the input references and any
            // special columns
            PushProjector pushProject = new PushProjector();
            ProjectRel newProject = pushProject.convertProject(
                origProject, null, origScan, 
                LucidDbOperatorTable.ldbInstance().getSpecialOperators(),
                defaultExpr);
            if (newProject != null) {
                call.transformTo(newProject);
            }
            return;
        }

        // TODO jvs 13-Mar-2006:  I put this in for safety so
        // that once residuals get implemented, we don't accidentally
        // project away the clustered indexes needed to evaluate them;
        // but the right thing to do is to union those with the
        // real projection list in order to come up with the
        // set of clustered indexes needed.
        if (origScan.hasExtraFilter()) {
            return;
        }

        // Find all the clustered indexes that reference columns in
        // the projection list.  If the index references any column
        // in the projection, then it needs to be used in the scan.
        ArrayList<FemLocalIndex> indexList = new ArrayList<FemLocalIndex>();

        // Test which clustered indexes are needed to cover the
        // projectedColumns.
        Iterator iter = origScan.clusteredIndexes.iterator();
        while (iter.hasNext()) {
            FemLocalIndex index = (FemLocalIndex) iter.next();

            if (!origScan.getIndexGuide().testIndexCoverage(
                    index, 
                    projectedColumns)) {
                continue;
            }
            indexList.add(index);
        }
        
        // if no clusters need to be read, read from the cluster with the
        // fewest pages; if more than one has the fewest pages, then
        // pick based on alphabetical order of the cluster name
        if (indexList.size() == 0) {
            indexList.add(origScan.getIndexGuide().pickBestIndex(
                origScan.clusteredIndexes));
        }

        // REVIEW:  should cluster be from origProject or origScan?
        RelNode projectedScan =
            new LcsRowScanRel(
                origProject.getCluster(),
                origScan.getInputs(),
                origScan.lcsTable,
                indexList,
                origScan.getConnection(),
                projectedColumns,
                origScan.isFullScan(),
                origScan.hasExtraFilter());

        if (needRename) {
            projectedScan = renameProjectedScan(projectedScan);
        }

        call.transformTo(projectedScan);
    }
    
    /**
     * Maps a projection expression to its underlying field reference.  Also
     * handles special columns, mapping them to their special column ids.
     * 
     * @param exp expression to be mapped
     * @param origFieldName returns field name corresponding to the field
     * reference
     * @param rowType row from which the field reference originated
     * @return ordinal representing the projection element
     */
    protected Integer mapProjCol(
        RexNode exp, List<String> origFieldName, RelDataType rowType)
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
                    projIndex = new Integer(
                        LucidDbOperatorTable.ldbInstance().getSpecialOpColumnId(
                            op));
                    origFieldName.add(
                        LucidDbOperatorTable.ldbInstance().getSpecialOpName(op));
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
