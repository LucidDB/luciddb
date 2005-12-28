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

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexNode;

/**
 * LcsTableProjectionRule implements the rule for pushing a Projection into
 * a LcsRowScanRel.
 * 
 * @author Zelaine Fong
 * @version $Id$Id: //open/lu/dev_lcs/farrago/src/com/lucidera/lcs/LcsTableProjectionRule.java#1 $
 */
public class LcsTableProjectionRule extends RelOptRule
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
    public CallingConvention getOutConvention()
    {
        return FennelRel.FENNEL_EXEC_CONVENTION;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        // REVIEW jvs 27-Dec-2005: Most of this code is copied
        // from FtrsTableProjectionRule.  Factor it out instead into
        // a new class
        // net.sf.farrago.namespace.impl.MedAbstractFennelProjectionRule.
        
        ProjectRel origProject = (ProjectRel) call.rels[0];
        if (!origProject.isBoxed()) {
            return;
        }

        LcsRowScanRel origScan = (LcsRowScanRel) call.rels[1];
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
                rowType.getFields()[fieldAccess.getIndex()].getName();
            if (!projFieldName.equals(origFieldName)) {
                needRename = true;
            }
            projectedColumns[i] = new Integer(fieldAccess.getIndex());
        }

        // Find all the clustered indexes that reference columns in
        // the projection list.  If the index references any column
        // in the projection, then it needs to be used in the scan.
        final FarragoRepos repos = FennelRelUtil.getRepos(origScan);
        ArrayList indexList = new ArrayList();

        Iterator iter = FarragoCatalogUtil.getTableIndexes(
            repos, origScan.lcsTable.getCwmColumnSet()).iterator();
        while (iter.hasNext()) {
            FemLocalIndex index = (FemLocalIndex) iter.next();

            if (!origScan.getIndexGuide().testIndexCoverage(
                    index, 
                    projectedColumns)) {
                continue;
            }
            indexList.add(index);
        }

        // REVIEW:  should cluster be from origProject or origScan?
        RelNode projectedScan =
            new LcsRowScanRel(
                origProject.getCluster(),
                origScan.lcsTable,
                indexList,
                origScan.getConnection(),
                projectedColumns);

        if (needRename) {
            // Replace calling convention with FENNEL_EXEC_CONVENTION
            RelTraitSet traits =
                RelOptUtil.clone(origProject.getTraits());
            traits.setTrait(
                CallingConventionTraitDef.instance,
                FennelRel.FENNEL_EXEC_CONVENTION);

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

// End LcsTableProjectionRule.java
