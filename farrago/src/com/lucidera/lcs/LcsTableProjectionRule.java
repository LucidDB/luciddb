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
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;

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
            // projection rule cannot be applied
            return;
        }

        // Find all the clustered indexes that reference columns in
        // the projection list.  If the index references any column
        // in the projection, then it needs to be used in the scan.
        final FarragoRepos repos = FennelRelUtil.getRepos(origScan);
        ArrayList indexList = new ArrayList();

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

        // REVIEW:  should cluster be from origProject or origScan?
        RelNode projectedScan =
            new LcsRowScanRel(
                origProject.getCluster(),
                null,
                origScan.lcsTable,
                indexList,
                origScan.getConnection(),
                projectedColumns);

        if (needRename) {
            projectedScan = renameProjectedScan(projectedScan);
        }

        call.transformTo(projectedScan);
    }
}

// End LcsTableProjectionRule.java
