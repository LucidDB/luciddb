/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.farrago.query;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.util.*;

import net.sf.saffron.core.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.util.*;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.rex.RexInputRef;

import java.util.*;


/**
 * FennelTableProjectionRule implements the rule for pushing a Projection into
 * a FennelIndexScanRel.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FennelTableProjectionRule extends VolcanoRule
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FennelTableProjectionRule object.
     */
    public FennelTableProjectionRule()
    {
        super(
            new RuleOperand(
                ProjectRel.class,
                new RuleOperand [] {
                    new RuleOperand(FennelIndexScanRel.class,null)
                }));
    }

    //~ Methods ---------------------------------------------------------------

    // implement VolcanoRule
    public CallingConvention getOutConvention()
    {
        return FennelRel.FENNEL_CALLING_CONVENTION;
    }

    // implement VolcanoRule
    public void onMatch(VolcanoRuleCall call)
    {
        ProjectRel origProject = (ProjectRel) call.rels[0];
        if (!origProject.isBoxed()) {
            return;
        }

        FennelIndexScanRel origScan = (FennelIndexScanRel) call.rels[1];
        if (origScan.projectedColumns != null) {
            // TODO:  fold existing projection?
            return;
        }

        // REVIEW:  what about AnonFields?
        
        // TODO:  rather than failing, split into parts that can be
        // pushed down and parts that can't
        
        int n = origProject.getChildExps().length;
        Integer [] projectedColumns = new Integer[n];
        SaffronType rowType = origScan.fennelTable.getRowType();
        SaffronType projType = origProject.getRowType();
        SaffronField [] projFields = projType.getFields();
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
        FarragoCatalog catalog = origScan.getPreparingStmt().getCatalog();
        Iterator iter =
            catalog.getIndexes(origScan.fennelTable.cwmTable).iterator();
        while (iter.hasNext()) {
            CwmSqlindex index = (CwmSqlindex) iter.next();

            if (origScan.isOrderPreserving && !index.equals(origScan.index)) {
                // can't switch indexes if original scan order needs to be
                // preserved
                continue;
            }

            if (!testIndexCoverage(catalog,index,projectedColumns)) {
                continue;
            }

            // REVIEW:  should cluster be from origProject or origScan?
            SaffronRel projectedScan =
                new FennelIndexScanRel(
                    origProject.getCluster(),
                    origScan.fennelTable,
                    index,
                    origScan.getConnection(),
                    projectedColumns,
                    origScan.isOrderPreserving);

            if (needRename) {
                projectedScan =
                    new FennelRenameRel(
                        origProject.getCluster(),
                        projectedScan,
                        fieldNames);
            }

            call.transformTo(projectedScan);
        }
    }

    private boolean testIndexCoverage(
        FarragoCatalog catalog,
        CwmSqlindex index,
        Integer [] projection)
    {
        if (catalog.isClustered(index)) {
            // clustered index guarantees coverage
            return true;
        }
        Integer [] indexProjection =
            FennelRelUtil.getUnclusteredCoverageArray(catalog,index);
        return Arrays.asList(indexProjection).containsAll(
            Arrays.asList(projection));
    }
}


// End FennelTableProjectionRule.java
