/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
*/
package net.sf.farrago.namespace.ftrs;

import java.util.*;
import java.util.List;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.keysindexes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.query.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.oj.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.*;

// TODO jvs 22-Feb-2005:  combine FtrsScanToSearchRule with
// FtrsTableProjectionRule (say FtrsIndexAccessRule?).  Without combining them,
// we currently miss the opportunity to use an index-only search
// for {select name from sales.depts where name='Hector'}.

/**
 * FtrsScanToSearchRule is a rule for converting FilterRel+FtrsIndexScanRel
 * into FtrsIndexSearchRel (when the filter has the appropriate form).
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FtrsScanToSearchRule extends RelOptRule
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FtrsScanToSearchRule object.
     */
    public FtrsScanToSearchRule()
    {
        super(new RelOptRuleOperand(
                FilterRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(FtrsIndexScanRel.class, null)
                }));
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return null;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        FilterRel filter = (FilterRel) call.rels[0];
        FtrsIndexScanRel scan = (FtrsIndexScanRel) call.rels[1];

        FarragoRepos repos = FennelRelUtil.getRepos(scan);

        // TODO: General framework for converting filters into ranges.  Build
        // on the rex expression pattern-matching framework?  Or maybe ANTLR
        // tree matching?  Need canonical form, compound keys, inequalities.
        RexNode filterExp = filter.condition;

        RexNode extraFilter = null;

        if (filterExp.isA(RexKind.And)) {
            RexCall andExpression = (RexCall) filterExp;
            filterExp = andExpression.operands[0];
            extraFilter = andExpression.operands[1];
        }

        if (!filterExp.isA(RexKind.Equals)) {
            return;
        }
        RexCall binaryExpression = (RexCall) filterExp;

        RexNode left = binaryExpression.operands[0];
        RexNode right = binaryExpression.operands[1];
        if (!(left instanceof RexInputRef)) {
            return;
        }

        // TODO:  support other types of constant (e.g. CURRENT_USER) on RHS
        if (!right.isA(RexKind.Literal) && !right.isA(RexKind.DynamicParam)) {
            return;
        }
        RexInputRef fieldAccess = (RexInputRef) left;
        FemAbstractColumn filterColumn =
            scan.getColumnForFieldAccess(fieldAccess.index);
        assert (filterColumn != null);

        if (scan.index.isClustered()) {
            // if we're working with a clustered index scan, consider all of
            // the unclustered indexes as well
            Iterator iter = FarragoCatalogUtil.getTableIndexes(
                repos, scan.ftrsTable.getCwmColumnSet()).iterator();
            while (iter.hasNext()) {
                FemLocalIndex index = (FemLocalIndex) iter.next();
                considerIndex(index, scan, filterColumn, right, call,
                    extraFilter);
            }
        } else {
            // if we're already working with an unclustered index scan, either
            // we can convert the filter or not; no other indexes are involved
            considerIndex(scan.index, scan, filterColumn, right, call,
                extraFilter);
        }
    }

    static boolean testIndexColumn(
        FemLocalIndex index,
        CwmColumn column)
    {
        List indexedFeatures = index.getIndexedFeature();
        CwmIndexedFeature indexedFeature =
            (CwmIndexedFeature) indexedFeatures.get(0);
        CwmColumn indexedColumn = (CwmColumn) indexedFeature.getFeature();
        if (!column.equals(indexedColumn)) {
            return false;
        }
        return true;
    }

    private void considerIndex(
        FemLocalIndex index,
        FtrsIndexScanRel origScan,
        FemAbstractColumn filterColumn,
        RexNode searchValue,
        RelOptRuleCall call,
        RexNode extraFilter)
    {
        // TODO:  support compound keys
        if (!testIndexColumn(index, filterColumn)) {
            return;
        }

        boolean isUnique =
            index.isUnique() && (index.getIndexedFeature().size() == 1);

        RexNode [] searchExps = new RexNode [] { searchValue };

        // Generate a one-row relation producing the key to search for.
        OneRowRel oneRowRel = new OneRowRel(origScan.getCluster());
        ProjectRel keyRel =
            new ProjectRel(
                origScan.getCluster(),
                oneRowRel,
                searchExps,
                null,
                ProjectRel.Flags.Boxed);

        // Add a filter to remove nulls, since they can never match the
        // equals condition.
        RelNode nullFilterRel = RelOptUtil.createNullFilter(keyRel, null);

        // Generate code to cast the literal to the index column type.
        FarragoPreparingStmt stmt = FennelRelUtil.getPreparingStmt(origScan);
        FarragoTypeFactory typeFactory = stmt.getFarragoTypeFactory();
        RelDataType lhsRowType =
            typeFactory.createStructType(
                new RelDataType [] {
                    typeFactory.createCwmElementType(filterColumn)
                },
                new String [] { "filterColumn" });
        RelNode castRel = RelOptUtil.createCastRel(nullFilterRel, lhsRowType);

        RelNode keyInput =
            convert(castRel, FennelPullRel.FENNEL_PULL_CONVENTION);
        assert (keyInput != null);

        if (!index.isClustered() && origScan.index.isClustered()) {
            // By itself, an unclustered index is not going to produce the
            // requested rows.  Instead, it will produce clustered index keys,
            // which we'll use to drive a parent search against the clustered
            // index.
            if (origScan.isOrderPreserving) {
                // Searching on an unclustered index would destroy the required
                // scan ordering, so we can't do that.
                return;
            }

            Integer [] clusteredKeyColumns =
                origScan.ftrsTable.getIndexGuide().getClusteredDistinctKeyArray(
                    origScan.index);
            FtrsIndexScanRel unclusteredScan =
                new FtrsIndexScanRel(
                    origScan.getCluster(),
                    origScan.ftrsTable,
                    index,
                    origScan.getConnection(),
                    clusteredKeyColumns,
                    origScan.isOrderPreserving);
            FtrsIndexSearchRel unclusteredSearch =
                new FtrsIndexSearchRel(unclusteredScan, keyInput, isUnique,
                    false, null, null);
            FtrsIndexSearchRel clusteredSearch =
                new FtrsIndexSearchRel(origScan, unclusteredSearch, true,
                    false, null, null);
            transformCall(call, clusteredSearch, extraFilter);
        } else {
            // A direct search against an index is easier.
            FtrsIndexSearchRel search =
                new FtrsIndexSearchRel(origScan, keyInput, isUnique, false,
                    null, null);
            transformCall(call, search, extraFilter);
        }
    }

    private void transformCall(
        RelOptRuleCall call,
        RelNode searchRel,
        RexNode extraFilter)
    {
        if (extraFilter != null) {
            searchRel =
                new FilterRel(
                    searchRel.getCluster(),
                    searchRel,
                    extraFilter);
        }
        call.transformTo(searchRel);
    }
}


// End FtrsScanToSearchRule.java
