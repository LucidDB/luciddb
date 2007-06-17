/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
import net.sf.farrago.cwm.keysindexes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.query.*;
import net.sf.farrago.type.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sarg.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.type.*;


// TODO jvs 22-Feb-2005:  combine FtrsScanToSearchRule with
// FtrsTableProjectionRule (say FtrsIndexAccessRule?).  Without combining them,
// we currently miss the opportunity to use an index-only search for {select
// name from sales.depts where name='Hector'}.

/**
 * FtrsScanToSearchRule is a rule for converting FilterRel+FtrsIndexScanRel into
 * FtrsIndexSearchRel (when the filter has the appropriate form).
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FtrsScanToSearchRule
    extends RelOptRule
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FtrsScanToSearchRule object.
     */
    public FtrsScanToSearchRule()
    {
        super(
            new RelOptRuleOperand(
                FilterRel.class,
                new RelOptRuleOperand[] {
                    new RelOptRuleOperand(FtrsIndexScanRel.class, null)
                }));
    }

    //~ Methods ----------------------------------------------------------------

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
        RexBuilder rexBuilder = scan.getCluster().getRexBuilder();
        SargFactory sargFactory = new SargFactory(rexBuilder);
        SargRexAnalyzer rexAnalyzer = sargFactory.newRexAnalyzer();

        RexNode filterExp = filter.getCondition();
        RexNode extraFilter = null;

        List<SargBinding> sargBindingList = rexAnalyzer.analyzeAll(filterExp);

        if (sargBindingList.isEmpty()) {
            // Predicate was not sargable
            return;
        }

        // only support one sargBinding now.
        SargBinding sargBinding = sargBindingList.get(0);
        sargBindingList.remove(0);

        // and make the rest residual for now
        RexNode residualRexNode =
            rexAnalyzer.getSargBindingListToRexNode(sargBindingList);

        RexNode postFilterRexNode = rexAnalyzer.getNonSargFilterRexNode();

        if ((residualRexNode != null) && (postFilterRexNode != null)) {
            extraFilter =
                rexBuilder.makeCall(
                    SqlStdOperatorTable.andOperator,
                    residualRexNode,
                    postFilterRexNode);
        } else if (residualRexNode != null) {
            extraFilter = residualRexNode;
        } else if (postFilterRexNode != null) {
            extraFilter = postFilterRexNode;
        }

        RexInputRef fieldAccess = sargBinding.getInputRef();
        FemAbstractColumn filterColumn =
            scan.getColumnForFieldAccess(fieldAccess.getIndex());
        assert (filterColumn != null);

        if (scan.index.isClustered()) {
            // if we're working with a clustered index scan, consider all of
            // the unclustered indexes as well
            for (
                FemLocalIndex index
                : FarragoCatalogUtil.getTableIndexes(
                    repos,
                    scan.ftrsTable.getCwmColumnSet()))
            {
                considerIndex(
                    index,
                    scan,
                    filterColumn,
                    sargBinding.getExpr(),
                    call,
                    extraFilter);
            }
        } else {
            // if we're already working with an unclustered index scan, either
            // we can convert the filter or not; no other indexes are involved
            considerIndex(
                scan.index,
                scan,
                filterColumn,
                sargBinding.getExpr(),
                call,
                extraFilter);
        }
    }

    static boolean testIndexColumn(
        FemLocalIndex index,
        CwmColumn column)
    {
        List<CwmIndexedFeature> indexedFeatures = index.getIndexedFeature();
        CwmIndexedFeature indexedFeature = indexedFeatures.get(0);
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
        SargExpr sargExpr,
        RelOptRuleCall call,
        RexNode extraFilter)
    {
        // TODO:  support compound keys
        if (!testIndexColumn(index, filterColumn)) {
            return;
        }

        FtrsIndexGuide indexGuide = origScan.ftrsTable.getIndexGuide();

        if (!indexGuide.isValid(index)) {
            return;
        }

        final RelTraitSet callTraits = call.rels[0].getTraits();

        if (!index.isClustered() && origScan.index.isClustered()) {
            if (origScan.isOrderPreserving) {
                // Searching on an unclustered index would destroy the required
                // scan ordering, so we can't do that.
                return;
            }
        }

        // NOTE jvs 24-Jan-2006: I turned this optimization off because
        // BTreeSearchUnique can no longer be used with interval inputs. Turning
        // it back on requires verifying that all intervals are points, and then
        // suppressing generation of directives.
        boolean isUnique;
        if (false) {
            isUnique =
                index.isUnique() && (index.getIndexedFeature().size() == 1);
        } else {
            isUnique = false;
        }

        // Create a type descriptor for the rows representing search
        // keys along with their directives.  Note that we force
        // the key type to nullable because we use null for the representation
        // of infinity (rather than domain-specific junk).
        FarragoPreparingStmt stmt = FennelRelUtil.getPreparingStmt(origScan);
        FarragoTypeFactory typeFactory = stmt.getFarragoTypeFactory();
        RelDataType directiveType =
            typeFactory.createSqlType(
                SqlTypeName.CHAR,
                1);
        RelDataType keyType =
            typeFactory.createTypeWithNullability(
                typeFactory.createCwmElementType(filterColumn),
                true);

        RelDataType keyRowType =
            typeFactory.createStructType(
                new RelDataType[] {
                    directiveType,
                    keyType,
                    directiveType,
                    keyType
                },
                new String[] {
                    "lowerBoundDirective",
                    "lowerBoundKey",
                    "upperBoundDirective",
                    "upperBoundKey"
                });
        RelNode sargRel =
            FennelRelUtil.convertSargExpr(
                callTraits,
                keyRowType,
                origScan.getCluster(),
                sargExpr);
        RelNode keyInput =
            mergeTraitsAndConvert(
                callTraits,
                FennelRel.FENNEL_EXEC_CONVENTION,
                sargRel);
        assert (keyInput != null);

        // Set up projections for the search directive and key.
        Integer [] inputDirectiveProj = new Integer[] { 0, 2 };
        Integer [] inputKeyProj = new Integer[] { 1, 3 };

        if (!index.isClustered() && origScan.index.isClustered()) {
            // By itself, an unclustered index is not going to produce the
            // requested rows.  Instead, it will produce clustered index keys,
            // which we'll use to drive a parent search against the clustered
            // index.
            Integer [] clusteredKeyColumns =
                indexGuide.getClusteredDistinctKeyArray(
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
                new FtrsIndexSearchRel(
                    unclusteredScan,
                    keyInput,
                    isUnique,
                    false,
                    inputKeyProj,
                    null,
                    inputDirectiveProj);

            FtrsIndexSearchRel clusteredSearch =
                new FtrsIndexSearchRel(
                    origScan,
                    unclusteredSearch,
                    true,
                    false,
                    null,
                    null,
                    null);

            transformCall(call, clusteredSearch, extraFilter);
        } else {
            // A direct search against an index is easier.
            FtrsIndexSearchRel search =
                new FtrsIndexSearchRel(
                    origScan,
                    keyInput,
                    isUnique,
                    false,
                    inputKeyProj,
                    null,
                    inputDirectiveProj);

            transformCall(call, search, extraFilter);
        }
    }

    private void transformCall(
        RelOptRuleCall call,
        RelNode searchRel,
        RexNode extraFilter)
    {
        if (extraFilter != null) {
            searchRel = CalcRel.createFilter(searchRel, extraFilter);
        }
        call.transformTo(searchRel);
    }
}

// End FtrsScanToSearchRule.java
