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

package net.sf.farrago.namespace.ftrs;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.keysindexes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;
import net.sf.farrago.query.*;

import net.sf.saffron.core.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.rel.convert.*;
import net.sf.saffron.oj.util.*;
import net.sf.saffron.util.*;
import net.sf.saffron.rex.*;

import java.util.*;
import java.util.List;


/**
 * FtrsScanToSearchRule is a rule for converting FilterRel+FtrsIndexScanRel
 * into FtrsIndexSearchRel (when the filter has the appropriate form).
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FtrsScanToSearchRule extends VolcanoRule
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FtrsScanToSearchRule object.
     */
    public FtrsScanToSearchRule()
    {
        super(
            new RuleOperand(
                FilterRel.class,
                new RuleOperand [] {
                    new RuleOperand(FtrsIndexScanRel.class,null)
                }));
    }

    //~ Methods ---------------------------------------------------------------

    // implement VolcanoRule
    public CallingConvention getOutConvention()
    {
        return null;
    }

    // implement VolcanoRule
    public void onMatch(VolcanoRuleCall call)
    {
        FilterRel filter = (FilterRel) call.rels[0];
        FtrsIndexScanRel scan = (FtrsIndexScanRel) call.rels[1];

        FarragoCatalog catalog = scan.getPreparingStmt().getCatalog();

        // TODO: General framework for converting filters into ranges.  Build
        // on Saffron's expression pattern-matching framework?  Or maybe ANTLR
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
        CwmColumn filterColumn = scan.getColumnForFieldAccess(
            fieldAccess.index);
        assert (filterColumn != null);

        if (catalog.isClustered(scan.index)) {
            // if we're working with a clustered index scan, consider all of
            // the unclustered indexes as well
            Iterator iter =
                catalog.getIndexes(scan.ftrsTable.getCwmColumnSet()).iterator();
            while (iter.hasNext()) {
                CwmSqlindex index = (CwmSqlindex) iter.next();
                considerIndex(index,scan,filterColumn,right,call,extraFilter);
            }
        } else {
            // if we're already working with an unclustered index scan, either
            // we can convert the filter or not; no other indexes are involved
            considerIndex(scan.index,scan,filterColumn,right,call,extraFilter);
        }
    }

    static boolean testIndexColumn(CwmSqlindex index,CwmColumn column)
    {
        List indexedFeatures = index.getIndexedFeature();
        CwmIndexedFeature indexedFeature =
            (CwmIndexedFeature) indexedFeatures.get(0);
        CwmColumn indexedColumn =
            (CwmColumn) indexedFeature.getFeature();
        if (!column.equals(indexedColumn)) {
            return false;
        }
        return true;
    }

    private void considerIndex(
        CwmSqlindex index,
        FtrsIndexScanRel origScan,
        CwmColumn filterColumn,
        RexNode searchValue,
        VolcanoRuleCall call,
        RexNode extraFilter)
    {
        FarragoCatalog catalog = origScan.getPreparingStmt().getCatalog();

        // TODO:  support compound keys
        if (!testIndexColumn(index,filterColumn)) {
            return;
        }

        boolean isUnique = index.isUnique()
            && (index.getIndexedFeature().size() == 1);

        RexNode[] searchExps = new RexNode [] { searchValue };

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
        SaffronRel nullFilterRel = OptUtil.createNullFilter(keyRel,null);

        // Generate code to cast the literal to the index column type.
        FarragoTypeFactory typeFactory =
            origScan.getPreparingStmt().getFarragoTypeFactory();
        SaffronType lhsRowType =
            typeFactory.createColumnType(filterColumn,true);
        SaffronRel castRel = OptUtil.createCastRel(nullFilterRel,lhsRowType);

        SaffronRel keyInput = convert(
            castRel,FennelPullRel.FENNEL_PULL_CONVENTION);
        assert (keyInput != null);

        if (!catalog.isClustered(index)
            && catalog.isClustered(origScan.index))
        {
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
                FtrsUtil.getClusteredDistinctKeyArray(
                    catalog,
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
                    unclusteredScan,keyInput,isUnique,false,null,null);
            FtrsIndexSearchRel clusteredSearch =
                new FtrsIndexSearchRel(
                    origScan,unclusteredSearch,true,false,null,null);
            transformCall(call,clusteredSearch,extraFilter);
        } else {
            // A direct search against an index is easier.
            FtrsIndexSearchRel search =
                new FtrsIndexSearchRel(
                    origScan,keyInput,isUnique,false,null,null);
            transformCall(call,search,extraFilter);
        }
    }

    private void transformCall(
        VolcanoRuleCall call,SaffronRel searchRel,RexNode extraFilter)
    {
        if (extraFilter != null) {
            searchRel = new FilterRel(
                searchRel.getCluster(),searchRel,extraFilter);
        }
        call.transformTo(searchRel);
    }
}


// End FtrsScanToSearchRule.java
