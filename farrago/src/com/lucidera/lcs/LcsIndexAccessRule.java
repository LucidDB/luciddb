/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.query.*;
import net.sf.farrago.type.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.keysindexes.*;

import org.eigenbase.oj.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sarg.*;

/**
 * LcsIndexAccessRule is a rule for converting FilterRel+LcsRowScanRel
 * into LcsIndexAccessRel (when the filter has the appropriate form).
 *
 * @author Rushan Chen
 * @version $Id$
 */
class LcsIndexAccessRule extends RelOptRule
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new LcsIndexAccessRule object.
     */
    public LcsIndexAccessRule()
    {
        super(new RelOptRuleOperand(
                FilterRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(LcsRowScanRel.class, null)
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
        LcsRowScanRel origRowScan = (LcsRowScanRel) call.rels[1];

        FarragoRepos repos = FennelRelUtil.getRepos(origRowScan);

        RexNode filterExp = filter.getCondition();

        RexNode extraFilter = null;

        SargFactory sargFactory = new SargFactory(
            origRowScan.getCluster().getRexBuilder());
        SargRexAnalyzer rexAnalyzer = sargFactory.newRexAnalyzer();
        SargBinding sargBinding = rexAnalyzer.analyze(filterExp);

        if (sargBinding == null) {
            // Predicate was not sargable
            return;
        }

        RexInputRef fieldAccess = sargBinding.getInputRef();
        FemAbstractColumn filterColumn =
            origRowScan.getColumnForFieldAccess(fieldAccess.getIndex());
        assert (filterColumn != null);

        Iterator iter = FarragoCatalogUtil.getUnclusteredIndexes(
            repos, origRowScan.lcsTable.getCwmColumnSet()).iterator();

        while (iter.hasNext()) {
            FemLocalIndex index = (FemLocalIndex) iter.next();
            considerIndex(
                index, origRowScan, filterColumn, sargBinding.getExpr(), call,
                extraFilter);
        }
    }

    // check if the index has the same prefix
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
        LcsRowScanRel origRowScan,
        FemAbstractColumn filterColumn,
        SargExpr sargExpr,
        RelOptRuleCall call,
        RexNode extraFilter)
    {
        // TODO:  support compound keys
        if (!testIndexColumn(index, filterColumn)) {
            return;
        }

        LcsIndexGuide indexGuide = origRowScan.lcsTable.getIndexGuide();

        if (!indexGuide.isValid(index)) {
            return;
        }
        
        final RelTraitSet callTraits = call.rels[0].getTraits(); 

        // NOTE jvs 24-Jan-2006: I turned this optimization off because
        // BTreeSearchUnique can no longer be used with interval inputs.
        // Turning it back on requires verifying that all intervals are points,
        // and then suppressing generation of directives.
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
        FarragoPreparingStmt stmt = FennelRelUtil.getPreparingStmt(origRowScan);
        FarragoTypeFactory typeFactory = stmt.getFarragoTypeFactory();
        RelDataType directiveType =
            typeFactory.createSqlType(
                SqlTypeName.Char,
                1);
        RelDataType keyType =
            typeFactory.createTypeWithNullability(
                typeFactory.createCwmElementType(filterColumn),
                true);

        RelDataType keyRowType =
            typeFactory.createStructType(
                new RelDataType [] {
                    directiveType,
                    keyType,
                    directiveType,
                    keyType
                },
                new String [] {
                    "lowerBoundDirective",
                    "lowerBoundKey",
                    "upperBoundDirective",
                    "upperBoundKey"
                });

        RelNode sargRel = FennelRelUtil.convertSargExpr(
            callTraits,
            keyRowType,
            origRowScan.getCluster(),
            sargExpr);

        RelNode keyInput =
            mergeTraitsAndConvert(
                callTraits, FennelRel.FENNEL_EXEC_CONVENTION,
                sargRel);

        assert (keyInput != null);

        // Set up projections for the search directive and key.
        // TODO: multi-key index key proj and directive proj
        Integer [] inputDirectiveProj = new Integer [] { 0, 2 };
        Integer [] inputKeyProj = new Integer [] { 1, 3 };

        
        // First construct an index scan, and then try to add index search.
        // TODO: do we need the orderPreserving property?It is set to false now.
        LcsIndexScanRel indexScan =
            new LcsIndexScanRel(
                origRowScan.getCluster(),
                origRowScan.lcsTable,
                index,
                origRowScan.getConnection(),
                null,
                false);
        
        LcsIndexSearchRel indexSearch =
            new LcsIndexSearchRel(indexScan, keyInput, false,
                false, inputKeyProj, null, inputDirectiveProj);

        // check if the index contains all the required columns.
        // TODO: this is not implemented yet. IndexScan/Search always returns
        // the [SRID, bitmap1, bitmap2] which is sent to drive a LcsRowScan
        //
        // if ((origRowScan.projectedColumns.size() == 1) &&
        // !testIndexColumn(index, filterColumn)) {
            // A direct search against an index is easier.
        //    transformCall(call, indexSearch, extraFilter);
        // } else 
        {
            RelNode [] inputRels = new RelNode[1];
            
            // TODO: add extra range list here to the stream def's.
            inputRels[0] = indexSearch;

            // Build a RowScan rel based on index search with no extra filters.
            LcsRowScanRel rowScan =
                new LcsRowScanRel(
                    origRowScan.getCluster(),
                    inputRels,
                    origRowScan.lcsTable,
                    origRowScan.clusteredIndexes,
                    origRowScan.getConnection(),
                    origRowScan.projectedColumns,
                    false, false);

            transformCall(call, rowScan, extraFilter);
        }
    }

    private void transformCall(
        RelOptRuleCall call,
        RelNode rel,
        RexNode extraFilter)
    {
        if (extraFilter != null) {
            rel =
                new FilterRel(rel.getCluster(), rel, extraFilter);
        }
        call.transformTo(rel);
    }

}

// End LcsIndexAccessRule.java
