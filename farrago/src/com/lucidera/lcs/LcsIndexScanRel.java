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
import java.util.List;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.*;
import net.sf.farrago.cwm.keysindexes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.query.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import openjava.ptree.Literal;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.util.*;


/**
 * LcsIndexScanRel is the relational expression corresponding to a scan via
 * a particular index over the contents of a table stored in LCS format.
 *
 * TODO: is the super class TableAccessRelBase necessary?
 *
 * @author Rushan Chen
 * @version $Id$
 */
class LcsIndexScanRel extends TableAccessRelBase implements FennelRel
{
    //~ Instance fields -------------------------------------------------------

    /** Index to use for access. */
    final FemLocalIndex index;

    /** Refinement for super.table. */
    final LcsTable lcsTable;

    /**
     * Array of 0-based flattened column ordinals to project; if null, project
     * all columns.  Note that these ordinals are relative to the table, not
     * the index.
     */
    final Integer [] projectedColumns;
    final boolean isOrderPreserving;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new LcsIndexScanRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param lcsTable table being scanned
     * @param index index to use for table access
     * @param connection connection
     * @param projectedColumns array of 0-based table-relative column ordinals,
     * or null to project all columns
     * @param isOrderPreserving if true, returned rows must be in index order;
     * if false, rows can be returned out of order
     */
    public LcsIndexScanRel(
        RelOptCluster cluster,
        LcsTable lcsTable,
        FemLocalIndex index,
        RelOptConnection connection,
        Integer [] projectedColumns,
        boolean isOrderPreserving)
    {
        super(
            cluster, new RelTraitSet(FENNEL_EXEC_CONVENTION), lcsTable,
            connection);
        this.lcsTable = lcsTable;
        this.index = index;
        this.projectedColumns = projectedColumns;
        this.isOrderPreserving = isOrderPreserving;
        assert lcsTable.getPreparingStmt() ==
            FennelRelUtil.getPreparingStmt(this);
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelNode
    public Object clone()
    {
        LcsIndexScanRel clone =
            new LcsIndexScanRel(
                getCluster(), lcsTable, index, connection, projectedColumns,
                isOrderPreserving);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        RelOptCost cost = computeCost(
            planner,
            getRows());

        return cost;
    }

    // implement RelNode
    public RelDataType deriveRowType()
    {
        // TODO: to handle the projection case where only the key fields are
        // the output.
        return lcsTable.getIndexGuide().createUnclusteredBitmapRowType();
    }

    // override TableAccess
    public void explain(RelOptPlanWriter pw)
    {
        Object projection;
        if (projectedColumns == null) {
            projection = "*";
        } else {
            projection = Arrays.asList(projectedColumns);
        }
        pw.explain(
            this,
            new String [] { "table", "projection", "index", "preserveOrder" },
            new Object [] {
                Arrays.asList(lcsTable.getQualifiedName()), projection,
                index.getName(), Boolean.valueOf(isOrderPreserving)
            });
    }

    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        return Literal.constantNull();
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        return lcsTable.getIndexGuide().newIndexScan(this, index);
    }

    public FemIndexSearchDef newIndexSearch()
    {
        return lcsTable.getIndexGuide().newIndexSearch(this, index);
    }

    // implement RelNode
    RelOptCost computeCost(
        RelOptPlanner planner,
        double dRows)
    {
        // TODO:  compute page-based I/O cost
        // CPU cost is proportional to number of columns projected
        // I/O cost is proportional to pages of index scanned
        double dCpu = dRows * getRowType().getFieldList().size();

        // [RID, bitmapfield1, bitmapfield2]
        int nIndexCols = 3;

        double dIo = dRows * nIndexCols;

        return planner.makeCost(dRows, dCpu, dIo);
    }

    // implement FennelRel
    // get list of columns ordinals that the scan is ordered on.
    // The list simply consists of the index keys.
    // TODO: in the case where the output format is [RID, bitmapfield1,
    // bitmapfield2], what should getCollations return? i.e  Do the collation
    // fields need to be contained by the output tuple?
    public RelFieldCollation [] getCollations()
    {
        List collationList = new ArrayList();
        List indexFeatures = index.getIndexedFeature();
        for (int i = 0; i < indexFeatures.size(); i++) {
            RelFieldCollation collation = null;
            CwmIndexedFeature feature = 
                (CwmIndexedFeature) indexFeatures.get(i);
            FemAbstractColumn column = 
                (FemAbstractColumn) feature.getFeature();
            collation = new RelFieldCollation(column.getOrdinal());
            collationList.add(collation);
        }
        return (RelFieldCollation []) collationList.toArray(RelFieldCollation.emptyCollationArray);
    }
}

// End LcsIndexScanRel.java
