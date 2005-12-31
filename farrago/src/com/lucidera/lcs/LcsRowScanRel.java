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
import java.util.List;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fennel.tuple.FennelStandardTypeDescriptor;
import net.sf.farrago.fennel.tuple.FennelStoredTypeDescriptor;
import net.sf.farrago.query.*;
import net.sf.farrago.util.*;

import openjava.ptree.Literal;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;

/**
 * LcsRowScanRel is the relational expression corresponding to a scan on a
 * column store table.
 * 
 * @author Zelaine Fong
 * @version $Id$
 */
public class LcsRowScanRel extends TableAccessRelBase implements FennelRel
{
    //~ Instance fields -------------------------------------------------------
   
    private LcsIndexGuide indexGuide;
    
    /** Clusters to use for access */
    final List<FemLocalIndex> clusteredIndexes;
    
    /** Refinement for super.table */
    final LcsTable lcsTable;

    /**
     * Array of 0-based flattened column ordinals to project; if null, project
     * all columns.  Note that these ordinals are relative to the table.
     */
    final Integer [] projectedColumns;
    
    FarragoRepos repos;
    
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new LcsRowScanRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param lcsTable table being scanned
     * @param clusteredIndexes clusters to use for table access
     * @param connection connection
     * @param projectedColumns array of 0-based table-relative column ordinals,
     * or null to project all columns
     */
    public LcsRowScanRel(
        RelOptCluster cluster,
        LcsTable lcsTable,
        List<FemLocalIndex> clusteredIndexes,
        RelOptConnection connection,
        Integer [] projectedColumns)
    {
        super(cluster, new RelTraitSet(FENNEL_EXEC_CONVENTION), lcsTable,
              connection);
        this.lcsTable = lcsTable;
        this.clusteredIndexes = clusteredIndexes;
        this.projectedColumns = projectedColumns;
        assert lcsTable.getPreparingStmt() ==
            FennelRelUtil.getPreparingStmt(this);
        
        repos = FennelRelUtil.getRepos(this);
    }
    
    //~ Methods ---------------------------------------------------------------
    
    // implement RelNode
    public Object clone()
    {
        LcsRowScanRel clone = 
            new LcsRowScanRel(getCluster(), lcsTable, clusteredIndexes,
                              connection, projectedColumns);
        clone.inheritTraitsFrom(this);
        return clone;
    }
    
    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        return computeCost(planner, getRows());
    }
    
    // implement RelNode
    public RelDataType deriveRowType()
    {
        RelDataType flattenedRowType =
            getIndexGuide().getFlattenedRowType();
        if (projectedColumns == null) {
            return flattenedRowType;
        } else {
            final RelDataTypeField [] fields = flattenedRowType.getFields();
            return getCluster().getTypeFactory().createStructType(
                new RelDataTypeFactory.FieldInfo() {
                    public int getFieldCount()
                    {
                        return projectedColumns.length;
                    }

                    public String getFieldName(int index)
                    {
                        final int i = projectedColumns[index].intValue();
                        return fields[i].getName();
                    }

                    public RelDataType getFieldType(int index)
                    {
                        final int i = projectedColumns[index].intValue();
                        return fields[i].getType();
                    }
                });
        }
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
        
        // REVIEW jvs 27-Dec-2005:  Since LcsRowScanRel is given
        // a list (implying ordering) as input, it seems to me that
        // the caller should be responsible for putting the
        // list into a deterministic order rather than doing
        // it here.
        
        TreeSet indexNames = new TreeSet();
        for (FemLocalIndex index : clusteredIndexes) {
            indexNames.add(index.getName());
        }

        // REVIEW jvs 27-Dec-2005: See
        // http://issues.eigenbase.org/browse/FRG-8; the "clustered indexes"
        // attribute is an example of a derived attribute which doesn't need to
        // be part of the digest (it's implied by the column projection since
        // we don't allow clusters to overlap), but is useful in verbose mode.
        // Can't resolve this comment until FRG-8 is completed.
        
        pw.explain(
            this,
            new String [] { "table", "projection", "clustered indexes"},
            new Object [] {
                Arrays.asList(lcsTable.getQualifiedName()), projection,
                indexNames});
    }

    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        return Literal.constantNull();
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);

        FemLcsRowScanStreamDef scanStream = repos.newFemLcsRowScanStreamDef();

        defineScanStream(scanStream);
        
        // for now, create a dummy empty rid stream as input to the scan;
        // this will trigger a full table scan
        
        FemMockTupleStreamDef input = repos.newFemMockTupleStreamDef();
        input.setRowCount(0);
        
        FemTupleDescriptor tupleDesc = repos.newFemTupleDescriptor();
        FennelStoredTypeDescriptor typeDesc =
            FennelStandardTypeDescriptor.INT_64;
        FemTupleAttrDescriptor attrDesc = repos.newFemTupleAttrDescriptor();
        tupleDesc.getAttrDescriptor().add(attrDesc);
        attrDesc.setTypeOrdinal(typeDesc.getOrdinal());
        input.setOutputDesc(tupleDesc);
            
        implementor.addDataFlowFromProducerToConsumer(input, scanStream);

        return scanStream;
    }

    public LcsIndexGuide getIndexGuide()
    {
        if (indexGuide == null) {
            indexGuide = new LcsIndexGuide(
                lcsTable.getPreparingStmt().getFarragoTypeFactory(),
                lcsTable.getCwmColumnSet(),
                clusteredIndexes);
        }
        return indexGuide;
    }

    // implement RelNode
    private RelOptCost computeCost(
        RelOptPlanner planner,
        double dRows)
    {
        // TODO:  compute page-based I/O cost
        // CPU cost is proportional to number of columns projected
        // I/O cost is proportional to pages of index scanned
        double dCpu = dRows * getRowType().getFieldList().size();

        int nIndexCols = 0;
        for (FemLocalIndex index : clusteredIndexes) {
            nIndexCols +=
                getIndexGuide().getNumFlattenedClusterCols(index);
        }
        
        double dIo = dRows * nIndexCols;

        return planner.makeCost(dRows, dCpu, dIo);
    }

    /**
     * Fills in a stream definition for this scan.
     *
     * @param scanStream stream definition to fill in
     */
    private void defineScanStream(FemLcsRowScanStreamDef scanStream)
    {  
        // setup each cluster scan def
        for (FemLocalIndex index : clusteredIndexes) {
            FemLcsClusterScanDef clusterScan = repos.newFemLcsClusterScanDef();
            defineClusterScan(index, clusterScan);
            scanStream.getClusterScan().add(clusterScan);
        }
        
        // setup the output projection relative to the ordered list of
        // clustered indexes
        Integer [] clusterProjection = 
            getIndexGuide().computeProjectedColumns(projectedColumns);
        scanStream.setOutputProj(
            FennelRelUtil.createTupleProjection(repos, clusterProjection));
    }
    
    /**
     * Fills in a cluster scan def for this scan
     * 
     * @param index clustered index corresponding to this can
     * @param clusterScan clustered scan to fill in
     */
    private void defineClusterScan(FemLocalIndex index,
                                   FemLcsClusterScanDef clusterScan)
    {
        FarragoPreparingStmt stmt = FennelRelUtil.getPreparingStmt(this);
        
        // setup cluster tuple descriptor and add to cluster map
        FemTupleDescriptor clusterDesc =
            getIndexGuide().getClusterTupleDesc(index);
        clusterScan.setClusterTupleDesc(clusterDesc);
        
        // setup index accessor def fields
        
        if (!FarragoCatalogUtil.isIndexTemporary(index)) {
            clusterScan.setRootPageId(stmt.getIndexMap().getIndexRoot(index));
        } else {
            // For a temporary index, each execution needs to bind to
            // a session-private root.  So don't burn anything into
            // the plan.
            clusterScan.setRootPageId(-1);
        }

        clusterScan.setSegmentId(LcsDataServer.getIndexSegmentId(index));
        clusterScan.setIndexId(JmiUtil.getObjectId(index));

        FemTupleDescriptor tupleDesc = indexGuide.createBtreeTupleDesc();
        clusterScan.setTupleDesc(tupleDesc);
        
        Integer[] keyProj = {0};
        clusterScan.setKeyProj(
            FennelRelUtil.createTupleProjection(repos, keyProj));
    }

    // implement FennelRel
    public RelFieldCollation [] getCollations()
    {
        // lcs clustered scan returns rows in rid order, but we don't
        // yet support selects on rids so return an empty list
        return RelFieldCollation.emptyCollationArray;
    }
}

// End LcsRowScanRel.java
