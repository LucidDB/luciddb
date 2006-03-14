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
import net.sf.farrago.cwm.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fennel.tuple.*;
import net.sf.farrago.query.*;
import net.sf.farrago.util.*;

import openjava.ptree.Literal;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;

/*
 * LcsRowScanRel is the relational expression corresponding to a scan on a
 * column store table.
 * 
 * @author Zelaine Fong
 * @version $Id$
 */
public class LcsRowScanRel extends FennelMultipleRel
{
    //~ Instance fields -------------------------------------------------------
   
    private LcsIndexGuide indexGuide;
    
    // Clusters to use for access.
    final List<FemLocalIndex> clusteredIndexes;
    
    // Refinement for super.table.
    final LcsTable lcsTable;

    // TODO: keep the connection property (originally of TableAccessRelbase)
    // remove this when LcsIndexScan is changed to derive from SingleRel.
    RelOptConnection connection;

    /**
     * Array of 0-based flattened column ordinals to project; if null, project
     * all columns.  Note that these ordinals are relative to the table.
     */
    final Integer [] projectedColumns;
    
    FarragoRepos repos;
    
    /**
     * Types of scans to perform.
     */
    boolean isFullScan;
    boolean hasExtraFilter;
    
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
        RelNode[] children,
        LcsTable lcsTable,
        List<FemLocalIndex> clusteredIndexes,
        RelOptConnection connection,
        Integer [] projectedColumns,
        boolean isFullScan,
        boolean hasExtraFilter)
    {
        super(cluster, children);
        this.lcsTable = lcsTable;
        this.clusteredIndexes = clusteredIndexes;
        this.projectedColumns = projectedColumns;
        this.connection = connection;
        this.isFullScan = isFullScan;
        this.hasExtraFilter = hasExtraFilter;

        assert (lcsTable.getPreparingStmt() ==
            FennelRelUtil.getPreparingStmt(this));
        
        repos = FennelRelUtil.getRepos(this);
    }
    
    //~ Methods ---------------------------------------------------------------
    
    // implement RelNode
    public Object clone()
    {
        LcsRowScanRel clone = 
            new LcsRowScanRel(getCluster(), RelOptUtil.clone(inputs),
                lcsTable, clusteredIndexes, connection, projectedColumns,
                isFullScan, hasExtraFilter);
        clone.inheritTraitsFrom(this);
        return clone;
    }
    
    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        return computeCost(planner, getRows());
    }

    // overwrite SingleRel
    public double getRows()
    {
        if (inputs.length == 0) {
            // full table scan.
            return lcsTable.getRowCount();
        } else {
            // table scan from an input RID stream.
            return inputs[0].getRows();
        }
    }

    // implement RelNode
    protected RelDataType deriveRowType()
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
        
        if (inputs.length == 0) {
            pw.explain(
                this,
                new String [] {"table", "projection", "clustered indexes"},
                new Object [] {
                    Arrays.asList(lcsTable.getQualifiedName()), projection,
                    indexNames});
        } else {
            pw.explain(
                this,
                new String [] {"child", "table", "projection", "clustered indexes"},
                new Object [] {
                    Arrays.asList(lcsTable.getQualifiedName()), projection,
                    indexNames});
        }
    }

    // overwrite FennelSingleRel
    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        if (inputs.length == 0) {
            return Literal.constantNull();
        } else {
            return super.implementFennelChild(implementor);
        }
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);

        FemLcsRowScanStreamDef scanStream = 
            indexGuide.newRowScan(this, projectedColumns);
        
        for (int i = 0; i < inputs.length; i++) {
            FemExecutionStreamDef inputStream =
                implementor.visitFennelChild((FennelRel) inputs[i]);
            implementor.addDataFlowFromProducerToConsumer(
                inputStream,
                scanStream);
        }

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
    public RelOptCost computeCost(
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

        RelOptCost cost = planner.makeCost(dRows, dCpu, dIo);

        if (inputs.length != 0) {
            // table scan from RID stream is less costly.
            // Once we have good cost, the calculation should be 
            // cost * (# of inputRIDs/# of totaltableRows).
            cost = cost.multiplyBy(0.1);
        }
        return cost;
    }

    /**
     * Gets the column referenced by a FieldAccess relative to this scan.
     *
     * @param columnOrdinal 0-based ordinal of an output field of the scan
     *
     * @return underlying column
     */
    public FemAbstractColumn getColumnForFieldAccess(int columnOrdinal)
    {
        assert columnOrdinal >= 0;
        if (projectedColumns != null) {
            columnOrdinal = projectedColumns[columnOrdinal].intValue();
        }
        return (FemAbstractColumn) lcsTable.getCwmColumnSet().getFeature().get(columnOrdinal);
    }

    public RelOptTable getTable()
    {
        return lcsTable;
    }

    public RelOptConnection getConnection()
    {
        return connection;
    }

    // REVIEW jvs 13-Mar-2006:  naming convention is to leave off the "get"
    // when the attribute is already boolean, so just "isFullScan()"
    // and "hasExtraFilter()"

    public boolean getIsFullScan() 
    {
        return isFullScan;
    }
        
    public boolean getHasExtraFilter() 
    {
        return hasExtraFilter;
    }
}

// End LcsRowScanRel.java
