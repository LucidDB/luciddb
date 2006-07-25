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

import com.lucidera.farrago.*;
import com.lucidera.query.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.query.*;
import net.sf.farrago.type.*;

import openjava.ptree.Literal;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sarg.*;
import org.eigenbase.sql.type.*;

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
        return computeCost(planner, RelMetadataQuery.getRowCount(this));
    }

    // overwrite SingleRel
    public double getRows()
    {
        if (inputs.length == 0) {
            // full table scan.
            return lcsTable.getRowCount();
        } else {
            // table scan from an input RID stream.
            return RelMetadataQuery.getRowCount(inputs[0]);
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
                        if (LucidDbOperatorTable.ldbInstance().
                            isSpecialColumnId(i))
                        {
                            return LucidDbOperatorTable.ldbInstance().
                                getSpecialOpName(i);
                        } else {
                            return fields[i].getName();
                        }
                    }

                    public RelDataType getFieldType(int index)
                    {
                        final int i = projectedColumns[index].intValue();
                        LucidDbOperatorTable ldbInstance =
                            LucidDbOperatorTable.ldbInstance();
                        if (ldbInstance.isSpecialColumnId(i))
                        {
                            RelDataTypeFactory typeFactory =
                                getCluster().getTypeFactory();
                            SqlTypeName typeName =
                                ldbInstance.getSpecialOpRetTypeName(i);
                            return
                                typeFactory.createTypeWithNullability(
                                    typeFactory.createSqlType(typeName),
                                    ldbInstance.isNullable(i));
                        } else {
                            return fields[i].getType();
                        }
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
            Object[] modifiedProj = new Object[projectedColumns.length];
            System.arraycopy(
                projectedColumns, 0, modifiedProj, 0, projectedColumns.length);
            projection = Arrays.asList(modifiedProj);
            // replace the numbers for the special columns so they're more
            // readable
            List projList = (List) projection;
            for (int i = 0; i < projList.size(); i++) {
                Integer colId = (Integer) projList.get(i);
                if (LucidDbOperatorTable.ldbInstance().isSpecialColumnId(colId))
                {
                    projList.set(
                        i, LucidDbOperatorTable.ldbInstance().getSpecialOpName(
                            colId));
                }
            }
        }
        
        // REVIEW jvs 27-Dec-2005:  Since LcsRowScanRel is given
        // a list (implying ordering) as input, it seems to me that
        // the caller should be responsible for putting the
        // list into a deterministic order rather than doing
        // it here.
        
        TreeSet<String> indexNames = new TreeSet<String>();
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
        // modify the input to the scan to either scan the deletion index
        // (in the case of a full table scan) or to minus off the deletion
        // index (in the case of an index scan)      
        if (isFullScan) {
            RelNode[] oldInputs = new RelNode[inputs.length + 1];
            System.arraycopy(inputs, 0, oldInputs, 0, inputs.length);
            LcsIndexSearchRel delIndexScan =
                indexGuide.createDeletionIndexScan(
                    this, lcsTable, null, null, true);
            oldInputs[inputs.length] = delIndexScan;
            inputs = oldInputs;
        } else {  
            inputs[0] = indexGuide.createMinusOfDeletionIndex(
                this, lcsTable, inputs[0]);
        }
        
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
     * @return underlying column if the the column is a real one; otherwise,
     * null is returned (e.g., if the column corresponds to the rid column)
     */
    public FemAbstractColumn getColumnForFieldAccess(int columnOrdinal)
    {
        // TODO zfong 5/29/06 - The code below does not account for UDTs.
        // columnOrdinal represents a field ordinal and needs to be mapped
        // back to its unflattened column ordinal.
        assert columnOrdinal >= 0;
        if (projectedColumns != null) {
            columnOrdinal = projectedColumns[columnOrdinal].intValue();
        }    
        if (LucidDbOperatorTable.ldbInstance().isSpecialColumnId(columnOrdinal))
        {
            return null;
        } else {
            return (FemAbstractColumn) lcsTable.getCwmColumnSet().getFeature().
                get(columnOrdinal);
        }
    }
    
    /**
     * Returns the projected column ordinal for a given column ordinal,
     * relative to this scan.
     * 
     * @param origColOrdinal original column ordinal (without projection)
     * 
     * @return column ordinal corresponding to the column in the projection
     * for this scan; -1 if column is not in the projection list
     */
    public int getProjectedColumnOrdinal(int origColOrdinal)
    {
        // TODO zfong 5/29/06 - The code below does not account for UDTs.
        // origColOrdinal represents an unflattened column ordinal.  It needs
        // to be converted to a flattened ordinal.  Furthermore, the flattened
        // ordinal may map to multiple fields.
        if (projectedColumns == null) {
            return origColOrdinal;
        }
        for (int i = 0; i < projectedColumns.length; i++) {
            if (projectedColumns[i] == origColOrdinal) {
                return i;
            }
        }
        return -1;
    }

    public RelOptTable getTable()
    {
        return lcsTable;
    }

    public RelOptConnection getConnection()
    {
        return connection;
    }

    public boolean isFullScan() 
    {
        return isFullScan;
    }
        
    public boolean hasExtraFilter() 
    {
        return hasExtraFilter;
    }
    
    public RelFieldCollation[] getCollations()
    {
        // if the rid column is projected, then the scan result is sorted
        // on that column
        if (projectedColumns != null) {
            for (int i = 0; i < projectedColumns.length; i++) {
                if (LucidDbSpecialOperators.isLcsRidColumnId(
                    projectedColumns[i]))
                {
                    return new RelFieldCollation [] 
                        { new RelFieldCollation(i) };
                }
            }
        }
        return RelFieldCollation.emptyCollationArray;    
    }
}

// End LcsRowScanRel.java
