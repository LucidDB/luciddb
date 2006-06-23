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
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.type.*;

/**
 * LcsTableMergeRel is the relational expression corresponding to
 * merges on a column-store table.
 * 
 * @author Zelaine Fong
 * @version $Id$
 */
public class LcsTableMergeRel extends MedAbstractFennelTableModRel
{
    //~ Instance fields -------------------------------------------------------
    
    /* Refinement for TableModificationRelBase.table. */
    final LcsTable lcsTable;
    
    /**
     * Constructor.
     * 
     * @param cluster RelOptCluster for this rel
     * @param lcsTable target table of merge
     * @param connection connection
     * @param child input to the merge
     * @param operation DML operation type
     * @param updateColumnList update column list in the update substatement
     */
    public LcsTableMergeRel(
        RelOptCluster cluster,
        LcsTable lcsTable, 
        RelOptConnection connection,
        RelNode child,
        Operation operation,
        List updateColumnList)
    {
        super(cluster, new RelTraitSet(FennelRel.FENNEL_EXEC_CONVENTION),
            lcsTable, connection, child, operation, updateColumnList, true);
        
        assert (getOperation().getOrdinal()
            == TableModificationRel.Operation.MERGE_ORDINAL);
        
        this.lcsTable = lcsTable;
        assert lcsTable.getPreparingStmt() ==
            FennelRelUtil.getPreparingStmt(this);   
    }
    
    //~ Methods ---------------------------------------------------------------
    
    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {        
        double dInputRows = RelMetadataQuery.getRowCount(getChild());
        
        // TODO:  compute page-based I/O cost
        // CPU cost is proportional to number of columns projected
        // I/O cost is proportional to pages of clustered index to write plus
        //      some proportion of the deletion pages that need to be
        //      written -- assume 1/2 the number of rows
        double dCpu =
            dInputRows * getChild().getRowType().getFieldList().size();
        int nIndexCols = lcsTable.getIndexGuide().getNumFlattenedClusterCols();
        
        double dIo = dInputRows * nIndexCols + dInputRows / 2;

        return planner.makeCost(dInputRows, dCpu, dIo);       
    }
    
    // implement Cloneable
    public Object clone()
    {
        LcsTableMergeRel clone = new LcsTableMergeRel(
            getCluster(),
            lcsTable,
            getConnection(),
            RelOptUtil.clone(getChild()),
            getOperation(),
            getUpdateColumnList());
        clone.inheritTraitsFrom(this);
        return clone;
    }
    
    // Override TableModificationRelBase
    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String [] {"child", "table"},
            new Object [] {Arrays.asList(lcsTable.getQualifiedName())});
    }
    
    // override RelNode
    public RelDataType getExpectedInputRowType(int ordinalInParent)
    {
        // input consists of the bitmap entry (rid, descriptor, segment) 
        // followed by the fields of the table
        assert(ordinalInParent == 0);
        RelDataType rowType =
            getCluster().getTypeFactory().createJoinType(
                new RelDataType [] {
                    createBitmapEntryRowType(true),
                    table.getRowType()
                });
        return rowType;
    }
    
    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FennelRel childFennelRel = (FennelRel) getChild();
        FemExecutionStreamDef input =
            implementor.visitFennelChild(childFennelRel);
        
        CwmTable table = (CwmTable) lcsTable.getCwmColumnSet();
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        LcsIndexGuide indexGuide = lcsTable.getIndexGuide();
        FemLocalIndex deletionIndex =
            FarragoCatalogUtil.getDeletionIndex(repos, table);
        
        // Setup the execution stream as follows:
        //
        //                              Reshape -------> Insert
        //                                 /                 \
        // ChildInput -> Buffer -> Splitter                Barrier
        //                                 \                 /
        //                              Reshape -> Sort -> Delete
        
        FemBufferingTupleStreamDef buffer = newInputBuffer(repos);
        implementor.addDataFlowFromProducerToConsumer(input,  buffer);
        
        FemSplitterStreamDef splitter =
            indexGuide.newSplitter((SingleRel) childFennelRel);
        implementor.addDataFlowFromProducerToConsumer(buffer, splitter);
        
        // create the delete substream
        FemReshapeStreamDef deleteReshape =
            newMergeReshape(true, childFennelRel);
        implementor.addDataFlowFromProducerToConsumer(splitter, deleteReshape);
        FemSortingStreamDef sortingStream =
            indexGuide.newSorter(deletionIndex);
        implementor.addDataFlowFromProducerToConsumer(
            deleteReshape, sortingStream);       
        FemLbmSplicerStreamDef deleter = 
            indexGuide.newSplicer(this, deletionIndex, 0);
        implementor.addDataFlowFromProducerToConsumer(sortingStream, deleter);
        
        // create the insert substream
        FemReshapeStreamDef insertReshape =
            newMergeReshape(false, childFennelRel);
        implementor.addDataFlowFromProducerToConsumer(splitter, insertReshape); 
        LcsAppendStreamDef appendStreamDef =
            new LcsAppendStreamDef(repos, lcsTable, insertReshape, this);
        FemExecutionStreamDef appendSubStream =
            appendStreamDef.toStreamDef(implementor);
        
        FemBarrierStreamDef barrier = indexGuide.newBarrier(this, 1);
        implementor.addDataFlowFromProducerToConsumer(deleter, barrier);
        implementor.addDataFlowFromProducerToConsumer(appendSubStream, barrier);
        
        return barrier;
    }
    
    /**
     * Creates a Reshape execution stream for either the delete or insert
     * portion of the overall MERGE execution stream
     * 
     * @param compareRid true if the stream will be doing rid comparison
     * @param input input that will be reshaped
     * 
     * @return created Reshape execution stream
     */
    FemReshapeStreamDef newMergeReshape(boolean compareRid, FennelRel input)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemReshapeStreamDef reshape = repos.newFemReshapeStreamDef();
        RelDataTypeField[] inputFields = input.getRowType().getFields();
        Integer[] outputProj;
        RelDataType outputRowType;
        RexBuilder rexBuilder = getCluster().getRexBuilder();
        RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
        
        if (compareRid) {
            // In the comparison case, create a filter on the rid column
            // to filter out nulls, and project out the rid column and the
            // segment and descriptor fields that follow it
            reshape.setCompareOp(CompOperatorEnum.COMP_NE);
            outputProj = FennelRelUtil.newIotaProjection(3);
            Integer[] compareProj = { 0 };
            reshape.setInputCompareProjection(
                FennelRelUtil.createTupleProjection(repos, compareProj));
            
            // create a tuple containing a single null value and convert
            // it to a base-64 string
            List<RexNode> tuple = new ArrayList<RexNode>();
            tuple.add(rexBuilder.constantNull());
            List<List<RexNode>> compareTuple = new ArrayList<List<RexNode>>();
            compareTuple.add(tuple);
            RelDataType ridType = typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.Bigint), true);
            RelDataType ridRowType =
                typeFactory.createStructType(
                    new RelDataType [] { ridType },
                    new String [] { "rid" });
            reshape.setTupleCompareBytesBase64(
                FennelRelUtil.convertTuplesToBase64String(
                    ridRowType, (List) compareTuple));
            
            // rid needs to not be nullable in the output since the delete
            // (i.e., splicer) expects non-null rid values
            outputRowType = createBitmapEntryRowType(false);
        } else {
            // In the non-comparsion case, project all except the first 3
            // columns, which correspond to the rid and the descriptor and
            // segment
            reshape.setCompareOp(CompOperatorEnum.COMP_NOOP);
            int nNonRidFields = inputFields.length - 3;
            outputProj = FennelRelUtil.newBiasedIotaProjection(
                nNonRidFields, 3);  
            
            RelDataType[] nonRidFieldTypes = new RelDataType[nNonRidFields];
            String[] fieldNames = new String[nNonRidFields];
            for (int i = 0; i < nNonRidFields; i++) {
                nonRidFieldTypes[i] = inputFields[i + 3].getType();
                fieldNames[i] = inputFields[i + 3].getName();
            }
            outputRowType =
                typeFactory.createStructType(nonRidFieldTypes, fieldNames);
        }
        
        reshape.setOutputProjection(
            FennelRelUtil.createTupleProjection(repos, outputProj));
        reshape.setOutputDesc(
            FennelRelUtil.createTupleDescriptorFromRowType(
                repos, typeFactory, outputRowType));
        
        return reshape;
    }
    
    /**
     * Creates a rowtype corresponding to a bitmap entry.  It consists of
     * a rid followed by 2 varbinary's corresponding to the segment and
     * descriptor in a bitmap entry.
     * 
     * @param nullableRid if true, create the rid type column as nullable
     * 
     * @return created rowtype
     */
    private RelDataType createBitmapEntryRowType(boolean nullableRid)
    {
        RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        RelDataType varBinaryType = typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(
                SqlTypeName.Varbinary, LcsIndexGuide.LbmBitmapSegMaxSize),
                true);
        RelDataType ridType;
        if (nullableRid) {
            ridType = typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.Bigint), true);
        } else {
            ridType = typeFactory.createSqlType(SqlTypeName.Bigint);
        }
        RelDataType rowType =
            typeFactory.createStructType(
                new RelDataType [] { ridType, varBinaryType, varBinaryType },
                new String [] { "rid", "descriptor", "segment" });
        
        return rowType;
    }
}

// End LcsTableMergeRel
