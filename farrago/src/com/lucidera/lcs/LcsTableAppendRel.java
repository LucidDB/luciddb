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
import net.sf.farrago.cwm.keysindexes.CwmIndexedFeature;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.fem.sql2003.FemAbstractColumn;
import net.sf.farrago.query.*;
import net.sf.farrago.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * LcsTableAppendRel is the relational expression corresponding to
 * appending rows to all of the clusters of a column-store table.
 * 
 * @author Rushan Chen
 * @version $Id$
 */
public class LcsTableAppendRel
    extends MedAbstractFennelTableModRel
{
    //~ Instance fields -------------------------------------------------------

    /** Helper class to manipulate the cluster indexes. */
    private LcsIndexGuide indexGuide;
    
    /** Refinement for TableModificationRelBase.table. */
    final LcsTable lcsTable;
    
    /**
     * Constructor. Currectly only insert is supported.
     * 
     * @param cluster RelOptCluster for this rel
     * @param lcsTable target table of insert
     * @param connection connection
     * @param child input to the load
     * @param operation DML operation type
     * @param updateColumnList
     */
    public LcsTableAppendRel(
        RelOptCluster cluster,
        LcsTable lcsTable, 
        RelOptConnection connection,
        RelNode child,
        Operation operation,
        List updateColumnList)
    {
        super(cluster, new RelTraitSet(FennelRel.FENNEL_EXEC_CONVENTION),
            lcsTable, connection, child, operation, updateColumnList, true);
        
        // Only INSERT is supported currently. 
        assert (getOperation().getOrdinal()
            == TableModificationRel.Operation.INSERT_ORDINAL);

        this.lcsTable = lcsTable;
        assert lcsTable.getPreparingStmt() ==
            FennelRelUtil.getPreparingStmt(this);   
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {        
        double dInputRows = getChild().getRows();
        
        // TODO:  compute page-based I/O cost
        // CPU cost is proportional to number of columns projected
        // I/O cost is proportional to pages of clustered index to write
        double dCpu = dInputRows * getChild().getRowType().getFieldList().size();

        int nIndexCols = getIndexGuide().getNumFlattenedClusterCols();
        
        double dIo = dInputRows * nIndexCols;
        
        return planner.makeCost(dInputRows, dCpu, dIo);
        
    }

    // implement Cloneable
    public Object clone()
    {
        LcsTableAppendRel clone = new LcsTableAppendRel(
            getCluster(),
            lcsTable,
            getConnection(),
            RelOptUtil.clone(getChild()),
            getOperation(),
            getUpdateColumnList());
        clone.inheritTraitsFrom(this);
        return clone;
    }
        
    private LcsIndexGuide getIndexGuide()
    {
        if (indexGuide == null) {
            indexGuide = new LcsIndexGuide(
                lcsTable.getPreparingStmt().getFarragoTypeFactory(),
                lcsTable.getCwmColumnSet());
        }
        return indexGuide;
    }
    
    // Override TableModificationRelBase
    public void explain(RelOptPlanWriter pw)
    {        
        // TODO: 
        // make list of index names available in the verbose mode of
        // explain plan.
        pw.explain(
            this,
            new String [] {"child", "table"},
            new Object [] {Arrays.asList(lcsTable.getQualifiedName())});
    }

    private FemSplitterStreamDef newSplitter(FarragoRepos repos)
    {
        FemSplitterStreamDef splitter = repos.newFemSplitterStreamDef();
        
        splitter.setOutputDesc(
            FennelRelUtil.createTupleDescriptorFromRowType(
                repos,
                getFarragoTypeFactory(),
                getChild().getRowType()));
      return splitter;          
    }

    private FemBarrierStreamDef newBarrier(FarragoRepos repos)
    {
        FemBarrierStreamDef barrier = repos.newFemBarrierStreamDef();

        barrier.setOutputDesc(
            FennelRelUtil.createTupleDescriptorFromRowType(
            repos,
            getFarragoTypeFactory(),
            getRowType()));

        return barrier;
    }
    
    private FemLcsClusterAppendStreamDef newClusterAppend(
        FarragoRepos repos,
        FarragoPreparingStmt stmt,
        FemLocalIndex clusterIndex)
    {
        FemLcsClusterAppendStreamDef clusterAppend = 
            repos.newFemLcsClusterAppendStreamDef();
        
        //
        // Set up FemExecutionStreamDef
        //        - setOutputDesc
        //
        clusterAppend.setOutputDesc(
            FennelRelUtil.createTupleDescriptorFromRowType(
                repos,
                getFarragoTypeFactory(),
                getRowType()));

        //
        // Set up FemIndexAccessorDef
        //        - setRootPageId
        //        - setSegmentId
        //        - setTupleDesc
        //        - setKeyProj
        //
        clusterAppend.setRootPageId(
            stmt.getIndexMap().getIndexRoot(clusterIndex));
        
        clusterAppend.setSegmentId(
            LcsDataServer.getIndexSegmentId(clusterIndex));
        
        long indexId = JmiUtil.getObjectId(clusterIndex);
        
        clusterAppend.setIndexId(indexId);
        
        FemTupleDescriptor indexTupleDesc =
            indexGuide.createClusteredBTreeTupleDesc();
        clusterAppend.setTupleDesc(indexTupleDesc);
        
        //
        // The key is simply the RID from the [RID, PageId] mapping stored in
        // this index.
        //
        Integer[] keyProj ={0};
        
        clusterAppend.setKeyProj(
            FennelRelUtil.createTupleProjection(repos, keyProj));
        
        //
        // Set up FemLcsClusterAppendStreamDef
        //        - setOverwrite
        //        - setClusterColProj
        //
        clusterAppend.setOverwrite(false);
        
        Integer[] clusterColProj;
        clusterColProj =
            new Integer[indexGuide.getNumFlattenedClusterCols(clusterIndex)];
        
        //
        // Figure out the projection covering columns contained in each index.
        //
        int i = 0;
        for (Object f : clusterIndex.getIndexedFeature()) {
            CwmIndexedFeature indexedFeature = (CwmIndexedFeature) f;
            FemAbstractColumn column = 
                (FemAbstractColumn) indexedFeature.getFeature();
            int n = indexGuide.getNumFlattenedSubCols(column.getOrdinal());
            for (int j = 0; j < n; ++j) {
                clusterColProj[i] =
                    indexGuide.flattenOrdinal(column.getOrdinal()) + j;
                i++;
            }
        }
        
        clusterAppend.setClusterColProj(
            FennelRelUtil.createTupleProjection(repos, clusterColProj));
        
        return clusterAppend;
        
    }
    
    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FemExecutionStreamDef input =
            implementor.visitFennelChild((FennelRel) getChild());

        CwmTable table = (CwmTable) lcsTable.getCwmColumnSet();
        FarragoRepos repos = FennelRelUtil.getRepos(this);
       
        final FarragoPreparingStmt stmt =
            FennelRelUtil.getPreparingStmt(this);
       
        //
        // 1. Setup the SplitterStreamDef
        //
        FemSplitterStreamDef splitter = newSplitter(repos);
        
        //
        // 2. Setup all the LcsClusterAppendStreamDef's
        //    - Get all the clustered indices.
        //    - For each index, set up the corresponding clusterAppend stream
        //      def.
        //
            
        ArrayList clusterAppendDefs = new ArrayList();
        
        // Get the clustered indexes associated with this table.
        List<FemLocalIndex> clusteredIndexes =
            FarragoCatalogUtil.getClusteredIndexes(repos, table);
        
        for (FemLocalIndex clusteredIndex : clusteredIndexes) {            
            clusterAppendDefs.add(
                newClusterAppend(repos, stmt, clusteredIndex));
        }
         
        //
        // 3. Setup the BarrierStreamDef.
        //
        FemBarrierStreamDef barrier = newBarrier(repos);
        
        //
        // 4. Set up buffering if required.
        // We only need a buffer if the target table is also a source.
        //
        if (inputNeedBuffer()) {
            FemBufferingTupleStreamDef buffer = newInputBuffer(repos);
            implementor.addDataFlowFromProducerToConsumer(
                input,
                buffer);
            input = buffer;

        }
        
        //
        // 5. Link the StreamDefs together.
        //                               -> clusterAppend ->
        // input( -> buffer) -> splitter -> clusterAppend -> barrier
        //                                  ...
        //                               -> clusterAppned ->
        //
        implementor.addDataFlowFromProducerToConsumer(
            input,
            splitter);
            
        for (Object streamDef : clusterAppendDefs) {
            FemLcsClusterAppendStreamDef clusterAppend =
                (FemLcsClusterAppendStreamDef) streamDef;
            implementor.addDataFlowFromProducerToConsumer(
                splitter,
                clusterAppend);
            implementor.addDataFlowFromProducerToConsumer(
                clusterAppend,
                barrier);                
        }
        
        return barrier;
    }
}

//End LcsTableAppendRel
