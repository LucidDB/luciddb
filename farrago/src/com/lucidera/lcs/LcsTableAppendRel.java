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

        int nIndexCols = lcsTable.getIndexGuide().getNumFlattenedClusterCols();
        
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
        

    /**
     * Returns an index guide specific to an unclustered index
     */
    private LcsIndexGuide getIndexGuide(FemLocalIndex unclusteredIndex)
    {
        return new LcsIndexGuide(
            lcsTable.getPreparingStmt().getFarragoTypeFactory(),
            lcsTable.getCwmColumnSet(),
            unclusteredIndex);
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

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FemExecutionStreamDef input =
            implementor.visitFennelChild((FennelRel) getChild());

        CwmTable table = (CwmTable) lcsTable.getCwmColumnSet();
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        LcsIndexGuide indexGuide = lcsTable.getIndexGuide();
       
        //
        // 1. Setup the SplitterStreamDef
        //
        FemSplitterStreamDef splitter = indexGuide.newSplitter(this);
        
        //
        // 2. Setup all the LcsClusterAppendStreamDef's
        //    - Get all the clustered indices.
        //    - For each index, set up the corresponding clusterAppend stream
        //      def.
        //
            
        ArrayList<FemLcsClusterAppendStreamDef> clusterAppendDefs = 
        	new ArrayList<FemLcsClusterAppendStreamDef>();
        
        // Get the clustered indexes associated with this table.
        List<FemLocalIndex> clusteredIndexes =
            FarragoCatalogUtil.getClusteredIndexes(repos, table);
        
        for (FemLocalIndex clusteredIndex : clusteredIndexes) {            
            clusterAppendDefs.add(
                indexGuide.newClusterAppend(this, clusteredIndex));
        }
         
        //
        // 3. Setup the BarrierStreamDef.
        //
        FemBarrierStreamDef barrier = indexGuide.newBarrier(this);
        
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

        //
        // 6. If there are no unclustered indexes, stop at the barrier
        //
        List<FemLocalIndex> unclusteredIndexes =
            FarragoCatalogUtil.getUnclusteredIndexes(repos, table);
        if (unclusteredIndexes.size() == 0) {
            return barrier;
        }

        // Update clustered index scans
        for (Object streamDef : clusterAppendDefs) {
            FemLcsClusterAppendStreamDef clusterAppend =
                (FemLcsClusterAppendStreamDef) streamDef;
            clusterAppend.setOutputDesc(
                indexGuide.getUnclusteredInputDesc());
        }
        barrier.setOutputDesc(indexGuide.getUnclusteredInputDesc());

        //
        // 7. Setup unclustered indices.
        //    - For each index, set up the corresponding bitmap append
        //
        ArrayList<LcsCompositeStreamDef> bitmapAppendDefs = 
        	new ArrayList<LcsCompositeStreamDef>();
        
        // FIXME: we need some system of generating new param ids 
        // per graph.
        int dynParamId = 1;
        for (FemLocalIndex unclusteredIndex : unclusteredIndexes) {
            LcsIndexGuide ucxIndexGuide = getIndexGuide(unclusteredIndex);
            bitmapAppendDefs.add( 
                ucxIndexGuide.newBitmapAppend(
                	this, unclusteredIndex, implementor, false, dynParamId));
            dynParamId++;
        }
         
        //
        // 8. Setup a bitmap SplitterStreamDef
        //
        FemSplitterStreamDef bitmapSplitter = 
        	indexGuide.newSplitter(this);
        
        //
        // 9. Setup a bitmap BarrierStreamDef
        //
        FemBarrierStreamDef bitmapBarrier = 
        	indexGuide.newBarrier(this);
        
        //
        // 10. Link the bitmap StreamDefs together.
        //                     -> bitmap append streams ->
        // barrier -> splitter -> bitmap append streams -> barrier
        //                                  ...
        //                     -> bitmap append streams ->
        //

        implementor.addDataFlowFromProducerToConsumer(
            barrier,
            bitmapSplitter);

        for (Object streamDef : bitmapAppendDefs) {
            LcsCompositeStreamDef bitmapAppend =
                (LcsCompositeStreamDef) streamDef;
            implementor.addDataFlowFromProducerToConsumer(
                bitmapSplitter,
                bitmapAppend.getConsumer());
            implementor.addDataFlowFromProducerToConsumer(
                bitmapAppend.getProducer(),
                bitmapBarrier);                
        }
        
        return bitmapBarrier;
    }
}

//End LcsTableAppendRel
