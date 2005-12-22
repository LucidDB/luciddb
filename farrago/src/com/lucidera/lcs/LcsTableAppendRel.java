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
import net.sf.farrago.fem.sql2003.FemAbstractColumn;
import net.sf.farrago.fennel.tuple.FennelStandardTypeDescriptor;
import net.sf.farrago.fennel.tuple.FennelStoredTypeDescriptor;
import net.sf.farrago.query.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * LcsTableAppendRel is the relational expression corresponding to
 * appending to a single cluster of a column storage table.
 * 
 * @author Rushan Chen
 * @version $Id$
 */
public class LcsTableAppendRel extends TableModificationRelBase implements FennelRel    
{
    //~ Instance fields -------------------------------------------------------

	/** Helper class to manipulate the cluster indexes. */
    private LcsIndexGuide indexGuide;
    
    /** Refinement for TableModificationRelBase.table. */
    final LcsTable lcsTable;
    
    /**
     * Constructor. Currectly only insert is supported.
     * 
     * @param[in] cluster RelOptCluster for this rel
     * @param[in] lcsTable target table of insert
     * @param[in] connection connection
     * @param[in] child input to the load
     * @param[in] DML operation type. 
     * @param[in] updateColumnList
     */
    public LcsTableAppendRel(RelOptCluster cluster, LcsTable lcsTable, 
        RelOptConnection connection, RelNode child,
        Operation operation, List updateColumnList)
    {
        super(cluster, new RelTraitSet(FennelRel.FENNEL_EXEC_CONVENTION),
            lcsTable, connection, child, operation, updateColumnList, true);
        this.lcsTable = lcsTable;
        assert lcsTable.getPreparingStmt() ==
            FennelRelUtil.getPreparingStmt(this);   
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        double dRows = getRows();
        // TODO:  compute page-based I/O cost
        // CPU cost is proportional to number of columns projected
        // I/O cost is proportional to pages of clustered index to write
        double dCpu = dRows * getRowType().getFieldList().size();

        int nIndexCols = getIndexGuide().getSizeOfClusteredIndexes();
        
        double dIo = dRows * nIndexCols;
        
        return planner.makeCost(dRows, dCpu, dIo);
        
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

    // implement FennelRel
    public RelOptConnection getConnection()
    {
        return connection;
    }

    // implement FennelRel
    public FarragoTypeFactory getFarragoTypeFactory()
    {
        return (FarragoTypeFactory) getCluster().getTypeFactory();
    }

    // implement FennelRel
    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        return implementor.visitChild(this, 0, getChild());
    }
    
    // implement FennelRel
    public RelFieldCollation [] getCollations()
    {
        // TODO:  say it's sorted instead.  This can be done generically for all
        // FennelRel's guaranteed to return at most one row
        return RelFieldCollation.emptyCollationArray;
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
        // 1. display cost information via a generic method
        // See issue from http://jirahost.eigenbase.org:8080/browse/FRG-8
        // 2. make list of index names available in the verbose mode of explain plan.
        pw.explain(
            this,
            new String [] {"child", "table", "operation", "flattened"},
            new Object [] {
                Arrays.asList(lcsTable.getQualifiedName()), getOperation(),
                Boolean.valueOf(true),
            });
    }

    private FemSplitterStreamDef newSplitter(FarragoRepos repos)
    {
        FemSplitterStreamDef splitter =repos.newFemSplitterStreamDef();
        
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

        FemTupleDescriptor rowCountTupleDesc = repos.newFemTupleDescriptor();
        
        FemTupleAttrDescriptor rowCountAttrDesc = repos.newFemTupleAttrDescriptor();
        FennelStoredTypeDescriptor rowCountTypeDesc =
            FennelStandardTypeDescriptor.INT_64;
                
        rowCountAttrDesc.setTypeOrdinal(
            rowCountTypeDesc.getOrdinal());
        rowCountAttrDesc.setByteLength(rowCountTypeDesc.getFixedByteCount());
        rowCountAttrDesc.setNullable(false);
    
        rowCountTupleDesc.getAttrDescriptor().add(rowCountAttrDesc);
    
        barrier.setOutputDesc(rowCountTupleDesc);

        return barrier;
    }

    private FemBufferingTupleStreamDef newBuffer(FarragoRepos repos)    
    {
        FemBufferingTupleStreamDef buffer = repos.newFemBufferingTupleStreamDef();
    
        buffer.setInMemory(false);
        buffer.setMultipass(false);
    
        buffer.setOutputDesc(
            FennelRelUtil.createTupleDescriptorFromRowType(
                repos,
                getFarragoTypeFactory(),
                getChild().getRowType()));
        return buffer;
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
        FemTupleDescriptor rowCountTupleDesc = repos.newFemTupleDescriptor();
       
        FennelStoredTypeDescriptor rowCountTypeDesc =
            FennelStandardTypeDescriptor.INT_64;
        
        FemTupleAttrDescriptor rowCountAttrDesc = repos.newFemTupleAttrDescriptor();
        rowCountAttrDesc.setTypeOrdinal(
                rowCountTypeDesc.getOrdinal());
        rowCountAttrDesc.setByteLength(rowCountTypeDesc.getFixedByteCount());
        rowCountAttrDesc.setNullable(false);
        
        rowCountTupleDesc.getAttrDescriptor().add(rowCountAttrDesc);
        clusterAppend.setOutputDesc(rowCountTupleDesc);

        //
        // Set up FemIndexAccessorDef
        //        - setRootPageId
        //        - setSegmentId
        //        - setTupleDesc
        //        - setKeyProj
        //
        clusterAppend.setRootPageId(
            stmt.getIndexMap().getIndexRoot(clusterIndex));
        
        clusterAppend.setSegmentId(LcsDataServer.getIndexSegmentId(clusterIndex));
        
        long indexId = JmiUtil.getObjectId(clusterIndex);
        
        clusterAppend.setIndexId(indexId);
        
        //
        // For LCS clustered indexes, the stored tuple is always the same:
        // [RID, PageId]; and the key is just the RID.  In Fennel,
        // both attributes are represented as 64-bit int.
        //
        FemTupleDescriptor indexTupleDesc = repos.newFemTupleDescriptor();
        
        FennelStoredTypeDescriptor indexTypeDesc =
            FennelStandardTypeDescriptor.INT_64;
        
        for (int i = 0; i < 2; ++i) {
            FemTupleAttrDescriptor indexAttrDesc = repos.newFemTupleAttrDescriptor();
            indexAttrDesc.setTypeOrdinal(
                indexTypeDesc.getOrdinal());
            indexAttrDesc.setByteLength(indexTypeDesc.getFixedByteCount());
            indexAttrDesc.setNullable(false);
            indexTupleDesc.getAttrDescriptor().add(indexAttrDesc);
        }

        clusterAppend.setTupleDesc(indexTupleDesc);
        
        //
        // The key is simply the RID from the [RID, PageId] mapping stored in
        // this index.
        //
        Integer[] keyProj ={0};
        
        clusterAppend.setKeyProj(FennelRelUtil.createTupleProjection(repos, keyProj));
        
        //
        // Set up FemLcsClusterAppendStreamDef
        //        - setOverwrite
        //        - setClusterColProj
        //
        clusterAppend.setOverwrite(false);
        
        Integer[] clusterColProj;
        clusterColProj = new Integer[clusterIndex.getIndexedFeature().size()];
        
        //
        // Figure out the projection covering columns contained in each index.
        //
        int i = 0;
        for (Object f : clusterIndex.getIndexedFeature()) {
            CwmIndexedFeature indexedFeature = (CwmIndexedFeature) f;
            FemAbstractColumn column = 
                (FemAbstractColumn) indexedFeature.getFeature();    
            clusterColProj[i] = column.getOrdinal();
            i++;
        }
        
        clusterAppend.setClusterColProj(FennelRelUtil.createTupleProjection(repos, clusterColProj));
        
        return clusterAppend;
        
    }
    
    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        assert (getOperation().getOrdinal() == TableModificationRel.Operation.INSERT_ORDINAL);
        
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
        //    - For each index, set up the corresponding clusterAppend stream def.
        //
            
        ArrayList clusterAppendDefs = new ArrayList();
        
        // Get the clustered index associated with this table. Currently we only allow 
        // loading into a Lcs table with only one clustered index defined.
        List<FemLocalIndex> clusteredIndexes =
            FarragoCatalogUtil.getClusteredIndexes(repos, table);
        
        for (FemLocalIndex clusteredIndex : clusteredIndexes) {            
            clusterAppendDefs.add(newClusterAppend(repos, stmt, clusteredIndex));
        }
         
        //
        // 3. Setup the BarrierStreamDef.
        //
        FemBarrierStreamDef barrier = newBarrier(repos);
        
        //
        // 4. Set up buffering if required.
        // We only need a buffer if the target table is also a source.
        //
        TableAccessMap tableAccessMap = new TableAccessMap(this);
        
        if (tableAccessMap.isTableAccessedForRead(lcsTable)) {
            
            FemBufferingTupleStreamDef buffer = newBuffer(repos);
                
            implementor.addDataFlowFromProducerToConsumer(
                input,
                buffer);
            
            input = buffer;

        }
        
        //
        // 5. Link the StreamDefs together.
        //
        implementor.addDataFlowFromProducerToConsumer(
            input,
            splitter);
            
        for (Object streamDef : clusterAppendDefs) {
            FemLcsClusterAppendStreamDef clusterAppend = (FemLcsClusterAppendStreamDef) streamDef;
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
