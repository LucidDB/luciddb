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
import net.sf.farrago.fennel.tuple.FennelStandardTypeDescriptor;
import net.sf.farrago.fennel.tuple.FennelStoredTypeDescriptor;
import net.sf.farrago.query.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.util.*;

/**
 * LcsClusterAppendRel is the relational expression corresponding to
 * appending to a single cluster of a column storage table.
 * 
 * @author Rushan Chen
 * @version $Id$
 */
public class LcsClusterAppendRel extends TableModificationRelBase implements FennelRel    
{
    //~ Instance fields -------------------------------------------------------

    /** Refinement for TableModificationRelBase.table. */
    final LcsTable lcsTable;

    /**
     * @param cluster
     * @param traits
     * @param table
     * @param connection
     * @param child
     * @param operation
     * @param updateColumnList
     * @param flattened
     */
    public LcsClusterAppendRel(RelOptCluster cluster, LcsTable lcsTable, 
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
        // TODO:  the real thing
        return planner.makeTinyCost();
    }

    // implement Cloneable
    public Object clone()
    {
        LcsClusterAppendRel clone = new LcsClusterAppendRel(
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

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        assert (getOperation().getOrdinal() == TableModificationRel.Operation.INSERT_ORDINAL);
        
        FemExecutionStreamDef input =
            implementor.visitFennelChild((FennelRel) getChild());

        CwmTable table = (CwmTable) lcsTable.getCwmColumnSet();
        FarragoRepos repos = FennelRelUtil.getRepos(this);

        FemLcsClusterAppendStreamDef clusterAppendDef = repos.newFemLcsClusterAppendStreamDef();

        // Get the clustered index associated with this table. Currently we only allow 
        // loading into a Lcs table with only one clustered index defined.
        FemLocalIndex clusterIndex =
            FarragoCatalogUtil.getClusteredIndex(repos, table);
        
        final FarragoPreparingStmt stmt =
            FennelRelUtil.getPreparingStmt(this);
       
        // Set up the IndexStreamDef(IndexAccessorDef) that LcsClusterAppendStreamDef derives from.
        clusterAppendDef.setRootPageId(
            stmt.getIndexMap().getIndexRoot(clusterIndex));
        
        clusterAppendDef.setSegmentId(LcsDataServer.getIndexSegmentId(clusterIndex));
        
        long indexId = JmiUtil.getObjectId(clusterIndex);
        
        clusterAppendDef.setIndexId(indexId);
        
        // For LCS clustered indexes, the stored tuple is always the same:
        // [RID, PageId]; and the key is just the RID.  In Fennel,
        // both attributes are represented as 64-bit ints.
        FemTupleDescriptor tupleDesc = repos.newFemTupleDescriptor();
        
        FennelStoredTypeDescriptor typeDesc =
            FennelStandardTypeDescriptor.INT_64;
        
        for (int i = 0; i < 2; ++i) {
            FemTupleAttrDescriptor attrDesc = repos.newFemTupleAttrDescriptor();
            tupleDesc.getAttrDescriptor().add(attrDesc);
            attrDesc.setTypeOrdinal(
                typeDesc.getOrdinal());
            attrDesc.setByteLength(typeDesc.getFixedByteCount());
            attrDesc.setNullable(false);
        }

        clusterAppendDef.setTupleDesc(tupleDesc);
        
        // The key is simply the RID from the [RID, PageId] mapping stored in this index.
        Integer[] keyProj ={0};
        
        clusterAppendDef.setKeyProj(FennelRelUtil.createTupleProjection(repos, keyProj));
        
        // setup FemLcsClusterAppendStreamDef
        // Do not over write.
        clusterAppendDef.setOverwrite(false);
        
        // Set up buffering.
        // We only need a buffer if the target table is
        // also a source.
        TableAccessMap tableAccessMap = new TableAccessMap(this);
        
        if (tableAccessMap.isTableAccessedForRead(lcsTable)) {
            FemBufferingTupleStreamDef buffer =
                repos.newFemBufferingTupleStreamDef();
            buffer.setInMemory(false);
            buffer.setMultipass(false);
            buffer.setOutputDesc(
                FennelRelUtil.createTupleDescriptorFromRowType(
                    repos,
                    getFarragoTypeFactory(),
                    getChild().getRowType()));

            implementor.addDataFlowFromProducerToConsumer(
                input,
                buffer);

            implementor.addDataFlowFromProducerToConsumer(
                buffer,
                clusterAppendDef);
        } else {
            implementor.addDataFlowFromProducerToConsumer(
                input,
                clusterAppendDef);
        }
        return clusterAppendDef;
    }
    
}


//End LcsClusterAppendRel
