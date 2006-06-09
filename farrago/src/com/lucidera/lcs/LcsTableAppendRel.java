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
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
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
        double dInputRows = RelMetadataQuery.getRowCount(getChild());
        
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
        
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        
        if (inputNeedBuffer()) {
            FemBufferingTupleStreamDef buffer = newInputBuffer(repos);
            implementor.addDataFlowFromProducerToConsumer(
                input,
                buffer);
            input = buffer;
        }
        
        LcsAppendStreamDef appendStreamDef =
            new LcsAppendStreamDef(
                repos,
                lcsTable, 
                input,
                this);
        
        return appendStreamDef.toStreamDef(implementor);
    }
}

//End LcsTableAppendRel
