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

import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;

/**
 * LcsIndexIntersectRel is a relation for intersecting the results of two index
 * scans.
 * The input to this relation must be more than one.
 *
 * @author John Pham
 * @version $Id$
 */
class LcsIndexIntersectRel extends FennelMultipleRel
{
    //~ Instance fields -------------------------------------------------------
    
    final LcsTable lcsTable;
    FennelRelParamId startRidParamId;
    FennelRelParamId rowLimitParamId;
    
    //~ Constructors ----------------------------------------------------------
    
    /**
     * Creates a new LcsIndexIntersectRel object.
     *
     * @param indexSearchRel the input to this merge
     */
    public LcsIndexIntersectRel(
        RelOptCluster cluster,
        RelNode[] inputs,
        LcsTable lcsTable,
        FennelRelParamId startRidParamId,
        FennelRelParamId rowLimitParamId)
    {
        super (cluster, inputs);
        assert (inputs.length > 1);
        
        this.lcsTable = lcsTable;
        
        // These two parameters are used to communicate with upstream producers
        // to optimize the number of rows to be fetched, and from which point
        // in the RID sequence.
        this.startRidParamId = startRidParamId;
        this.rowLimitParamId = rowLimitParamId;
    }
    
    //~ Methods ---------------------------------------------------------------
    
    // implement Cloneable
    public Object clone()
    {
        return new LcsIndexIntersectRel(
            getCluster(),
            RelOptUtil.clone(getInputs()),
            lcsTable,
            startRidParamId,
            rowLimitParamId);
    }
    
    // implement RelNode
    public double getRows()
    {
        // get the minimum number of rows across the children and then make
        // the cost inversely proportional to the number of children
        double minChildRows = 0;
        for (int i = 0; i < inputs.length; i++) {
            if (minChildRows == 0 || inputs[i].getRows() < minChildRows) {
                minChildRows = RelMetadataQuery.getRowCount(inputs[i]);
            }
        }
        return minChildRows / inputs.length;
    }
    
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        double dRows = getRows();
        
        // TODO:  compute page-based I/O cost
        // CPU cost is proportional to number of columns projected
        // I/O cost is proportional to pages of index scanned
        double dCpu = dRows * getRowType().getFieldList().size();
        
        // [RID, bitmapfield1, bitmapfield2]
        int nIndexCols = 3;
        
        double dIo = dRows * nIndexCols;
        
        return planner.makeCost(dRows, dCpu, dIo);
    }
    
    // implement RelNode
    public void explain(RelOptPlanWriter pw)
    {
        String[] names = new String[inputs.length + 2];
        
        for(int i = 0; i < inputs.length; i++) {
            names[i] = "child#" + i;
        }
        names[inputs.length] = "startRidParamId";
        names[inputs.length + 1] = "rowLimitParamId";
        pw.explain(
            this, names, new Object[] { 
                (startRidParamId == null) ? (Integer)0 : startRidParamId,
                (rowLimitParamId == null) ? (Integer)0 : rowLimitParamId });
    }

    // implement RelNode
    protected RelDataType deriveRowType()
    {
        assert (getInputs().length >= 1);
        return getInput(0).getRowType();
    }
    
    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FemLbmIntersectStreamDef intersectStream = 
            lcsTable.getIndexGuide().newBitmapIntersect(
                implementor.translateParamId(startRidParamId),
                implementor.translateParamId(rowLimitParamId));
        
        for (int i = 0; i < inputs.length; i++) {
            FemExecutionStreamDef inputStream =
                implementor.visitFennelChild((FennelRel) inputs[i]);
            implementor.addDataFlowFromProducerToConsumer(
                inputStream,
                intersectStream);
        }
        
        return intersectStream;
        
    }
    
    public FennelRelParamId getStartRidParamId()
    {
        return startRidParamId;
    }
    
    public FennelRelParamId getRowLimitParamId()
    {
        return rowLimitParamId;
    }
}

//End LcsIndexIntersectRel.java
