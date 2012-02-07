/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package org.luciddb.lcs;

import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fennel.rel.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * LcsIndexBitOpRel is a base class for implementing bit operation streams with
 * 2 or more inputs
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public abstract class LcsIndexBitOpRel
    extends FennelMultipleRel
{
    //~ Instance fields --------------------------------------------------------

    final LcsTable lcsTable;
    FennelRelParamId startRidParamId;
    FennelRelParamId rowLimitParamId;

    //~ Constructors -----------------------------------------------------------

    public LcsIndexBitOpRel(
        RelOptCluster cluster,
        RelNode [] inputs,
        LcsTable lcsTable,
        FennelRelParamId startRidParamId,
        FennelRelParamId rowLimitParamId)
    {
        super(cluster, inputs);
        assert (inputs.length > 1);

        this.lcsTable = lcsTable;

        // These two parameters are used to communicate with upstream producers
        // to optimize the number of rows to be fetched, and from which point in
        // the RID sequence.
        this.startRidParamId = startRidParamId;
        this.rowLimitParamId = rowLimitParamId;
    }

    //~ Methods ----------------------------------------------------------------

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
        String [] names = new String[inputs.length + 2];

        for (int i = 0; i < inputs.length; i++) {
            names[i] = "child#" + i;
        }
        names[inputs.length] = "startRidParamId";
        names[inputs.length + 1] = "rowLimitParamId";
        pw.explain(
            this,
            names,
            new Object[] {
                (startRidParamId == null) ? (Integer) 0 : startRidParamId,
                (rowLimitParamId == null) ? (Integer) 0 : rowLimitParamId
            });
    }

    // implement RelNode
    protected RelDataType deriveRowType()
    {
        assert (getInputs().length >= 1);
        return getInput(0).getRowType();
    }

    /**
     * Connects the children input streams to the parent bit operation stream
     *
     * @param implementor implementor for the bit operation stream
     * @param bitOpStream the bit operation stream
     */
    protected void setBitOpChildStreams(
        FennelRelImplementor implementor,
        FemLbmBitOpStreamDef bitOpStream)
    {
        for (int i = 0; i < inputs.length; i++) {
            FemExecutionStreamDef inputStream =
                implementor.visitFennelChild((FennelRel) inputs[i], i);
            implementor.addDataFlowFromProducerToConsumer(
                inputStream,
                bitOpStream);
        }
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

// End LcsIndexBitOpRel.java
