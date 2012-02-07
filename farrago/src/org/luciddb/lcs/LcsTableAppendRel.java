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

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fennel.rel.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;


/**
 * LcsTableAppendRel is the relational expression corresponding to appending
 * rows to all of the clusters of a column-store table.
 *
 * @author Rushan Chen
 * @version $Id$
 */
public class LcsTableAppendRel
    extends MedAbstractFennelTableModRel
{
    //~ Instance fields --------------------------------------------------------

    /**
     * Refinement for TableModificationRelBase.table.
     */
    final LcsTable lcsTable;

    //~ Constructors -----------------------------------------------------------

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
        List<String> updateColumnList)
    {
        super(
            cluster,
            new RelTraitSet(FennelRel.FENNEL_EXEC_CONVENTION),
            lcsTable,
            connection,
            child,
            operation,
            updateColumnList,
            true);

        // Only INSERT is supported currently.
        assert (getOperation() == TableModificationRel.Operation.INSERT);

        this.lcsTable = lcsTable;
        assert lcsTable.getPreparingStmt()
            == FennelRelUtil.getPreparingStmt(this);
    }

    //~ Methods ----------------------------------------------------------------

    public LcsTable getLcsTable()
    {
        return lcsTable;
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        double dInputRows = RelMetadataQuery.getRowCount(getChild());

        // TODO:  compute page-based I/O cost
        // CPU cost is proportional to number of columns projected
        // I/O cost is proportional to pages of clustered index to write
        double dCpu =
            dInputRows * getChild().getRowType().getFieldList().size();

        int nIndexCols = lcsTable.getIndexGuide().getNumFlattenedClusterCols();

        double dIo = dInputRows * nIndexCols;

        return planner.makeCost(dInputRows, dCpu, dIo);
    }

    // implement Cloneable
    public LcsTableAppendRel clone()
    {
        LcsTableAppendRel clone =
            new LcsTableAppendRel(
                getCluster(),
                lcsTable,
                getConnection(),
                getChild().clone(),
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
            new String[] { "child", "table" },
            new Object[] { Arrays.asList(lcsTable.getQualifiedName()) });
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        RelNode childInput = getChild();
        FemExecutionStreamDef input =
            implementor.visitFennelChild((FennelRel) childInput, 0);

        FarragoRepos repos = FennelRelUtil.getRepos(this);

        if (inputNeedBuffer(childInput)) {
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
                this,
                RelMetadataQuery.getRowCount(childInput));

        // create the top half of the insertion stream
        FemBarrierStreamDef clusterAppendBarrier =
            appendStreamDef.createClusterAppendStreams(implementor);

        // if there are clustered indexes, create the bottom half of the
        // insertion stream; otherwise, just return the cluster append barrier
        return appendStreamDef.createBitmapAppendStreams(
            implementor,
            clusterAppendBarrier,
            0);
    }
}

// End LcsTableAppendRel.java
