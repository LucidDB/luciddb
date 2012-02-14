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
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fennel.rel.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;


/**
 * LcsTableDeleteRel is the relational expression corresponding to deletes from
 * a column-store table.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class LcsTableDeleteRel
    extends MedAbstractFennelTableModRel
{
    //~ Instance fields --------------------------------------------------------

    /* Refinement for TableModificationRelBase.table. */
    final LcsTable lcsTable;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructor.
     *
     * @param cluster RelOptCluster for this rel
     * @param lcsTable target table of insert
     * @param connection connection
     * @param child input to the load
     * @param operation DML operation type
     * @param updateColumnList update column list
     */
    public LcsTableDeleteRel(
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

        assert (getOperation() == TableModificationRel.Operation.DELETE);

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
        // I/O cost is proportional to the number of pages of the deletion
        //      index that need to be writen
        double dCpu =
            dInputRows * getChild().getRowType().getFieldList().size();

        double dIo = dInputRows;

        return planner.makeCost(dInputRows, dCpu, dIo);
    }

    // implement Cloneable
    public LcsTableDeleteRel clone()
    {
        LcsTableDeleteRel clone =
            new LcsTableDeleteRel(
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

        CwmTable table = (CwmTable) lcsTable.getCwmColumnSet();
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        LcsIndexGuide indexGuide = lcsTable.getIndexGuide();
        FemLocalIndex deletionIndex =
            FarragoCatalogUtil.getDeletionIndex(repos, table);

        // Determine whether we need to sort the input into the delete.
        // If the input is sorted on the rid column, we can bypass the sort.
        // If not and real snapshot support is not available, then we
        // still need to buffer the input since the input reads from
        // the deletion index while the delete writes to it.  (For that reason,
        // the sort also needs to do an early close on its producers.)  We know
        // that the input is sorted on the rid if the input is sorted on the
        // first field in the input, as the delete always projects the rid in
        // the first column of its input.
        //
        // NOTE zfong 5/23/06 - The code below only works with Fennel calc.
        // Java calc methods are not propagating collation information.
        boolean sort = true;
        RelFieldCollation [] collation =
            ((FennelRel) getChild()).getCollations();
        if ((collation.length > 0) && (collation[0].getFieldIndex() == 0)) {
            sort = false;
        }
        if (sort) {
            FemSortingStreamDef sortingStream =
                indexGuide.newSorter(
                    deletionIndex,
                    RelMetadataQuery.getRowCount(childInput),
                    true,
                    true);
            implementor.addDataFlowFromProducerToConsumer(input, sortingStream);
            input = sortingStream;
        } else {
            if (inputNeedBuffer(childInput)) {
                FemBufferingTupleStreamDef buffer = newInputBuffer(repos);
                implementor.addDataFlowFromProducerToConsumer(input, buffer);
                input = buffer;
            }
        }

        FemLbmSplicerStreamDef splicer =
            indexGuide.newSplicer(this, deletionIndex, null, 0, 0, false);
        implementor.addDataFlowFromProducerToConsumer(input, splicer);

        FemBarrierStreamDef barrier =
            indexGuide.newBarrier(
                getRowType(),
                BarrierReturnModeEnum.BARRIER_RET_ANY_INPUT,
                0);
        implementor.addDataFlowFromProducerToConsumer(splicer, barrier);

        return barrier;
    }
}

// End LcsTableDeleteRel.java
