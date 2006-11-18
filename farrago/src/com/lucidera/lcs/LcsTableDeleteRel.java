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
        List updateColumnList)
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

        assert (getOperation().getOrdinal()
                == TableModificationRel.Operation.DELETE_ORDINAL);

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
        FemExecutionStreamDef input =
            implementor.visitFennelChild((FennelRel) getChild());

        CwmTable table = (CwmTable) lcsTable.getCwmColumnSet();
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        LcsIndexGuide indexGuide = lcsTable.getIndexGuide();
        FemLocalIndex deletionIndex =
            FarragoCatalogUtil.getDeletionIndex(repos, table);

        // Determine whether we need to sort the input into the delete.
        // If the input is sorted on the rid column, we can bypass the sort,
        // but still need to buffer the input since the input reads from
        // the deletion index while the delete writes to it.  We know that
        // the input is sorted on the rid if the input is sorted on the first
        // field in the input, as the delete always projects the rid in the
        // first column of its input.
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
                    RelMetadataQuery.getRowCount(getChild()));
            implementor.addDataFlowFromProducerToConsumer(input, sortingStream);
            input = sortingStream;
        } else {
            FemBufferingTupleStreamDef buffer = newInputBuffer(repos);
            implementor.addDataFlowFromProducerToConsumer(input, buffer);
            input = buffer;
        }

        FemLbmSplicerStreamDef splicer =
            indexGuide.newSplicer(this, deletionIndex, 0, false);
        implementor.addDataFlowFromProducerToConsumer(input, splicer);

        FemBarrierStreamDef barrier =
            indexGuide.newBarrier(
                this,
                LcsIndexGuide.BarrierReturnFirstInput);
        implementor.addDataFlowFromProducerToConsumer(splicer, barrier);

        return barrier;
    }
}

// End LcsTableDeleteRel
