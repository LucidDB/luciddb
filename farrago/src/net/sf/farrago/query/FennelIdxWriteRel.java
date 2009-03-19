/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2007-2007 The Eigenbase Project
// Copyright (C) 2007-2007 SQLstream, Inc.
// Copyright (C) 2007-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
package net.sf.farrago.query;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fennel.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;


/**
 * FennelIdxWriteRel takes its input and writes the records into an index.
 * Currently, this class only support writes to a temporary index.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class FennelIdxWriteRel
    extends FennelSingleRel
{
    //~ Instance fields --------------------------------------------------------

    final boolean discardDuplicates;
    final boolean monotonicInserts;
    final FennelRelParamId rootPageIdParamId;
    final Integer [] indexCols;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FennelIdxWriteRel object.
     *
     * @param child input the provides the records to be written to the index
     * @param discardDuplicates whether duplicates should be discard during
     * inserts
     * @param monotonicInserts whether the records to be written are provided in
     * increasing key order and therefore can be written to the index in
     * monotonic mode
     * @param rootPageIdParamId dynamic parameter id corresponding to the
     * temporary index that will be written
     * @param indexCols projection of the index columns
     */
    public FennelIdxWriteRel(
        RelNode child,
        boolean discardDuplicates,
        boolean monotonicInserts,
        FennelRelParamId rootPageIdParamId,
        Integer [] indexCols)
    {
        super(
            child.getCluster(),
            child);
        this.discardDuplicates = discardDuplicates;
        this.monotonicInserts = monotonicInserts;
        this.rootPageIdParamId = rootPageIdParamId;
        this.indexCols = indexCols;
    }

    //~ Methods ----------------------------------------------------------------

    // implement Cloneable
    public FennelIdxWriteRel clone()
    {
        FennelIdxWriteRel clone =
            new FennelIdxWriteRel(
                getChild().clone(),
                discardDuplicates,
                monotonicInserts,
                rootPageIdParamId,
                indexCols);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        double dInputRows = RelMetadataQuery.getRowCount(getChild());

        // CPU cost is proportional to number of columns projected
        // I/O cost is proportional to pages of index scanned
        double dCpu =
            dInputRows * getChild().getRowType().getFieldList().size();
        double dIo = dInputRows * indexCols.length;

        return planner.makeCost(dInputRows, dCpu, dIo);
    }

    // implement RelNode
    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String[] {
                "child",
                "discardDuplicates",
                "monotonicInserts",
                "rootPageIdParamId",
                "indexCols"
            },
            new Object[] {
                Boolean.valueOf(discardDuplicates),
                Boolean.valueOf(monotonicInserts),
                rootPageIdParamId,
                Arrays.asList(indexCols)
            });
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);

        FemIndexLoaderDef indexWriter = repos.newFemIndexLoaderDef();
        if (discardDuplicates) {
            indexWriter.setDistinctness(DistinctnessEnum.DUP_DISCARD);
        } else {
            indexWriter.setDistinctness(DistinctnessEnum.DUP_ALLOW);
        }
        indexWriter.setMonotonic(monotonicInserts);
        FemTupleDescriptor tupleDesc =
            FennelRelUtil.createTupleDescriptorFromRowType(
                repos,
                getCluster().getTypeFactory(),
                getChild().getRowType());
        indexWriter.setTupleDesc(tupleDesc);
        indexWriter.setKeyProj(
            FennelRelUtil.createTupleProjection(repos, indexCols));

        // Indicate that the rootPageId parameter is produced by this stream
        indexWriter.setRootPageIdParamId(
            implementor.translateParamId(
                rootPageIdParamId,
                indexWriter,
                FennelDynamicParamId.StreamType.PRODUCER).intValue());
        indexWriter.setRootPageId(-1);
        indexWriter.setReadOnlyCommittedData(false);

        implementor.addDataFlowFromProducerToConsumer(
            implementor.visitFennelChild((FennelRel) getChild(), 0),
            indexWriter);

        return indexWriter;
    }
}

// End FennelIdxWriteRel.java
