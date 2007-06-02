/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Copyright (C) 2005-2007 The Eigenbase Project
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
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.type.*;


/**
 * LcsTableMergeRel is the relational expression corresponding to merges on a
 * column-store table.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class LcsTableMergeRel
    extends MedAbstractFennelTableModRel
{
    //~ Enums ------------------------------------------------------------------

    enum ReshapeProjectionType
    {
        PROJECT_RID, PROJECT_NONRIDS, PROJECT_ALL
    }

    //~ Instance fields --------------------------------------------------------

    private boolean updateOnly;
    private boolean insertOnly;
    private Double estimatedNumRows;

    /* Refinement for TableModificationRelBase.table. */
    final LcsTable lcsTable;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructor.
     *
     * @param cluster RelOptCluster for this rel
     * @param lcsTable target table of merge
     * @param connection connection
     * @param child input to the merge
     * @param operation DML operation type
     * @param updateColumnList update column list in the update substatement
     * @param updateOnly merge only consists of an insert
     */
    public LcsTableMergeRel(
        RelOptCluster cluster,
        LcsTable lcsTable,
        RelOptConnection connection,
        RelNode child,
        Operation operation,
        List<String> updateColumnList,
        boolean updateOnly)
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

        assert (getOperation() == TableModificationRel.Operation.MERGE);

        this.lcsTable = lcsTable;
        assert lcsTable.getPreparingStmt()
            == FennelRelUtil.getPreparingStmt(this);
        this.updateOnly = updateOnly;
        insertOnly = updateColumnList.size() == 0;
    }

    //~ Methods ----------------------------------------------------------------

    public LcsTable getLcsTable()
    {
        return lcsTable;
    }

    public boolean getUpdateOnly()
    {
        return updateOnly;
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        double dInputRows = RelMetadataQuery.getRowCount(getChild());

        // TODO:  compute page-based I/O cost
        // CPU cost is proportional to number of columns projected
        // I/O cost is proportional to pages of clustered index to write plus
        //      some proportion of the deletion pages that need to be
        //      written -- assume 1/2 the number of rows
        double dCpu =
            dInputRows * getChild().getRowType().getFieldList().size();
        int nIndexCols = lcsTable.getIndexGuide().getNumFlattenedClusterCols();

        double dIo = (dInputRows * nIndexCols) + (dInputRows / 2);

        return planner.makeCost(dInputRows, dCpu, dIo);
    }

    // implement Cloneable
    public LcsTableMergeRel clone()
    {
        LcsTableMergeRel clone =
            new LcsTableMergeRel(
                getCluster(),
                lcsTable,
                getConnection(),
                getChild().clone(),
                getOperation(),
                getUpdateColumnList(),
                updateOnly);
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

    // override RelNode
    public RelDataType getExpectedInputRowType(int ordinalInParent)
    {
        // When there is both an UPDATE and INSERT substatement, the input
        // consists of a rid followed by the fields of the table.  If there is
        // only an INSERT, then only the fields of the table are in the input.
        // If there is only an UPDATE, then the rid is not nullable.
        assert (ordinalInParent == 0);
        RelDataType rowType;
        if (insertOnly) {
            rowType = table.getRowType();
        } else {
            rowType =
                getCluster().getTypeFactory().createJoinType(
                    new RelDataType[] {
                        createRidRowType(!updateOnly),
                        table.getRowType()
                    });
        }
        return rowType;
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FennelRel childFennelRel = (FennelRel) getChild();
        FemExecutionStreamDef childInput =
            implementor.visitFennelChild(childFennelRel);

        CwmTable table = (CwmTable) lcsTable.getCwmColumnSet();
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        LcsIndexGuide indexGuide = lcsTable.getIndexGuide();

        // Setup the execution stream as follows:
        //
        //          ChildInput
        //              |
        //         SourceBuffer
        //              |
        //          RidSplitter
        //          /        \
        //       NotNull     Null
        //       Reshape    Reshape
        //          |           |
        //          |     NullRidBuffer
        //          \         /
        //             Merge
        //               |
        //       InsertDeleteSplitter
        //           /        \
        //     InsertReshape DeleteReshape
        //          |           |
        //          |        Distinct
        //          |           |
        //     ClusterAppends  Sort
        //          |           |
        //           \        Delete
        //            \        /
        //          InsertBarrier
        //                |
        //          Bitmap Appends
        //
        // 1) SourceBuffer is used to buffer the source rows so reading of the
        //    target rows doesn't conflict with writing the target table.
        // 2) The RidSplitter, NotNullReshape, NullReshape, NullRidBuffer,
        //    Merge substream is used to rearrange the order of the source rows
        //    so rows with null target rids (corresponding to new insert rows)
        //    appear afterrows with non-null target rids (corresponding to
        //    update rows).  NotNullReshape filters on rows where the target
        //    rids are non-null while NullReshape filters on rows where the
        //    target rids are null.  The NullRidBuffer is needed to buffer up
        //    the rows with null rids values so all rows with non-null rid
        //    values can be produced by the Merge before the null ones.
        // 3) The substream starting at InsertDeleteSplitter splits the source
        //    rows so deletion rids are passed to the left and insert rows
        //    (both new rows and modified rows) are passed to the right.
        // 4) If this is an INSERT-only or UPDATE-only merge, then the
        //    substream described in #2 above is omitted.
        // 5) If this is an INSERT-only merge, then the substream described in
        //    #3 above omits the deletion substream.

        FemBufferingTupleStreamDef sourceBuffer = newInputBuffer(repos);
        implementor.addDataFlowFromProducerToConsumer(childInput, sourceBuffer);

        // if MERGE with both insert and update, create the substream to
        // separate the non-null rids from the null rids; then connect that
        // to the splitter that separates the insert and delete substreams
        FemSplitterStreamDef insertDeleteSplitter;
        FemExecutionStreamDef insertDeleteProducer;
        if (insertOnly) {
            insertDeleteSplitter = null;
            insertDeleteProducer = sourceBuffer;
        } else {
            FemExecutionStreamDef splitterInput;
            if (updateOnly) {
                splitterInput = sourceBuffer;
            } else {
                splitterInput =
                    createRidSplitterStream(
                        implementor,
                        indexGuide,
                        childFennelRel,
                        sourceBuffer);
            }
            insertDeleteSplitter =
                indexGuide.newSplitter(childFennelRel.getRowType());
            implementor.addDataFlowFromProducerToConsumer(
                splitterInput,
                insertDeleteSplitter);
            insertDeleteProducer = insertDeleteSplitter;
        }

        // create the cluster append substream
        FemExecutionStreamDef appendProducer;
        if (insertOnly) {
            appendProducer = insertDeleteProducer;
        } else {
            FemReshapeStreamDef insertReshape =
                createMergeReshape(
                    false,
                    CompOperatorEnum.COMP_NOOP,
                    ReshapeProjectionType.PROJECT_NONRIDS,
                    childFennelRel);
            implementor.addDataFlowFromProducerToConsumer(
                insertDeleteProducer,
                insertReshape);
            appendProducer = insertReshape;
        }
        estimatedNumRows = RelMetadataQuery.getRowCount(getChild());
        if ((estimatedNumRows != null) && !updateOnly) {
            // TODO zfong 9/7/06 - In the case of a non-update only
            // merge, arbitrarily assume that half the source rows are
            // to be updated and half are inserted.  We can get a
            // better estimate by converting the source join from an
            // outer join to an inner join and getting the rowcount
            // from that inner join.
            estimatedNumRows = estimatedNumRows / 2;
        }
        LcsAppendStreamDef appendStreamDef =
            new LcsAppendStreamDef(
                repos,
                lcsTable,
                appendProducer,
                this,
                estimatedNumRows);
        FemBarrierStreamDef clusterAppends =
            appendStreamDef.createClusterAppendStreams(implementor);

        // create the delete substream, unless this is an INSERT only merge
        int writeRowCountParamId = 0;
        FemLbmSplicerStreamDef deleter = null;
        if (!insertOnly) {
            FennelRelParamId fennelParamId = implementor.allocateRelParamId();
            writeRowCountParamId =
                implementor.translateParamId(fennelParamId).intValue();
            deleter =
                createDeleteStream(
                    repos,
                    implementor,
                    indexGuide,
                    appendStreamDef,
                    childFennelRel,
                    insertDeleteSplitter,
                    writeRowCountParamId);
        }

        // create the barrier that reconnects the cluster appends and the
        // delete; if there are no indexes, then this is the final barrier,
        // in which case the barrier needs to be setup to receive the
        // deletion rowcount
        FemBarrierStreamDef insertBarrier;
        if (insertOnly) {
            insertBarrier = clusterAppends;
        } else {
            RelDataType barrierOutputType;
            int dynParam;
            if (FarragoCatalogUtil.getUnclusteredIndexes(repos, table).size()
                > 0)
            {
                barrierOutputType = indexGuide.getUnclusteredInputType();
                dynParam = 0;
            } else {
                barrierOutputType = getRowType();
                dynParam = writeRowCountParamId;
            }
            insertBarrier =
                indexGuide.newBarrier(
                    barrierOutputType,
                    BarrierReturnModeEnum.BARRIER_RET_FIRST_INPUT,
                    dynParam);
            implementor.addDataFlowFromProducerToConsumer(
                clusterAppends,
                insertBarrier);
            implementor.addDataFlowFromProducerToConsumer(
                deleter,
                insertBarrier);
        }

        // finally, create the bitmap append streams
        return appendStreamDef.createBitmapAppendStreams(
            implementor,
            insertBarrier,
            writeRowCountParamId);
    }

    /**
     * Creates a substream that takes a input containing rid values. Rearranges
     * the input such that non-null rids are written to the output of the
     * substream before the null rids.
     *
     * @param implementor FennelRel implementor
     * @param indexGuide index guide
     * @param childFennelRel FennelRel corresponding to the source input for the
     * MERGE
     * @param sourceBuffer the stream def corresponding to the input into the
     * substream to be created
     *
     * @return a MergeStreamDef that outputs the rows with rows containing
     * non-null rids appearing before rows with null rids
     */
    private FemMergeStreamDef createRidSplitterStream(
        FennelRelImplementor implementor,
        LcsIndexGuide indexGuide,
        FennelRel childFennelRel,
        FemBufferingTupleStreamDef sourceBuffer)
    {
        FemSplitterStreamDef ridSplitter =
            indexGuide.newSplitter(childFennelRel.getRowType());
        implementor.addDataFlowFromProducerToConsumer(
            sourceBuffer,
            ridSplitter);

        FemReshapeStreamDef notNullReshape =
            createMergeReshape(
                true,
                CompOperatorEnum.COMP_NE,
                ReshapeProjectionType.PROJECT_ALL,
                childFennelRel);
        implementor.addDataFlowFromProducerToConsumer(
            ridSplitter,
            notNullReshape);

        FemReshapeStreamDef nullReshape =
            createMergeReshape(
                true,
                CompOperatorEnum.COMP_EQ,
                ReshapeProjectionType.PROJECT_ALL,
                childFennelRel);
        implementor.addDataFlowFromProducerToConsumer(
            ridSplitter,
            nullReshape);
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemBufferingTupleStreamDef nullRidBuffer = newInputBuffer(repos);
        implementor.addDataFlowFromProducerToConsumer(
            nullReshape,
            nullRidBuffer);

        FemMergeStreamDef mergeStream = repos.newFemMergeStreamDef();
        mergeStream.setSequential(true);
        mergeStream.setPrePullInputs(false);
        mergeStream.setOutputDesc(
            FennelRelUtil.createTupleDescriptorFromRowType(
                repos,
                getCluster().getTypeFactory(),
                childFennelRel.getRowType()));
        implementor.addDataFlowFromProducerToConsumer(
            notNullReshape,
            mergeStream);
        implementor.addDataFlowFromProducerToConsumer(
            nullRidBuffer,
            mergeStream);

        return mergeStream;
    }

    /**
     * Creates a Reshape execution stream used within the MERGE execution stream
     * that optionally compares the rid column in its input to either null or
     * non-null. Also projects the input based on a specified parameter.
     *
     * @param compareRid true if the stream will be doing rid comparison
     * @param compOp the operator to use in the rid comparison
     * @param projType the type of projection to be done (either project only
     * the rid column, the non-null rid columns, or all columns)
     * @param input FennelRel corresponding to the input that will be reshaped
     *
     * @return created Reshape execution stream
     */
    private FemReshapeStreamDef createMergeReshape(
        boolean compareRid,
        CompOperator compOp,
        ReshapeProjectionType projType,
        FennelRel input)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemReshapeStreamDef reshape = repos.newFemReshapeStreamDef();
        RelDataTypeField [] inputFields = input.getRowType().getFields();
        RexBuilder rexBuilder = getCluster().getRexBuilder();
        RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();

        if (compareRid) {
            // In the comparison case, create a filter on the rid column
            // to filter out nulls
            reshape.setCompareOp(compOp);
            Integer [] compareProj = { 0 };
            reshape.setInputCompareProjection(
                FennelRelUtil.createTupleProjection(repos, compareProj));

            // create a tuple containing a single null value and convert
            // it to a base-64 string
            List<RexLiteral> tuple = new ArrayList<RexLiteral>();
            tuple.add(rexBuilder.constantNull());
            List<List<RexLiteral>> compareTuple =
                new ArrayList<List<RexLiteral>>();
            compareTuple.add(tuple);
            RelDataType ridType =
                typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(SqlTypeName.BIGINT),
                    true);
            RelDataType ridRowType =
                typeFactory.createStructType(
                    new RelDataType[] { ridType },
                    new String[] { "rid" });
            reshape.setTupleCompareBytesBase64(
                FennelRelUtil.convertTuplesToBase64String(
                    ridRowType,
                    compareTuple));
        } else {
            assert (compOp == CompOperatorEnum.COMP_NOOP);
            reshape.setCompareOp(compOp);
        }

        Integer [] outputProj;
        RelDataType outputRowType;
        if (projType == ReshapeProjectionType.PROJECT_RID) {
            outputProj = new Integer[] { 0 };

            // rid needs to not be nullable in the output since the delete
            // (i.e., splicer) expects non-null rid values
            outputRowType = createRidRowType(false);
        } else {
            int nFieldsNotProjected =
                (projType == ReshapeProjectionType.PROJECT_NONRIDS) ? 1 : 0;
            int nFieldsProjected = inputFields.length - nFieldsNotProjected;
            outputProj =
                FennelRelUtil.newBiasedIotaProjection(
                    nFieldsProjected,
                    nFieldsNotProjected);
            if (projType == ReshapeProjectionType.PROJECT_ALL) {
                outputRowType = input.getRowType();
            } else {
                RelDataType [] projFieldTypes =
                    new RelDataType[nFieldsProjected];
                String [] fieldNames = new String[nFieldsProjected];
                for (int i = 0; i < nFieldsProjected; i++) {
                    projFieldTypes[i] =
                        inputFields[i + nFieldsNotProjected].getType();
                    fieldNames[i] =
                        inputFields[i + nFieldsNotProjected].getName();
                }
                outputRowType =
                    typeFactory.createStructType(projFieldTypes, fieldNames);
            }
        }

        reshape.setOutputProjection(
            FennelRelUtil.createTupleProjection(repos, outputProj));
        reshape.setOutputDesc(
            FennelRelUtil.createTupleDescriptorFromRowType(
                repos,
                typeFactory,
                outputRowType));

        return reshape;
    }

    /**
     * Creates a rowtype corresponding to a single rid column
     *
     * @param nullableRid if true, create the rid type column as nullable
     *
     * @return created rowtype
     */
    private RelDataType createRidRowType(boolean nullableRid)
    {
        RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        RelDataType ridType;
        if (nullableRid) {
            ridType =
                typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(SqlTypeName.BIGINT),
                    true);
        } else {
            ridType = typeFactory.createSqlType(SqlTypeName.BIGINT);
        }
        RelDataType rowType =
            typeFactory.createStructType(
                new RelDataType[] { ridType },
                new String[] { "rid" });

        return rowType;
    }

    /**
     * Creates the substream for deleting rows for the MERGE statement
     *
     * @param repos repository
     * @param implementor FennelRel implementor
     * @param indexGuide index guide
     * @param appendStreamDef the append stream that this delete stream is
     * associated with
     * @param childFennelRel FennelRel corresponding to the source input for the
     * MERGE
     * @param splitter the splitter producer that passes rows to the delete
     * substream
     * @param writeRowCountParamId parameter id that the splicer stream will use
     * to write out its rowcount to be read by a downstream barrier
     *
     * @return stream def for the deletion substream
     */
    private FemLbmSplicerStreamDef createDeleteStream(
        FarragoRepos repos,
        FennelRelImplementor implementor,
        LcsIndexGuide indexGuide,
        LcsAppendStreamDef appendStreamDef,
        FennelRel childFennelRel,
        FemSplitterStreamDef splitter,
        int writeRowCountParamId)
    {
        // if we only have an UPDATE substatement, null rids have already
        // been filtered out, so the delete reshape stream only needs to
        // project out the rid columns
        boolean compRid;
        CompOperatorEnum compOp;
        if (updateOnly) {
            compRid = false;
            compOp = CompOperatorEnum.COMP_NOOP;
        } else {
            compRid = true;
            compOp = CompOperatorEnum.COMP_NE;
        }
        FemReshapeStreamDef deleteReshape =
            createMergeReshape(
                compRid,
                compOp,
                ReshapeProjectionType.PROJECT_RID,
                childFennelRel);
        implementor.addDataFlowFromProducerToConsumer(
            splitter,
            deleteReshape);

        return appendStreamDef.createDeleteRidStream(
            implementor,
            deleteReshape,
            estimatedNumRows,
            writeRowCountParamId,
            true);
    }
}

// End LcsTableMergeRel
