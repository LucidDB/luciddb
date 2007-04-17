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

import java.util.*;

import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * LcsIndexSearchRel is a relation for reading from an unclustered index. It has
 * two major modes. In the "full scan" mode, it has no input and it scans the
 * entire index. In the "key search" mode, it has one input and only searches for
 * keys produced by its child.
 *
 * <p>Key search relations, have two formats for input. A single key set may be
 * specified, in which case, exact matches for keys are returned. The double key
 * format is a more versatile format in which both points and ranges can be
 * represented. The input produces two sets of keys, one for a lower bound and
 * one for an upper bound. The upper and lower bounds are described by
 * directives as OPEN, or CLOSED, etc.
 *
 * <p>The output of an index scan may be expanded with fields from its input,
 * making it more useful for implementing an index-based join.
 *
 * @author Rushan Chen
 * @version $Id$
 */
class LcsIndexSearchRel
    extends FennelOptionalRel
{

    //~ Instance fields --------------------------------------------------------

    /**
     * Index to use for access.
     */
    final FemLocalIndex index;

    /**
     * The table containing the index to be scanned
     */
    final LcsTable lcsTable;

    /**
     * Whether to perform a full scan
     */
    final boolean fullScan;

    /**
     * Array of 0-based flattened column ordinals to project; if null, project
     * bitmap columns. These ordinals are relative to the index.
     */
    final Integer [] projectedColumns;

    final boolean isUniqueKey;
    final boolean isOuter;

    final Integer [] inputKeyProj;
    final Integer [] inputJoinProj;
    final Integer [] inputDirectiveProj;

    FennelRelParamId startRidParamId;
    FennelRelParamId rowLimitParamId;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new LcsIndexSearchRel object. In general, parameters are
     * nullable if they do not apply to the search being specified.
     *
     * @param cluster the environment for the scan
     * @param child input which produces keys
     * @param lcsTable the table containing the index
     * @param fullScan whether to perform a full scan
     * @param projectedColumns the columns to be projected from an index scan.
     * If this parameter is null for a "key search", then the three bitmap
     * fields are projected. This parameter cannot be null for a "full scan".
     * @param isUniqueKey for a search, whether keys are known to be unique
     * @param isOuter for a search with join, whether nulls should be made up
     * for unmatched inputs
     * @param inputKeyProj for a double key search, the projection of input
     * fields to be used as search keys
     * @param inputJoinProj for an index join, a projection of input fields to
     * be added to the output
     * @param inputDirectiveProj for a double key search, the projection of
     * input fields describing search endpoints, such as OPEN or CLOSED
     * @param startRidParamId parameter ID for searching using start Rid as
     * part of the key
     * @param rowLimitParamId parameter ID to limit the number of rows fetched
     * in one execute
     */
    public LcsIndexSearchRel(
        RelOptCluster cluster,
        RelNode child,
        LcsTable lcsTable,
        FemLocalIndex index,
        boolean fullScan,
        Integer [] projectedColumns,
        boolean isUniqueKey,
        boolean isOuter,
        Integer [] inputKeyProj,
        Integer [] inputJoinProj,
        Integer [] inputDirectiveProj,
        FennelRelParamId startRidParamId,
        FennelRelParamId rowLimitParamId)
    {
        super(cluster, child);
        assert ((isOuter == false) && (inputJoinProj == null));

        this.lcsTable = lcsTable;
        this.index = index;
        this.fullScan = fullScan;
        if (fullScan) {
            assert (projectedColumns != null);
        }
        this.projectedColumns = projectedColumns;
        this.isUniqueKey = isUniqueKey;
        this.isOuter = isOuter;
        this.inputKeyProj = inputKeyProj;
        this.inputJoinProj = inputJoinProj;
        this.inputDirectiveProj = inputDirectiveProj;

        this.startRidParamId = startRidParamId;
        this.rowLimitParamId = rowLimitParamId;
    }

    //~ Methods ----------------------------------------------------------------

    // override Rel
    public double getRows()
    {
        // TODO:  this is not correct if num of rows returned by an index is
        // filtered by sargable predicte.
        return lcsTable.getRowCount();
    }

    // implement Cloneable
    public LcsIndexSearchRel clone()
    {
        LcsIndexSearchRel clone =
            new LcsIndexSearchRel(
                getCluster(),
                getChild(),
                lcsTable,
                index,
                fullScan,
                projectedColumns,
                isUniqueKey,
                isOuter,
                inputKeyProj,
                inputJoinProj,
                inputDirectiveProj,
                startRidParamId,
                rowLimitParamId);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    /**
     * Create a new index search rel based on the existing one but with new
     * dynamic parameters.
     * @param startRidParamId parameter ID for searching using start Rid as
     * part of the key
     * @param rowLimitParamId parameter ID to limit the number of rows fetched
     * in one execute
     * @return the newly created index search rel.
     */
    public LcsIndexSearchRel cloneWithNewParams(
        FennelRelParamId startRidParamId,
        FennelRelParamId rowLimitParamId)
    {
        LcsIndexSearchRel clone =
            new LcsIndexSearchRel(
                getCluster(),
                getChild(),
                lcsTable,
                index,
                fullScan,
                projectedColumns,
                isUniqueKey,
                isOuter,
                inputKeyProj,
                inputJoinProj,
                inputDirectiveProj,
                startRidParamId,
                rowLimitParamId);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        // TODO:  refined costing
        return computeCost(
                planner,
                RelMetadataQuery.getRowCount(this));
    }

    // implement RelNode
    RelOptCost computeCost(
        RelOptPlanner planner,
        double dRows)
    {
        // TODO:  compute page-based I/O cost
        // CPU cost is proportional to number of columns projected
        // I/O cost is proportional to pages of index scanned
        double dCpu = dRows * getRowType().getFieldList().size();

        // TODO: adjust when selecting index key values(index only scan)
        // [RID, bitmapfield1, bitmapfield2]
        int nIndexCols = 3;

        double dIo = dRows * nIndexCols;

        return planner.makeCost(dRows, dCpu, dIo);
    }

    // implement RelNode
    protected RelDataType deriveRowType()
    {
        RelDataType rowType =
            lcsTable.getIndexGuide().createUnclusteredRowType(index,
                projectedColumns);

        if (inputJoinProj != null) {
            // TODO: this part is no implemented yet.
            // We're implementing a join, so make up an appropriate join type.
            final RelDataTypeField [] childFields = rowType.getFields();
            RelDataType leftType =
                getCluster().getTypeFactory().createStructType(
                    new RelDataTypeFactory.FieldInfo() {
                        public int getFieldCount()
                        {
                            return inputJoinProj.length;
                        }

                        public String getFieldName(int index)
                        {
                            int i = inputJoinProj[index].intValue();
                            return childFields[i].getName();
                        }

                        public RelDataType getFieldType(int index)
                        {
                            int i = inputJoinProj[index].intValue();
                            return childFields[i].getType();
                        }
                    });

            RelDataType rightType = rowType;

            // for outer join, have to make left side nullable
            if (isOuter) {
                rightType =
                    getFarragoTypeFactory().createTypeWithNullability(rightType,
                        true);
            }

            return
                getCluster().getTypeFactory().createJoinType(
                    new RelDataType[] { leftType, rightType });
        } else {
            return rowType;
        }
    }

    // implement RelNode
    public void explain(RelOptPlanWriter pw)
    {
        Object projection;
        Object inputKeyProjObj;
        Object inputDirectiveProjObj;

        if (projectedColumns == null) {
            projection = "*";
        } else {
            projection = Arrays.asList(projectedColumns);
        }

        if (inputKeyProj == null) {
            inputKeyProjObj = "*";
        } else {
            inputKeyProjObj = Arrays.asList(inputKeyProj);
        }

        if (inputDirectiveProj == null) {
            inputDirectiveProjObj = Collections.EMPTY_LIST;
        } else {
            inputDirectiveProjObj = Arrays.asList(inputDirectiveProj);
        }

        pw.explain(
            this,
            new String[] {
                "child", "table", "index", "projection", "inputKeyProj",
            "inputDirectiveProj", "startRidParamId", "rowLimitParamId"
            },
            new Object[] {
                Arrays.asList(lcsTable.getQualifiedName()), index.getName(),
            projection, inputKeyProjObj, inputDirectiveProjObj,
            (startRidParamId == null) ? (Integer) 0 : startRidParamId,
            (rowLimitParamId == null) ? (Integer) 0 : rowLimitParamId
            });
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FemExecutionStreamDef newStream;

        if (!fullScan) {
            // index search use the default projection of:
            // [StartRID, BitmapDescriptor, BitmapSegment]
            newStream =
                lcsTable.getIndexGuide().newIndexSearch(
                    this,
                    index,
                    isUniqueKey,
                    isOuter,
                    inputKeyProj,
                    inputJoinProj,
                    inputDirectiveProj,
                    projectedColumns,
                    implementor.translateParamId(startRidParamId),
                    implementor.translateParamId(rowLimitParamId));

            implementor.addDataFlowFromProducerToConsumer(
                implementor.visitFennelChild((FennelRel) getChild()),
                newStream);
        } else {
            // pure scan
            newStream =
                lcsTable.getIndexGuide().newIndexScan(this,
                    index,
                    projectedColumns);
        }

        return newStream;
    }

    // override Rel
    public RelOptTable getTable()
    {
        return lcsTable;
    }

    public boolean isUniqueKey()
    {
        return isUniqueKey;
    }

    public boolean isOuter()
    {
        return isOuter;
    }

    public Integer [] getInputKeyProj()
    {
        return inputKeyProj;
    }

    public Integer [] getInputJoinProj()
    {
        return inputJoinProj;
    }

    public Integer [] getInputDirectiveProj()
    {
        return inputDirectiveProj;
    }

    public boolean isInputSingleKeyset()
    {
        return inputDirectiveProj == null;
    }

    public int getInputKeyCount()
    {
        if (inputDirectiveProj == null) {
            // one key format
            return index.getIndexedFeature().size();
        }

        // two key format
        return inputKeyProj.length / 2;
    }
}

//End LcsIndexSearchRel.java
