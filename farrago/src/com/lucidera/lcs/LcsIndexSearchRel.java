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

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.query.*;
import net.sf.farrago.type.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.RexNode;

/**
 * LcsIndexSearchRel refines LcsIndexScanRel.  Instead of scanning an
 * entire index, it only searches for keys produced by its child.
 *
 * @author Rushan Chen
 * @version $Id$
 */
class LcsIndexSearchRel extends FennelSingleRel
{
    //~ Instance fields -------------------------------------------------------

    /** Aggregation used since multiple inheritance is unavailable. */
    final LcsIndexScanRel indexScanRel;
    final boolean isUniqueKey;
    final boolean isOuter;
    final Integer [] inputKeyProj;
    final Integer [] inputJoinProj;
    final Integer [] inputDirectiveProj;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new LcsIndexSearchRel object.
     *
     * @param indexScanRel underlying LcsIndexScanRel
     * @param child input which produces keys
     * @param isUniqueKey whether keys are known to be unique
     * @param isOuter whether nulls should be made up for unmatched inputs
     * @param inputKeyProj TODO:  doc
     * @param inputJoinProj TODO:  doc
     * @param inputDirectiveProj TODO:  doc
     */
    public LcsIndexSearchRel(
        LcsIndexScanRel indexScanRel,
        RelNode child,
        boolean isUniqueKey,
        boolean isOuter,
        Integer [] inputKeyProj,
        Integer [] inputJoinProj,
        Integer [] inputDirectiveProj)
    {
        super(
            indexScanRel.getCluster(),
            child);
        this.indexScanRel = indexScanRel;
        this.isUniqueKey = isUniqueKey;
        this.isOuter = isOuter;
        this.inputKeyProj = inputKeyProj;
        this.inputJoinProj = inputJoinProj;
        this.inputDirectiveProj = inputDirectiveProj;
    }

    //~ Methods ---------------------------------------------------------------

    // override Rel
    public RexNode [] getChildExps()
    {
        return indexScanRel.getChildExps();
    }

    // override Rel
    public double getRows()
    {
        // TODO:  this is only true when isUniqueKey
        return getChild().getRows();
    }

    // implement Cloneable
    public Object clone()
    {
        LcsIndexSearchRel clone = new LcsIndexSearchRel(
            indexScanRel,
            RelOptUtil.clone(getChild()),
            isUniqueKey,
            isOuter,
            inputKeyProj,
            inputJoinProj,
            inputDirectiveProj);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        // TODO:  refined costing
        return indexScanRel.computeCost(
            planner,
            getRows());
    }

    // implement RelNode
    public RelDataType deriveRowType()
    {
        if (inputJoinProj != null) {
            // TODO: this part is no implemented yet.
            // We're implementing a join, so make up an appropriate join type.
            final RelDataTypeField [] childFields =
                getChild().getRowType().getFields();
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

            RelDataType rightType = indexScanRel.getRowType();

            // for outer join, have to make left side nullable
            if (isOuter) {
                rightType =
                    getFarragoTypeFactory().createTypeWithNullability(rightType,
                        true);
            }

            return getCluster().getTypeFactory().createJoinType(
                new RelDataType [] { leftType, rightType });
        } else {
            assert (!isOuter);
            return indexScanRel.getRowType();
        }
    }

    // implement RelNode
    public void explain(RelOptPlanWriter pw)
    {
        Object projection;
        Object inputKeyProjObj;
        Object inputJoinProjObj;
        Object inputDirectiveProjObj;

        if (indexScanRel.projectedColumns == null) {
            projection = "*";
        } else {
            projection = Arrays.asList(indexScanRel.projectedColumns);
        }

        if (inputKeyProj == null) {
            inputKeyProjObj = "*";
        } else {
            inputKeyProjObj = Arrays.asList(inputKeyProj);
        }

        if (inputJoinProj == null) {
            inputJoinProjObj = Collections.EMPTY_LIST;
        } else {
            inputJoinProjObj = Arrays.asList(inputJoinProj);
        }
        
        if (inputDirectiveProj == null) {
            inputDirectiveProjObj = Collections.EMPTY_LIST;
        } else {
            inputDirectiveProjObj = Arrays.asList(inputDirectiveProj);
        }
        pw.explain(
            this,
            new String [] {
                "child", "table", "projection", "index", "uniqueKey",
                "preserveOrder", "outer", "inputKeyProj", "inputJoinProj",
                "inputDirectiveProj"
            },
            new Object [] {
                Arrays.asList(indexScanRel.lcsTable.getQualifiedName()), projection,
                indexScanRel.index.getName(), Boolean.valueOf(isUniqueKey),
                Boolean.valueOf(indexScanRel.isOrderPreserving),
                Boolean.valueOf(isOuter), inputKeyProjObj, inputJoinProjObj,
                inputDirectiveProjObj
            });
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);

        FemLbmIndexScanStreamDef indexSearchStream = 
            indexScanRel.newLbmIndexSearch(this);

        indexSearchStream.setUniqueKey(isUniqueKey);
        indexSearchStream.setOuterJoin(isOuter);

        if (inputKeyProj != null) {
            indexSearchStream.setInputKeyProj(
                FennelRelUtil.createTupleProjection(repos, inputKeyProj));
        }

        if (inputJoinProj != null) {
            indexSearchStream.setInputJoinProj(
                FennelRelUtil.createTupleProjection(repos, inputJoinProj));
        }

        if (inputDirectiveProj != null) {
            indexSearchStream.setInputDirectiveProj(
                FennelRelUtil.createTupleProjection(repos, inputDirectiveProj));
        }

        indexSearchStream.setRowLimitParamId(0);
        indexSearchStream.setIgnoreRowLimit(true);
        indexSearchStream.setStartRidParamId(0);
        
        implementor.addDataFlowFromProducerToConsumer(
            implementor.visitFennelChild((FennelRel) getChild()), 
            indexSearchStream);

        return indexSearchStream;
    }

    // override Rel
    public RelOptTable getTable()
    {
        return indexScanRel.getTable();
    }

    // TODO: implement getCollations()
}

// End LcsIndexSearchRel.java
