/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
package net.sf.farrago.namespace.ftrs;

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
 * FtrsIndexSearchRel refines FtrsIndexScanRel.  Instead of scanning an
 * entire index, it only searches for keys produced by its child.  In addition,
 * it is able to propagate non-key values from its child, implementing an index
 * join.  For a join, the output order is child first and index search results
 * second.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FtrsIndexSearchRel extends FennelPullSingleRel
{
    //~ Instance fields -------------------------------------------------------

    /** Aggregation used since multiple inheritance is unavailable. */
    final FtrsIndexScanRel scanRel;
    final boolean isUniqueKey;
    final boolean isOuter;
    final Integer [] inputKeyProj;
    final Integer [] inputJoinProj;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FtrsIndexSearchRel object.
     *
     * @param scanRel underlying FtrsIndexScanRel
     * @param child input which produces keys
     * @param isUniqueKey whether keys are known to be unique
     * @param isOuter whether nulls should be made up for unmatched inputs
     * @param inputKeyProj TODO:  doc
     * @param inputJoinProj TODO:  doc
     */
    public FtrsIndexSearchRel(
        FtrsIndexScanRel scanRel,
        RelNode child,
        boolean isUniqueKey,
        boolean isOuter,
        Integer [] inputKeyProj,
        Integer [] inputJoinProj)
    {
        super(
            scanRel.getCluster(),
            child);
        this.scanRel = scanRel;
        this.isUniqueKey = isUniqueKey;
        this.isOuter = isOuter;
        this.inputKeyProj = inputKeyProj;
        this.inputJoinProj = inputJoinProj;
    }

    //~ Methods ---------------------------------------------------------------

    // override Rel
    public RexNode [] getChildExps()
    {
        return scanRel.getChildExps();
    }

    // override Rel
    public double getRows()
    {
        // TODO:  this is only true when isUniqueKey
        return child.getRows();
    }

    // implement Cloneable
    public Object clone()
    {
        FtrsIndexSearchRel clone = new FtrsIndexSearchRel(
            scanRel,
            RelOptUtil.clone(child),
            isUniqueKey,
            isOuter,
            inputKeyProj,
            inputJoinProj);
        clone.traits = cloneTraits();
        return clone;
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        // TODO:  refined costing
        return scanRel.computeCost(
            planner,
            getRows());
    }

    // implement RelNode
    public RelDataType deriveRowType()
    {
        if (inputJoinProj != null) {
            // We're implementing a join, so make up an appropriate join type.
            final RelDataTypeField [] childFields =
                child.getRowType().getFields();
            RelDataType leftType =
                getCluster().typeFactory.createStructType(
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

            RelDataType rightType = scanRel.getRowType();

            // for outer join, have to make left side nullable
            if (isOuter) {
                rightType =
                    getFarragoTypeFactory().createTypeWithNullability(rightType,
                        true);
            }

            return getCluster().typeFactory.createJoinType(
                new RelDataType [] { leftType, rightType });
        } else {
            assert (!isOuter);
            return scanRel.getRowType();
        }
    }

    // implement RelNode
    public void explain(RelOptPlanWriter pw)
    {
        Object projection;
        Object inputKeyProjObj;
        Object inputJoinProjObj;

        if (scanRel.projectedColumns == null) {
            projection = "*";
        } else {
            projection = Arrays.asList(scanRel.projectedColumns);
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
        pw.explain(
            this,
            new String [] {
                "child", "table", "projection", "index", "uniqueKey",
                "preserveOrder", "outer", "inputKeyProj", "inputJoinProj"
            },
            new Object [] {
                Arrays.asList(scanRel.ftrsTable.getQualifiedName()), projection,
                scanRel.index.getName(), Boolean.valueOf(isUniqueKey),
                Boolean.valueOf(scanRel.isOrderPreserving),
                Boolean.valueOf(isOuter), inputKeyProjObj, inputJoinProjObj
            });
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);

        FemIndexSearchDef searchStream = repos.newFemIndexSearchDef();

        scanRel.defineScanStream(searchStream);
        searchStream.setUniqueKey(isUniqueKey);
        searchStream.setOuterJoin(isOuter);
        if (inputKeyProj != null) {
            searchStream.setInputKeyProj(
                FennelRelUtil.createTupleProjection(repos, inputKeyProj));
        }
        if (inputJoinProj != null) {
            searchStream.setInputJoinProj(
                FennelRelUtil.createTupleProjection(repos, inputJoinProj));
        }
        searchStream.getInput().add(
            implementor.visitFennelChild((FennelRel) child));

        return searchStream;
    }

    // override Rel
    public RelOptTable getTable()
    {
        return scanRel.getTable();
    }

    // TODO:  under some circumstances, FtrsIndexSearchRel could produce
    // sorted output, in which case we should implement getCollations()
}


// End FtrsIndexSearchRel.java
