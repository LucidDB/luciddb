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
package net.sf.farrago.namespace.mdr;

import java.util.*;

import javax.jmi.model.*;

import openjava.ptree.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;


/**
 * MedMdrJoinRel is the relational expression corresponding to a join via
 * association to an MedMdrClassExtent on the right hand side.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMdrJoinRel
    extends JoinRelBase
    implements JavaRel
{
    //~ Instance fields --------------------------------------------------------

    private int leftOrdinal;
    private Reference rightReference;

    //~ Constructors -----------------------------------------------------------

    MedMdrJoinRel(
        RelOptCluster cluster,
        RelNode left,
        RelNode right,
        RexNode condition,
        JoinRelType joinType,
        int leftOrdinal,
        Reference rightReference)
    {
        super(
            cluster,
            new RelTraitSet(CallingConvention.ITERATOR),
            left,
            right,
            condition,
            joinType,
            Collections.<String>emptySet());
        assert ((joinType == JoinRelType.INNER)
            || (joinType == JoinRelType.LEFT));

        this.leftOrdinal = leftOrdinal;
        this.rightReference = rightReference;
    }

    //~ Methods ----------------------------------------------------------------

    int getLeftOrdinal()
    {
        return leftOrdinal;
    }

    Reference getRightReference()
    {
        return rightReference;
    }

    public MedMdrJoinRel clone()
    {
        MedMdrJoinRel clone =
            new MedMdrJoinRel(
                getCluster(),
                left.clone(),
                right.clone(),
                condition.clone(),
                joinType,
                leftOrdinal,
                rightReference);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        // TODO:  refine
        double rowCount = RelMetadataQuery.getRowCount(this);
        return planner.makeCost(
            rowCount,
            0,
            rowCount * getRowType().getFieldList().size());
    }

    // implement RelNode
    public double getRows()
    {
        if (rightReference == null) {
            // TODO:  selectivity
            // many-to-one
            return RelMetadataQuery.getRowCount(left);
        } else {
            // one-to-many:  assume a fanout of five, capped by the
            // total number of rows on the right
            return Math.min(
                5 * RelMetadataQuery.getRowCount(left),
                RelMetadataQuery.getRowCount(right));
        }
    }

    // implement RelNode
    public ParseTree implement(JavaRelImplementor implementor)
    {
        MedMdrJoinRelImplementor joinImplementor =
            new MedMdrJoinRelImplementor(this);
        return joinImplementor.implement(implementor);
    }
}

// End MedMdrJoinRel.java
