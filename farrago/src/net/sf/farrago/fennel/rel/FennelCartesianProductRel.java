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
package net.sf.farrago.fennel.rel;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.util.*;


/**
 * FennelCartesianProductRel represents the Fennel implementation of Cartesian
 * product.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelCartesianProductRel
    extends FennelDoubleRel
{
    //~ Instance fields --------------------------------------------------------

    private final JoinRelType joinType;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FennelCartesianProductRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param left left input
     * @param right right input
     * @param fieldNameList If not null, the row type will have these field
     * names
     */
    public FennelCartesianProductRel(
        RelOptCluster cluster,
        RelNode left,
        RelNode right,
        JoinRelType joinType,
        List<String> fieldNameList)
    {
        super(cluster, left, right);
        assert joinType != null;
        this.joinType = joinType;
        this.rowType =
            JoinRel.deriveJoinRowType(
                left.getRowType(),
                right.getRowType(),
                joinType,
                cluster.getTypeFactory(),
                fieldNameList,
                Collections.<RelDataTypeField>emptyList());
    }

    //~ Methods ----------------------------------------------------------------

    // implement Cloneable
    public FennelCartesianProductRel clone()
    {
        FennelCartesianProductRel clone =
            new FennelCartesianProductRel(
                getCluster(),
                left.clone(),
                right.clone(),
                joinType,
                RelOptUtil.getFieldNameList(rowType));
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        // TODO:  account for buffering I/O and CPU
        double rowCount = RelMetadataQuery.getRowCount(this);
        return planner.makeCost(
            rowCount,
            0,
            rowCount * getRowType().getFieldList().size());
    }

    // implement RelNode
    public double getRows()
    {
        return RelMetadataQuery.getRowCount(left)
            * RelMetadataQuery.getRowCount(right);
    }

    // override RelNode
    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String[] { "left", "right", "leftouterjoin" },
            new Object[] { isLeftOuter() });
    }

    private boolean isLeftOuter()
    {
        return JoinRelType.LEFT == joinType;
    }

    // implement RelNode
    protected RelDataType deriveRowType()
    {
        throw Util.newInternal("row type should have been set already");
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemCartesianProductStreamDef streamDef =
            repos.newFemCartesianProductStreamDef();

        FemExecutionStreamDef leftInput =
            implementor.visitFennelChild((FennelRel) left, 0);
        implementor.addDataFlowFromProducerToConsumer(
            leftInput,
            streamDef);
        FemExecutionStreamDef rightInput =
            implementor.visitFennelChild((FennelRel) right, 1);
        implementor.addDataFlowFromProducerToConsumer(
            rightInput,
            streamDef);
        streamDef.setLeftOuter(isLeftOuter());
        return streamDef;
    }

    // TODO:  implement getCollations()
}

// End FennelCartesianProductRel.java
