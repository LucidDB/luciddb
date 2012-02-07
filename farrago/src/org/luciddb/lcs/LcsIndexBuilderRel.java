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
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fennel.rel.*;
import net.sf.farrago.query.*;
import net.sf.farrago.type.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * LcsIndexBuilderRel is a relational expression that builds an unclustered Lcs
 * index. It is implemented via a bitmap index generator.
 *
 * <p>The input to this relation should be a single row of inputs expected by
 * LbmGeneratorStream. This row has two columns, number of rows to index and
 * start row id.
 *
 * @author John Pham
 * @version $Id$
 */
class LcsIndexBuilderRel
    extends FennelSingleRel
{
    //~ Instance fields --------------------------------------------------------

    /**
     * Index to be built.
     */
    private final FemLocalIndex index;

    //~ Constructors -----------------------------------------------------------

    protected LcsIndexBuilderRel(
        RelOptCluster cluster,
        RelNode child,
        FemLocalIndex index)
    {
        super(cluster, child);
        this.index = index;
    }

    //~ Methods ----------------------------------------------------------------

    // implement Cloneable
    public LcsIndexBuilderRel clone()
    {
        LcsIndexBuilderRel clone =
            new LcsIndexBuilderRel(
                getCluster(),
                getChild().clone(),
                index);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        // TODO:  the real thing
        return planner.makeTinyCost();
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FemExecutionStreamDef input =
            implementor.visitFennelChild((FennelRel) getChild(), 0);
        FarragoTypeFactory typeFactory = getFarragoTypeFactory();

        FemLocalTable table = FarragoCatalogUtil.getIndexTable(index);
        LcsIndexGuide indexGuide = new LcsIndexGuide(typeFactory, table, index);
        FennelRelParamId paramId = implementor.allocateRelParamId();
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemLocalIndex deletionIndex =
            FarragoCatalogUtil.isIndexUnique(index)
            ? FarragoCatalogUtil.getDeletionIndex(repos, table)
            : null;
        LcsCompositeStreamDef bitmapSet =
            indexGuide.newBitmapAppend(
                this,
                index,
                deletionIndex,
                implementor,
                true,
                paramId,
                false);

        // TODO: review recovery behavior
        implementor.addDataFlowFromProducerToConsumer(
            input,
            bitmapSet.getConsumer());
        return bitmapSet.getProducer();
    }

    // implement RelNode
    protected RelDataType deriveRowType()
    {
        return RelOptUtil.createDmlRowType(getCluster().getTypeFactory());
    }

    // implement RelNode
    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String[] { "child", "index" },
            new Object[] {
                Arrays.asList(
                    FarragoCatalogUtil.getQualifiedName(index).names)
            });
    }

    // implement FennelRel
    public RelFieldCollation [] getCollations()
    {
        // TODO:  say it's sorted instead.  This can be done generically for all
        // FennelRel's guaranteed to return at most one row
        return RelFieldCollation.emptyCollationArray;
    }
}

// End LcsIndexBuilderRel.java
