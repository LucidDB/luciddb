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
package net.sf.farrago.namespace.ftrs;

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
 * FtrsIndexBuilderRel is the relational expression corresponding to building a
 * single unclustered index on an FTRS table. Currently it is implemented via a
 * FemTableWriter; TODO: use a BTreeBuilder instead.
 *
 * <p>The input must be the coverage tuple of the index.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FtrsIndexBuilderRel
    extends FennelSingleRel
{
    //~ Instance fields --------------------------------------------------------

    /**
     * Index to be built.
     */
    private final FemLocalIndex index;

    //~ Constructors -----------------------------------------------------------

    FtrsIndexBuilderRel(
        RelOptCluster cluster,
        RelNode child,
        FemLocalIndex index)
    {
        super(cluster, child);
        this.index = index;
    }

    //~ Methods ----------------------------------------------------------------

    // implement Cloneable
    public FtrsIndexBuilderRel clone()
    {
        FtrsIndexBuilderRel clone =
            new FtrsIndexBuilderRel(
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
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemTableWriterDef tableWriterDef = repos.newFemTableInserterDef();
        FtrsIndexGuide indexGuide =
            new FtrsIndexGuide(
                typeFactory,
                FarragoCatalogUtil.getIndexTable(index));
        FemIndexWriterDef indexWriter = indexGuide.newIndexWriter(this, index);
        indexWriter.setUpdateInPlace(false);

        // NOTE jvs 30-Dec-2005:  we don't set any input projection;
        // the input tuple is already supposed to match the index
        // coverage tuple.  Down in the depths, this also means that
        // FtrsTableWriter will use the index ID as the "table ID" for
        // recovery purposes, and that's OK.

        tableWriterDef.getIndexWriter().add(indexWriter);
        implementor.addDataFlowFromProducerToConsumer(
            input,
            tableWriterDef);
        return tableWriterDef;
    }

    // implement RelNode
    public RelDataType deriveRowType()
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

// End FtrsIndexBuilderRel.java
