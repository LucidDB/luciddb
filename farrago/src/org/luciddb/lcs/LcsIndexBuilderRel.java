/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2010-2010 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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
