/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
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
