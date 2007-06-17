/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2002-2007 Disruptive Tech
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
package com.disruptivetech.farrago.rel;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;


/**
 * FennelPullCollectRel is the relational expression corresponding to a collect
 * implemented inside of Fennel.
 *
 * <p>Rules:
 *
 * <ul>
 * <li>{@link FennelCollectRule} creates this from a {@link CollectRel}.</li>
 * </ul>
 * </p>
 *
 * @author Wael Chatila
 * @version $Id$
 * @since Dec 11, 2004
 */
public class FennelPullCollectRel
    extends FennelSingleRel
{
    //~ Instance fields --------------------------------------------------------

    final String fieldName;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FennelPullCollectRel.
     *
     * @param cluster Cluster
     * @param child Child relational expression
     * @param fieldName Name of the sole output field
     */
    public FennelPullCollectRel(
        RelOptCluster cluster,
        RelNode child,
        String fieldName)
    {
        super(
            cluster,
            new RelTraitSet(FENNEL_EXEC_CONVENTION),
            child);
        this.fieldName = fieldName;
    }

    //~ Methods ----------------------------------------------------------------

    protected RelDataType deriveRowType()
    {
        return CollectRel.deriveCollectRowType(this, fieldName);
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        return planner.makeTinyCost();
    }

    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        final FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemCollectTupleStreamDef collectStreamDef =
            repos.newFemCollectTupleStreamDef();

        implementor.addDataFlowFromProducerToConsumer(
            implementor.visitFennelChild((FennelRel) getChild()),
            collectStreamDef);

        // The column containing the packaged multiset is always VARCHAR(4096)
        // NOT NULL. Even an empty multiset is not represented as NULL.
        FemTupleDescriptor outTupleDesc = repos.newFemTupleDescriptor();
        RelDataType type =
            getCluster().getTypeFactory().createSqlType(
                SqlTypeName.VARBINARY,
                4096);
        FennelRelUtil.addTupleAttrDescriptor(repos, outTupleDesc, type);
        collectStreamDef.setOutputDesc(outTupleDesc);
        return collectStreamDef;
    }

    // override Object (public, does not throw CloneNotSupportedException)
    public FennelPullCollectRel clone()
    {
        FennelPullCollectRel clone =
            new FennelPullCollectRel(
                getCluster(),
                getChild().clone(),
                fieldName);
        clone.inheritTraitsFrom(this);
        return clone;
    }
}

// End FennelPullCollectRel.java
