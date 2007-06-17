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


/**
 * FennelPullUncollectRel is the relational expression corresponding to an
 * UNNEST (Uncollect) implemented inside of Fennel.
 *
 * <p>Rules:
 *
 * <ul>
 * <li>{@link FennelUncollectRule} creates this from a rex call which has the
 * operator {@link
 * org.eigenbase.sql.fun.SqlStdOperatorTable#unnestOperator}</li>
 * </ul>
 * </p>
 *
 * @author Wael Chatila
 * @version $Id$
 * @since Dec 12, 2004
 */
public class FennelPullUncollectRel
    extends FennelSingleRel
{
    //~ Constructors -----------------------------------------------------------

    public FennelPullUncollectRel(RelOptCluster cluster, RelNode child)
    {
        super(
            cluster,
            new RelTraitSet(FENNEL_EXEC_CONVENTION),
            child);
        assert deriveRowType() != null : "invalid child rowtype";
    }

    //~ Methods ----------------------------------------------------------------

    protected RelDataType deriveRowType()
    {
        return UncollectRel.deriveUncollectRowType(getChild());
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        return planner.makeTinyCost();
    }

    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        final FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemUncollectTupleStreamDef uncollectStream =
            repos.newFemUncollectTupleStreamDef();

        implementor.addDataFlowFromProducerToConsumer(
            implementor.visitFennelChild((FennelRel) getChild()),
            uncollectStream);

        return uncollectStream;
    }

    // override Object (public, does not throw CloneNotSupportedException)
    public FennelPullUncollectRel clone()
    {
        FennelPullUncollectRel clone =
            new FennelPullUncollectRel(
                getCluster(),
                getChild().clone());
        clone.inheritTraitsFrom(this);
        return clone;
    }
}

// End FennelPullUncollectRel.java
