/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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

import net.sf.farrago.query.*;
import net.sf.farrago.fem.fennel.FemExecutionStreamDef;
import net.sf.farrago.fem.fennel.FemCollectTupleStreamDef;
import net.sf.farrago.fem.fennel.FemUncollectTupleStreamDef;
import net.sf.farrago.fem.fennel.FemTupleDescriptor;
import net.sf.farrago.catalog.FarragoRepos;
import org.eigenbase.relopt.*;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.UncollectRel;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.type.SqlTypeName;

/**
 * FennelPullUncollectRel is the relational expression corresponding to an
 * UNNEST (Uncollect) implemented inside of Fennel.
 *
 * <p>Rules:<ul>
 * <li>{@link FennelUncollectRule} creates this from a rex call which has the
 * operator {@link org.eigenbase.sql.fun.SqlStdOperatorTable#unnestOperator}</li>
 * </ul></p>
 *
 * @author Wael Chatila
 * @since Dec 12, 2004
 * @version $Id$
 */
public class FennelPullUncollectRel extends FennelSingleRel
                                  implements FennelPullRel {

    public FennelPullUncollectRel(RelOptCluster cluster, RelNode child) {
        super(cluster, child);
    }

    public CallingConvention getConvention() {
        return FENNEL_PULL_CONVENTION;
    }

    protected RelDataType deriveRowType()
    {
        return UncollectRel.deriveUncollectRowType(this);
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner) {
        return planner.makeTinyCost();
    }

    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor) {
        final FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemUncollectTupleStreamDef uncollectStream =
            repos.newFemUncollectTupleStreamDef();

        uncollectStream.getInput().add(
            implementor.visitFennelChild((FennelRel) child));

        return uncollectStream;
    }

    // override Object (public, does not throw CloneNotSupportedException)
    public Object clone() {
        return new FennelPullUncollectRel(cluster, RelOptUtil.clone(child));
    }
}
