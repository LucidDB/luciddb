/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
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

import net.sf.farrago.query.*;
import net.sf.farrago.fem.fennel.FemExecutionStreamDef;
import net.sf.farrago.fem.fennel.FemCollectTupleStreamDef;
import net.sf.farrago.fem.fennel.FemTupleDescriptor;
import net.sf.farrago.catalog.FarragoRepos;
import org.eigenbase.relopt.*;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.CollectRel;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.type.SqlTypeName;

/**
 * FennelPullCollectRel is the relational expression corresponding to a collect
 * implemented inside of Fennel.
 *
 * <p>Rules:<ul>
 * <li>{@link FennelCollectRule} creates this from a rex call which has the
 * operator {@link org.eigenbase.sql.fun.SqlMultisetOperator}</li>
 * </ul></p>
 *
 * @author Wael Chatila
 * @since Dec 11, 2004
 * @version $Id$
 */
public class FennelPullCollectRel extends FennelSingleRel
                                  implements FennelPullRel
{
    final String name;

    public FennelPullCollectRel(
        RelOptCluster cluster, RelNode child, String name) {
        super(cluster, new RelTraitSet(FENNEL_PULL_CONVENTION), child);
        this.name = name;
    }

    protected RelDataType deriveRowType()
    {
        return CollectRel.deriveCollectRowType(this, name);
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner) {
        return planner.makeTinyCost();
    }

    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor) {
        final FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemCollectTupleStreamDef collectStreamDef =
            repos.newFemCollectTupleStreamDef();

        collectStreamDef.getInput().add(
            implementor.visitFennelChild((FennelRel) getChild()));
        FemTupleDescriptor outTupleDesc = repos.newFemTupleDescriptor();
        RelDataType type=
            getCluster().getTypeFactory().createSqlType(
                SqlTypeName.Varbinary, 4096);
        type = getCluster().getTypeFactory().createTypeWithNullability(
            type, true);
        FennelRelUtil.addTupleAttrDescriptor(repos, outTupleDesc, type);
        collectStreamDef.setOutputDesc(outTupleDesc);
        return collectStreamDef;
    }

    // override Object (public, does not throw CloneNotSupportedException)
    public Object clone() {
        FennelPullCollectRel clone =
            new FennelPullCollectRel(
                getCluster(), RelOptUtil.clone(getChild()),name);
        clone.inheritTraitsFrom(this);
        return clone;
    }
}
