/*
// $Id$
// Firewater is a scaleout column store DBMS.
// Copyright (C) 2010-2010 John V. Sichi
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
package net.sf.firewater;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.namespace.jdbc.*;
import net.sf.farrago.query.*;
import net.sf.farrago.fem.med.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;

/**
 * FirewaterReplicaJoinRule chooses the correct replica to join against
 * a factor which is already local to a particular storage node.
 *
 * @author John Sichi
 * @version $Id$
 */
class FirewaterReplicaJoinRule extends RelOptRule
{
    public static final FirewaterReplicaJoinRule instanceReplicaOnLeft =
        new FirewaterReplicaJoinRule(
            new RelOptRuleOperand(
                JoinRel.class,
                new RelOptRuleOperand(FirewaterReplicatedTableRel.class, ANY),
                new RelOptRuleOperand(MedJdbcQueryRel.class, ANY)),
            "replica on left",
            true);

    public static final FirewaterReplicaJoinRule instanceReplicaOnRight =
        new FirewaterReplicaJoinRule(
            new RelOptRuleOperand(
                JoinRel.class,
                new RelOptRuleOperand(MedJdbcQueryRel.class, ANY),
                new RelOptRuleOperand(FirewaterReplicatedTableRel.class, ANY)),
            "replica on right",
            false);

    private final boolean replicaOnLeft;
    
    /**
     * Creates a FirewaterReplicaJoinRule.
     */
    private FirewaterReplicaJoinRule(
        RelOptRuleOperand operand, String id, boolean replicaOnLeft)
    {
        super(
            operand,
            "FirewaterReplicaJoinRule: " + id);
        this.replicaOnLeft = replicaOnLeft;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        JoinRel joinRel = (JoinRel) call.rels[0];
        FirewaterReplicatedTableRel tableRel =
            (FirewaterReplicatedTableRel) call.rels[replicaOnLeft ? 1 : 2];
        MedJdbcQueryRel otherRel =
            (MedJdbcQueryRel) call.rels[replicaOnLeft ? 2 : 1];
        MedJdbcColumnSet columnSet = otherRel.getColumnSet();
        MedJdbcDataServer server = columnSet.getDirectory().getServer();
        FarragoRepos repos =
            FarragoRelUtil.getPreparingStmt(tableRel).getRepos();
        FemDataServer node = (FemDataServer) repos.getMdrRepos().getByMofId(
            server.getServerMofId());
        String catalogName =
            FirewaterDdlHandler.getCatalogNameForServer(node);
        RelNode newTableRel =
            tableRel.replicatedTable.createRelForSpecificNode(
                tableRel.getCluster(),
                tableRel.getConnection(),
                node,
                catalogName);
        JoinRel newJoinRel = new JoinRel(
            joinRel.getCluster(),
            replicaOnLeft ? newTableRel : otherRel,
            replicaOnLeft ? otherRel : newTableRel,
            joinRel.getCondition(),
            joinRel.getJoinType(),
            joinRel.getVariablesStopped());
        call.transformTo(newJoinRel);
    }
}

// End FirewaterReplicaJoinRule.java
