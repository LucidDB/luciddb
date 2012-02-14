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
