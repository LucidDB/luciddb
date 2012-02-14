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
import net.sf.farrago.query.*;
import net.sf.farrago.fem.med.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;

/**
 * FirewaterArbitraryReplicaRule chooses an arbitrary replica to use
 * for implementing access to a {@link FirewaterReplicatedTableRel}.
 *
 * @author John Sichi
 * @version $Id$
 */
class FirewaterArbitraryReplicaRule extends RelOptRule
{
    public static final FirewaterArbitraryReplicaRule instance =
        new FirewaterArbitraryReplicaRule();

    /**
     * Creates a FirewaterArbitraryReplicaRule.
     */
    private FirewaterArbitraryReplicaRule()
    {
        super(
            new RelOptRuleOperand(
                FirewaterReplicatedTableRel.class,
                ANY));
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        FirewaterReplicatedTableRel tableRel =
            (FirewaterReplicatedTableRel) call.rels[0];
        FarragoRepos repos =
            FarragoRelUtil.getPreparingStmt(tableRel).getRepos();
        Collection<FemDataServer> servers =
            repos.allOfType(FemDataServer.class);
        FemDataServer node = null;
        for (FemDataServer server : servers) {
            String wrapperName = server.getWrapper().getName();
            if (!wrapperName.startsWith("SYS_FIREWATER")) {
                continue;
            }
            if (wrapperName.contains("DISTRIBUTED")) {
                continue;
            }
            node = server;
            if (wrapperName.equals("SYS_FIREWATER_EMBEDDED_WRAPPER")) {
                // Give preference to local replica
                break;
            }
        }
        assert(node != null);
        String catalogName =
            FirewaterDdlHandler.getCatalogNameForServer(node);
        RelNode newRel =
            tableRel.replicatedTable.createRelForSpecificNode(
                tableRel.getCluster(), tableRel.getConnection(), node,
                catalogName);
        call.transformTo(newRel);
    }
}

// End FirewaterArbitraryReplicaRule.java
