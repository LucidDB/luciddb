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
