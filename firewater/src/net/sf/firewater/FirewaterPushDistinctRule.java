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

import net.sf.farrago.fennel.rel.*;
import org.luciddb.lcs.*;
import org.eigenbase.reltype.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.namespace.jdbc.*;
import net.sf.farrago.query.*;
import net.sf.farrago.fem.med.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;

import java.util.logging.*;
import net.sf.farrago.trace.*;

import org.eigenbase.relopt.hep.*;

/**
 * FirewaterPushDistinctRule removes a top-level LhxAggRel step on the single
 * Firewater instance so that only each partition must do a DISTINCT check.
 *
 * @author Kevin Secretan
 * @version $Id$
 */
public class FirewaterPushDistinctRule extends RelOptRule {

    private static final Logger tracer
        = FarragoTrace.getClassTracer(FirewaterPushDistinctRule.class);

    public static final FirewaterPushDistinctRule instance =
        new FirewaterPushDistinctRule();

    /**
     * Creates a FirewaterPushDistinctRule
     */
    private FirewaterPushDistinctRule()
    {
        super(new RelOptRuleOperand(
                    AggregateRel.class,
                    new RelOptRuleOperand(
                        AggregateRel.class,
                        new RelOptRuleOperand(
                            UnionRel.class, ANY))));
    }

    public void onMatch(RelOptRuleCall call)
    {
        AggregateRel agg = (AggregateRel) call.rels[0];
        AggregateRel child = (AggregateRel) call.rels[1];
        UnionRel uni = (UnionRel) call.rels[2];

        //tracer.info("OHYEAH CALLED\n\n");
        // Assert that the top node is groupless and child is grouping for
        // distinct.
        if (child.getGroupCount() != 1)
            return;
        // Assert that the aggs have been pushed through the union
        if (!(((HepRelVertex)uni.getInput(0)).getCurrentRel() instanceof AggregateRel))
            return;


        // Assert that the key we're grouping by (count_key) is the
        // PARTITION_COLUMN stored in the table.
        String count_key = agg.getChild().getRowType().getFields()[0].getName();

        boolean is_partition_column = false;
        // Grab the FemLocalTable from the catalog.
        // (Commented out w.i.p. since it wasn't working right and haven't
        // fixed it yet.)
        FarragoRepos repos =
            FarragoRelUtil.getPreparingStmt(call.rels[0]).getRepos();
        RelOptTable tab = agg.getTable();
        //CwmCatalog catalog = repos.getSelfAsCatalog();
        /*for (CwmModelElement element : catalog.getOwnedElement()) {
            if (element instanceof FemLocalTable) {
                Properties tableProps =
                    FarragoCatalogUtil.getStorageOptionsAsProperties(repos, (FemLocalTable)table);
                    */

        is_partition_column = true;
        if (!is_partition_column)
            return;

        FennelAggRel newtop = new FennelAggRel(
            agg.getCluster(),
            child.getChild(),
            agg.getGroupCount(),
            agg.getAggCallList());

        call.transformTo(newtop);
    }
}

