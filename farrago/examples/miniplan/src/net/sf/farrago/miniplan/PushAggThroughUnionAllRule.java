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
package net.sf.farrago.miniplan;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rel.metadata.*;

import java.util.*;

/**
 * PushAggThroughUnionAllRule implements the rule for pushing an
 * {@link AggregateRel} past a non-distinct {@link UnionRel}.
 *
 *<p>
 *
 * If you modify this class, please update <a
 * href="http://pub.eigenbase.org/wiki/HowToWriteAnOptimizer">the
 * corresponding wiki page</a> as well.
 *
 * @author John Sichi
 * @version $Id$
 */
public class PushAggThroughUnionAllRule extends RelOptRule
{
    public static final PushAggThroughUnionAllRule instance =
        new PushAggThroughUnionAllRule();

    public PushAggThroughUnionAllRule()
    {
        super(
            new RelOptRuleOperand(
                AggregateRel.class,
                new RelOptRuleOperand[] {
                    new RelOptRuleOperand(UnionRel.class, RelOptRule.ANY)
                }));
    }

    public void onMatch(RelOptRuleCall call)
    {
        AggregateRel aggRel = (AggregateRel) call.rels[0];
        UnionRel unionRel = (UnionRel) call.rels[1];

        if (unionRel.isDistinct()) {
            // This transformation is only valid for UNION ALL.
            // Consider t1(i) with rows (5), (5) and t2(i) with
            // rows (5), (10), and the query
            // select sum(i) from (select i from t1) union (select i from t2).
            // The correct answer is 15.  If we apply the transformation,
            // we get
            // select sum(i) from
            // (select sum(i) as i from t1) union (select sum(i) as i from t2)
            // which yields 25 (incorrect).
            return;
        }

        // NOTE jvs 24-Aug-2008:  There's a bug in this code.  When
        // you find it, please don't fix it!  Finding it is an exercise
        // in http://wiki.eigenbase.org/pub/HowToWriteAnOptimizer,
        // so we want it to stay around.

        RelNode [] unionInputs = unionRel.getInputs();
        int nUnionInputs = unionInputs.length;
        RelNode [] newUnionInputs = new RelNode[nUnionInputs];
        RelOptCluster cluster = unionRel.getCluster();

        BitSet groupByKeyMask = new BitSet();
        for (int i = 0; i < aggRel.getGroupCount(); i++) {
            groupByKeyMask.set(i);
        }

        boolean anyTransformed = false;

        // create corresponding aggs on top of each union child
        for (int i = 0; i < nUnionInputs; i++) {
            boolean alreadyUnique =
                RelMdUtil.areColumnsDefinitelyUnique(
                    unionInputs[i],
                    groupByKeyMask);

            if (alreadyUnique) {
                newUnionInputs[i] = unionInputs[i];
            } else {
                anyTransformed = true;
                newUnionInputs[i] =
                    new AggregateRel(
                        cluster,
                        unionInputs[i],
                        aggRel.getGroupCount(),
                        aggRel.getAggCallList());
            }
        }

        if (!anyTransformed) {
            // none of the children could benefit from the pushdown,
            // so bail out (preventing the infinite loop to which most
            // planners would succumb)
            return;
        }

        // create a new union whose children are the aggs created above
        UnionRel newUnionRel = new UnionRel(cluster, newUnionInputs, true);

        AggregateRel newTopAggRel = new AggregateRel(
            cluster,
            newUnionRel,
            aggRel.getGroupCount(),
            aggRel.getAggCallList());

        call.transformTo(newTopAggRel);
    }
}

// End PushAggThroughUnionAllRule.java
