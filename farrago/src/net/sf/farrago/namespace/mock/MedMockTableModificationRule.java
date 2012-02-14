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
package net.sf.farrago.namespace.mock;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * MockTableModificationRule is a rule for converting an abstract {@link
 * TableModificationRel} into a corresponding local mock table update (always
 * returning rowcount 0, since local mock tables never store any data).
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMockTableModificationRule
    extends RelOptRule
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new MockTableModificationRule object.
     */
    public MedMockTableModificationRule()
    {
        super(
            new RelOptRuleOperand(
                TableModificationRel.class,
                ANY));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return CallingConvention.ITERATOR;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        TableModificationRel tableModification =
            (TableModificationRel) call.rels[0];

        // TODO jvs 13-Sept-2004:  disallow updates to mock foreign tables
        if (!(tableModification.getTable() instanceof MedMockColumnSet)) {
            return;
        }

        MedMockColumnSet targetColumnSet =
            (MedMockColumnSet) tableModification.getTable();

        // create a 1-row column set with the correct type for rowcount;
        // single value returned will be 0, which is what we want
        MedMockColumnSet rowCountColumnSet =
            new MedMockColumnSet(
                targetColumnSet.server,
                targetColumnSet.getLocalName(),
                tableModification.getRowType(),
                1,
                targetColumnSet.executorImpl,
                null);

        call.transformTo(
            rowCountColumnSet.toRel(
                tableModification.getCluster(),
                tableModification.getConnection()));
    }
}

// End MedMockTableModificationRule.java
