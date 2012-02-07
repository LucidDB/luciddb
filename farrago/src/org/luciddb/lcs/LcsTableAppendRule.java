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
package org.luciddb.lcs;

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * LcsTableAppendRule is a rule for converting an abstract {@link
 * TableModificationRel} into a corresponding {@link LcsTableAppendRel}.
 *
 * @author Rushan Chen
 * @version $Id$
 */
public class LcsTableAppendRule
    extends RelOptRule
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new LcsTableAppendRule object.
     */
    public LcsTableAppendRule()
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
        return FennelRel.FENNEL_EXEC_CONVENTION;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        TableModificationRel tableModification =
            (TableModificationRel) call.rels[0];

        // Target table has to be a column storage table,
        // i.e. created using sys_column_store_data_server.
        if (!(tableModification.getTable() instanceof LcsTable)) {
            return;
        }

        if (!tableModification.isFlattened()) {
            return;
        }

        // Currently only insert(append) is supported.
        if (!tableModification.isInsert()) {
            return;
        }

        RelNode inputRel = tableModification.getChild();

        // Require input types to match expected types exactly.  This
        // is accomplished by the usage of CoerceInputsRule.
        if (!RelOptUtil.areRowTypesEqual(
                inputRel.getRowType(),
                tableModification.getExpectedInputRowType(0),
                false))
        {
            return;
        }

        RelNode fennelInput =
            mergeTraitsAndConvert(
                call.rels[0].getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION,
                inputRel);
        if (fennelInput == null) {
            return;
        }

        LcsTableAppendRel clusterAppendRel =
            new LcsTableAppendRel(
                tableModification.getCluster(),
                (LcsTable) tableModification.getTable(),
                tableModification.getConnection(),
                fennelInput,
                tableModification.getOperation(),
                tableModification.getUpdateColumnList());

        call.transformTo(clusterAppendRel);
    }
}

// End LcsTableAppendRule.java
