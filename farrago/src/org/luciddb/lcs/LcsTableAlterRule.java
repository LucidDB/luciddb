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

import java.util.*;

import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fennel.rel.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * LcsTableAlterRule is a rule for converting an abstract {@link
 * TableModificationRel} into a corresponding {@link LcsTableAppendRel} in the
 * special case where it is being processed as part of an ALTER TABLE ADD COLUMN
 * statement.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LcsTableAlterRule
    extends RelOptRule
{
    //~ Static fields/initializers ---------------------------------------------

    public static final LcsTableAlterRule instance = new LcsTableAlterRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new LcsTableAppendRule object.
     */
    private LcsTableAlterRule()
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

        // Target table has to be a column store table.
        if (!(tableModification.getTable() instanceof LcsTable)) {
            return;
        }

        if (!tableModification.isFlattened()) {
            return;
        }

        if (!tableModification.isInsert()) {
            return;
        }

        FarragoPreparingStmt stmt =
            FennelRelUtil.getPreparingStmt(tableModification);
        CwmTable oldTable = stmt.getIndexMap().getOldTableStructure();
        if (oldTable == null) {
            // Not an ALTER TABLE statement.
            return;
        }

        CwmTable newTable =
            (CwmTable) ((LcsTable) tableModification.getTable())
            .getCwmColumnSet();

        // Sanity check that ALTER TABLE added a single column.  A
        // more thorough check would verify that existing column
        // names and types stayed the same.
        assert (newTable.getFeature().size()
            == (oldTable.getFeature().size() + 1));

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

        // Project the input down to just the newly added column.  For adding a
        // UDT column, we may be dealing with multiple component fields.
        LcsIndexGuide indexGuide =
            new LcsIndexGuide(
                stmt.getFarragoTypeFactory(),
                oldTable,
                new ArrayList<FemLocalIndex>());

        int nFieldsInput = inputRel.getRowType().getFieldCount();
        int nFieldsOld = indexGuide.getFlattenedRowType().getFieldCount();
        int nFieldsNew = nFieldsInput - nFieldsOld;
        List<Integer> projection = new ArrayList<Integer>();
        for (int i = 0; i < nFieldsNew; ++i) {
            projection.add(nFieldsOld + i);
        }
        inputRel =
            CalcRel.createProject(
                inputRel,
                projection);

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

// End LcsTableAlterRule.java
