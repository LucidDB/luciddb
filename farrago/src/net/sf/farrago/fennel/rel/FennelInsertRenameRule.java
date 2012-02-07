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
package net.sf.farrago.fennel.rel;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * FennelInsertRenameRule is a rule for converting a rename-only Project
 * underneath an insert TableModificationRel into FennelRename.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class FennelInsertRenameRule
    extends FennelRenameRule
{
    public static final FennelInsertRenameRule instance =
        new FennelInsertRenameRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FennelInsertRenameRule.
     */
    private FennelInsertRenameRule()
    {
        super(
            new RelOptRuleOperand(
                TableModificationRel.class,
                new RelOptRuleOperand(ProjectRel.class, ANY)));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        TableModificationRel origTableMod = (TableModificationRel) call.rels[0];
        if (origTableMod.getOperation()
            != TableModificationRel.Operation.INSERT)
        {
            return;
        }

        ProjectRel project = (ProjectRel) call.rels[1];
        FennelRenameRel rename = renameChild(project);
        if (rename == null) {
            return;
        }

        TableModificationRel tableMod =
            new TableModificationRel(
                origTableMod.getCluster(),
                origTableMod.getTable(),
                origTableMod.getConnection(),
                rename,
                origTableMod.getOperation(),
                origTableMod.getUpdateColumnList(),
                origTableMod.isFlattened());

        call.transformTo(tableMod);
    }
}

// End FennelInsertRenameRule.java
