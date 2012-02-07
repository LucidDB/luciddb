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

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * FennelRenameRule is a rule for converting a rename-only Project into
 * FennelRename.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelRenameRule
    extends RelOptRule
{
    public static final FennelRenameRule instance = new FennelRenameRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FennelRenameRule.
     */
    private FennelRenameRule()
    {
        super(
            new RelOptRuleOperand(
                ProjectRel.class,
                ANY));
    }

    /**
     * Creates a FennelRenameRule with an explicit root operand.
     *
     * @param operand root operand, must not be null
     */
    protected FennelRenameRule(RelOptRuleOperand operand)
    {
        super(operand);
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
        ProjectRel project = (ProjectRel) call.rels[0];

        FennelRenameRel rename = renameChild(project);
        if (rename == null) {
            return;
        }

        call.transformTo(rename);
    }

    /**
     * If appropriate, creates a FennelRenameRel to replace an existing
     * ProjectRel.
     *
     * @param project the existing ProjectRel
     *
     * @return the replacement FennelRenameRel or null if the project cannot be
     * replaced with a FennelRenameRel
     */
    protected FennelRenameRel renameChild(ProjectRel project)
    {
        boolean needRename = RelOptUtil.checkProjAndChildInputs(project, true);

        // either the inputs were different or they were identical, including
        // matching field names; in the case of the latter, let
        // RemoveTrivialProjectRule handle removing the redundant project
        if (!needRename) {
            return null;
        }

        RelNode fennelInput =
            mergeTraitsAndConvert(
                project.getChild().getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION,
                project.getChild());
        if (fennelInput == null) {
            return null;
        }
        return new FennelRenameRel(
            project.getCluster(),
            fennelInput,
            RelOptUtil.getFieldNames(project.getRowType()),
            RelOptUtil.mergeTraits(
                fennelInput.getTraits(),
                new RelTraitSet(FennelRel.FENNEL_EXEC_CONVENTION)));
    }
}

// End FennelRenameRule.java
