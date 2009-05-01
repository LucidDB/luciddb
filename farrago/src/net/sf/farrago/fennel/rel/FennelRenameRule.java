/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2003-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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
