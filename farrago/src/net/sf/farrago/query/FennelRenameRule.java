/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
package net.sf.farrago.query;

import java.util.*;
import java.util.List;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexNode;
import org.eigenbase.util.*;


/**
 * FennelRenameRule is a rule for converting a rename-only Project into
 * FennelRename.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelRenameRule extends RelOptRule
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FennelRenameRule object.
     */
    public FennelRenameRule()
    {
        super(new RelOptRuleOperand(
                ProjectRel.class,
                null));
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelRel.FENNEL_EXEC_CONVENTION;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        ProjectRel project = (ProjectRel) call.rels[0];
        if (!project.isBoxed()) {
            return;
        }

        RelNode inputRel = project.getChild();

        int n = project.getProjectExps().length;
        RelDataType inputType = inputRel.getRowType();
        if (inputType.getFieldList().size() != n) {
            return;
        }
        RelDataType projType = project.getRowType();
        RelDataTypeField [] projFields = projType.getFields();
        RelDataTypeField [] inputFields = inputType.getFields();
        String [] fieldNames = new String[n];
        boolean needRename = false;
        for (int i = 0; i < n; ++i) {
            RexNode exp = project.getProjectExps()[i];
            if (!(exp instanceof RexInputRef)) {
                return;
            }
            RexInputRef fieldAccess = (RexInputRef) exp;
            if (i != fieldAccess.getIndex()) {
                // can't support reorder yet
                return;
            }
            String inputFieldName = inputFields[i].getName();
            String projFieldName = projFields[i].getName();
            if (!projFieldName.equals(inputFieldName)) {
                needRename = true;
            }
            fieldNames[i] = projFieldName;
        }

        if (!needRename) {
            // let RemoveTrivialProjectRule handle this case
            return;
        }

        RelNode fennelInput =
            mergeTraitsAndConvert(project.getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION, inputRel);
        if (fennelInput == null) {
            return;
        }

        FennelRenameRel rename =
            new FennelRenameRel(
                project.getCluster(),
                fennelInput,
                fieldNames,
                RelOptUtil.mergeTraits(
                    fennelInput.getTraits(),
                    new RelTraitSet(FennelRel.FENNEL_EXEC_CONVENTION)));
        call.transformTo(rename);
    }
}


// End FennelRenameRule.java
