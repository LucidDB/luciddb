/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.farrago.query;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rel.*;
import org.eigenbase.util.*;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexInputRef;

import java.util.*;
import java.util.List;


/**
 * FennelRenameRule is a rule for converting a rename-only Project into
 * FennelRename.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelRenameRule extends RelOptRule
{
    private CallingConvention convention;
    
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FennelRenameRule object.
     */
    public FennelRenameRule(CallingConvention convention,String description)
    {
        super(
            new RelOptRuleOperand(
                ProjectRel.class,
                new RelOptRuleOperand [] { new RelOptRuleOperand(RelNode.class,null) }));
        this.convention = convention;
        this.description = description;
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return convention;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        ProjectRel project = (ProjectRel) call.rels[0];
        if (!project.isBoxed()) {
            return;
        }

        RelNode inputRel = call.rels[1];

        int n = project.getChildExps().length;
        RelDataType inputType = inputRel.getRowType();
        if (inputType.getFieldCount() != n) {
            return;
        }
        RelDataType projType = project.getRowType();
        RelDataTypeField [] projFields = projType.getFields();
        RelDataTypeField [] inputFields = inputType.getFields();
        String [] fieldNames = new String[n];
        boolean needRename = false;
        for (int i = 0; i < n; ++i) {
            RexNode exp = project.getChildExps()[i];
            if (!(exp instanceof RexInputRef)) {
                return;
            }
            RexInputRef fieldAccess = (RexInputRef) exp;
            if (i != fieldAccess.index) {
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

        RelNode fennelInput = convert(inputRel,convention);
        if (fennelInput == null) {
            return;
        }

        if (needRename) {
            FennelRenameRel rename = new FennelRenameRel(
                project.getCluster(),fennelInput,fieldNames,convention);
            call.transformTo(rename);
        } else {
            // REVIEW:  Probably shouldn't do this.  Instead, make generic
            // RemoveTrivialProject handle this case.
            call.transformTo(fennelInput);
        }
    }
}


// End FennelRenameRule.java
