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

import net.sf.saffron.core.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.util.*;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.rex.RexInputRef;

import java.util.*;
import java.util.List;


/**
 * FennelRenameRule is a rule for converting a rename-only Project into
 * FennelRename.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelRenameRule extends VolcanoRule
{
    private CallingConvention convention;
    
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FennelRenameRule object.
     */
    public FennelRenameRule(CallingConvention convention,String description)
    {
        super(
            new RuleOperand(
                ProjectRel.class,
                new RuleOperand [] { new RuleOperand(SaffronRel.class,null) }));
        this.convention = convention;
        this.description = description;
    }

    //~ Methods ---------------------------------------------------------------

    // implement VolcanoRule
    public CallingConvention getOutConvention()
    {
        return convention;
    }

    // implement VolcanoRule
    public void onMatch(VolcanoRuleCall call)
    {
        ProjectRel project = (ProjectRel) call.rels[0];
        if (!project.isBoxed()) {
            return;
        }

        SaffronRel inputRel = call.rels[1];

        int n = project.getChildExps().length;
        SaffronType inputType = inputRel.getRowType();
        if (inputType.getFieldCount() != n) {
            return;
        }
        SaffronType projType = project.getRowType();
        SaffronField [] projFields = projType.getFields();
        SaffronField [] inputFields = inputType.getFields();
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

        SaffronRel fennelInput = convert(inputRel,convention);
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
