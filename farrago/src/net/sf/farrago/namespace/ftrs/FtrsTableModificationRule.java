/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
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

package net.sf.farrago.namespace.ftrs;

import net.sf.farrago.query.*;

import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.util.*;


/**
 * FtrsTableModificationRule is a rule for converting an abstract
 * TableModification into a corresponding FtrsTableModificationRel.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FtrsTableModificationRule extends VolcanoRule
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FtrsTableModificationRule object.
     */
    public FtrsTableModificationRule()
    {
        super(
            new RuleOperand(
                TableModificationRel.class,
                new RuleOperand [] { new RuleOperand(SaffronRel.class,null) }));
    }

    //~ Methods ---------------------------------------------------------------

    // implement VolcanoRule
    public CallingConvention getOutConvention()
    {
        return FennelRel.FENNEL_CALLING_CONVENTION;
    }

    // implement VolcanoRule
    public void onMatch(VolcanoRuleCall call)
    {
        TableModificationRel tableModification =
            (TableModificationRel) call.rels[0];

        if (!(tableModification.getTable() instanceof FtrsTable)) {
            return;
        }

        SaffronRel inputRel = call.rels[1];

        // Require input types to match expected types exactly.  This
        // is accomplished by the usage of CoerceInputsRule.
        if (!OptUtil.areRowTypesEqual(
                inputRel.getRowType(),
                tableModification.getExpectedInputRowType(0)))
        {
            return;
        }
        
        SaffronRel fennelInput =
            convert(planner,inputRel,FennelRel.FENNEL_CALLING_CONVENTION);
        if (fennelInput == null) {
            return;
        }

        FtrsTableModificationRel fennelModificationRel =
            new FtrsTableModificationRel(
                tableModification.getCluster(),
                (FtrsTable) tableModification.getTable(),
                tableModification.getConnection(),
                fennelInput,
                tableModification.getOperation(),
                tableModification.getUpdateColumnList());

        call.transformTo(fennelModificationRel);
    }
}


// End FtrsTableModificationRule.java
