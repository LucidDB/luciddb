/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
package net.sf.farrago.namespace.ftrs;

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * FtrsTableModificationRule is a rule for converting an abstract {@link
 * TableModificationRel} into a corresponding {@link FtrsTableModificationRel}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FtrsTableModificationRule
    extends RelOptRule
{
    public static final FtrsTableModificationRule instance =
        new FtrsTableModificationRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FtrsTableModificationRule.
     */
    private FtrsTableModificationRule()
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

        if (!(tableModification.getTable() instanceof FtrsTable)) {
            return;
        }

        if (!tableModification.isFlattened()) {
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
