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

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.util.*;


/**
 * FtrsTableModificationRule is a rule for converting an abstract {@link
 * TableModificationRel} into a corresponding {@link FtrsTableModificationRel}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FtrsTableModificationRule extends RelOptRule
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FtrsTableModificationRule object.
     */
    public FtrsTableModificationRule()
    {
        super(new RelOptRuleOperand(
                TableModificationRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(RelNode.class, null)
                }));
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelPullRel.FENNEL_PULL_CONVENTION;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        TableModificationRel tableModification =
            (TableModificationRel) call.rels[0];

        if (!(tableModification.getTable() instanceof FtrsTable)) {
            return;
        }

        RelNode inputRel = call.rels[1];

        // Require input types to match expected types exactly.  This
        // is accomplished by the usage of CoerceInputsRule.
        if (!RelOptUtil.areRowTypesEqual(
                    inputRel.getRowType(),
                    tableModification.getExpectedInputRowType(0))) {
            return;
        }

        RelNode fennelInput =
            convert(inputRel, FennelPullRel.FENNEL_PULL_CONVENTION);
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
