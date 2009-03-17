/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Copyright (C) 2005-2007 The Eigenbase Project
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
package com.lucidera.lcs;

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * LcsTableAppendRule is a rule for converting an abstract {@link
 * TableModificationRel} into a corresponding {@link LcsTableAppendRel}.
 *
 * @author Rushan Chen
 * @version $Id$
 */
public class LcsTableAppendRule
    extends RelOptRule
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new LcsTableAppendRule object.
     */
    public LcsTableAppendRule()
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

        // Target table has to be a column storage table,
        // i.e. created using sys_column_store_data_server.
        if (!(tableModification.getTable() instanceof LcsTable)) {
            return;
        }

        if (!tableModification.isFlattened()) {
            return;
        }

        // Currently only insert(append) is supported.
        if (!tableModification.isInsert()) {
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

// End LcsTableAppendRule.java
