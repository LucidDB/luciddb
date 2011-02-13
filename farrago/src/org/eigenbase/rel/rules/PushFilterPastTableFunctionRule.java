/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2011 The Eigenbase Project
// Copyright (C) 2011 SQLstream, Inc.
// Copyright (C) 2011 Dynamo BI Corporation
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
package org.eigenbase.rel.rules;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * PushFilterPastTableFunctionRule implements the rule for pushing a
 * {@link FilterRel} past a {@link TableFunctionRel}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class PushFilterPastTableFunctionRule
    extends RelOptRule
{
    public static final PushFilterPastTableFunctionRule instance =
        new PushFilterPastTableFunctionRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a PushFilterPastTableFunctionRule.
     */
    private PushFilterPastTableFunctionRule()
    {
        super(
            new RelOptRuleOperand(
                FilterRel.class,
                new RelOptRuleOperand(TableFunctionRel.class, ANY)));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        FilterRel filterRel = (FilterRel) call.rels[0];
        TableFunctionRel funcRel = (TableFunctionRel) call.rels[1];
        Set<RelColumnMapping> columnMappings = funcRel.getColumnMappings();
        if ((columnMappings == null) || (columnMappings.isEmpty())) {
            // No column mapping information, so no pushdown
            // possible.
            return;
        }

        RelNode [] funcInputs = funcRel.getInputs();
        int nFuncInputs = funcInputs.length;
        if (nFuncInputs != 1) {
            // TODO:  support more than one relational input; requires
            // offsetting field indices, similar to join
            return;
        }
        // TODO:  support mappings other than 1-to-1
        if (funcRel.getRowType().getFieldCount()
            != funcInputs[0].getRowType().getFieldCount())
        {
            return;
        }
        for (RelColumnMapping mapping : columnMappings) {
            if (mapping.iInputColumn != mapping.iOutputColumn) {
                return;
            }
            if (mapping.isDerived) {
                return;
            }
        }
        RelNode [] newFuncInputs = new RelNode[nFuncInputs];
        RelOptCluster cluster = funcRel.getCluster();
        RexNode condition = filterRel.getCondition();

        // create filters on top of each func input, modifying the filter
        // condition to reference the child instead
        RexBuilder rexBuilder = filterRel.getCluster().getRexBuilder();
        RelDataTypeField [] origFields = funcRel.getRowType().getFields();
        // TODO:  these need to be non-zero once we
        // support arbitrary mappings
        int [] adjustments = new int[origFields.length];
        for (int i = 0; i < nFuncInputs; i++) {
            RexNode newCondition =
                condition.accept(
                    new RelOptUtil.RexInputConverter(
                        rexBuilder,
                        origFields,
                        funcInputs[i].getRowType().getFields(),
                        adjustments));
            newFuncInputs[i] =
                new FilterRel(cluster, funcInputs[i], newCondition);
        }

        // create a new UDX whose children are the filters created above
        TableFunctionRel newFuncRel =
            new TableFunctionRel(
                cluster,
                funcRel.getCall(),
                funcRel.getRowType(),
                newFuncInputs);
        newFuncRel.setColumnMappings(columnMappings);
        call.transformTo(newFuncRel);
    }
}

// End PushFilterPastTableFunctionRule.java
