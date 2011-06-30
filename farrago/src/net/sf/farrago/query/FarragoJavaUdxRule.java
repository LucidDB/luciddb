/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2006 Dynamo BI Corporation
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

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.Pair;


/**
 * FarragoJavaUdxRule is a rule for transforming an abstract {@link
 * TableFunctionRel} into a {@link FarragoJavaUdxRel}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJavaUdxRule
    extends RelOptRule
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * The singleton instance.
     */
    public static final FarragoJavaUdxRule instance = new FarragoJavaUdxRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoJavaUdxRule object.
     */
    public FarragoJavaUdxRule()
    {
        super(new RelOptRuleOperand(TableFunctionRel.class, ANY));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return CallingConvention.ITERATOR;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        TableFunctionRel callRel = (TableFunctionRel) call.rels[0];
        final RelNode [] inputs = callRel.getInputs().clone();

        for (int i = 0; i < inputs.length; i++) {
            inputs[i] =
                convert(
                    inputs[i],
                    inputs[i].getTraits().plus(CallingConvention.ITERATOR));
        }
        final RelDataType outputRowType = callRel.getCall().getType();
        FarragoJavaUdxRel javaTableFunctionRel =
            new FarragoJavaUdxRel(
                callRel.getCluster(),
                callRel.getCall(),
                outputRowType,
                null,
                inputs,
                callRel.getInputRowTypes());
        javaTableFunctionRel.setColumnMappings(callRel.getColumnMappings());

        // If there are system fields, the UDX may not provide them. To comply
        // with the TableFunctionRel's row type, we need to add extra fields by
        // projecting literals. If the consumer does not use these fields, the
        // planner may project them away later.
        RelNode child;
        if (outputRowType.equals(callRel.getRowType())) {
            child = javaTableFunctionRel;
        } else {
            final List<RelDataTypeField> fields =
                callRel.getRowType().getFieldList();
            final List<Pair<String, RexNode>> exprs =
                new ArrayList<Pair<String, RexNode>>();
            final RexBuilder rexBuilder =
                callRel.getCluster().getRexBuilder();
            List<RelDataTypeField> extraFields =
                fields.subList(
                    0, fields.size() - outputRowType.getFieldCount());
            for (RelDataTypeField extraField : extraFields) {
                exprs.add(
                    Pair.of(
                        extraField.getName(),
                        rexBuilder.makeZeroLiteral(
                            extraField.getType(), true)));
            }
            int i = 0;
            for (RelDataTypeField field : outputRowType.getFieldList()) {
                exprs.add(
                    Pair.of(
                        field.getName(),
                        (RexNode)
                            rexBuilder.makeInputRef(field.getType(), i++)));
            }
            child =
                CalcRel.createProject(
                    javaTableFunctionRel,
                    Pair.projectRight(exprs),
                    Pair.projectLeft(exprs));
        }

        call.transformTo(child);
    }
}

// End FarragoJavaUdxRule.java
