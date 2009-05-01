/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2009 The Eigenbase Project
// Copyright (C) 2006-2009 SQLstream, Inc.
// Copyright (C) 2006-2009 LucidEra, Inc.
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

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


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
            RelNode input = inputs[i];
            final RelTraitSet traits = RelOptUtil.clone(input.getTraits());

            // copy over other traits
            for (int j = 0; j < callRel.getTraits().size(); j++) {
                RelTrait trait = callRel.getTraits().getTrait(j);
                if (trait.getTraitDef() !=
                    CallingConventionTraitDef.instance)
                {
                    if (traits.getTrait(trait.getTraitDef()) != null) {
                        traits.setTrait(trait.getTraitDef(), trait);
                    } else {
                        traits.addTrait(trait);
                    }
                }
            }
            inputs[i] =
                mergeTraitsAndConvert(
                    traits,
                    CallingConvention.ITERATOR,
                    input);
        }
        FarragoJavaUdxRel javaTableFunctionRel =
            new FarragoJavaUdxRel(
                callRel.getCluster(),
                callRel.getCall(),
                callRel.getRowType(),
                null,
                inputs);
        javaTableFunctionRel.setColumnMappings(callRel.getColumnMappings());
        call.transformTo(javaTableFunctionRel);
    }
}

// End FarragoJavaUdxRule.java
