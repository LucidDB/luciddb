/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
package org.eigenbase.rel;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * CoerceInputsRule precasts inputs to a particular type. This can be used to
 * assist operator implementations which impose requirements on their input
 * types.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class CoerceInputsRule
    extends RelOptRule
{
    //~ Instance fields --------------------------------------------------------

    private final Class consumerRelClass;

    private final boolean coerceNames;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs the rule.
     *
     * @param consumerRelClass the RelNode class which will consume the inputs
     * @param coerceNames if true, coerce names and types; if false, coerce type
     * only
     */
    public CoerceInputsRule(
        Class consumerRelClass,
        boolean coerceNames)
    {
        super(new RelOptRuleOperand(consumerRelClass, null));
        this.consumerRelClass = consumerRelClass;
        this.coerceNames = coerceNames;
        description = "CoerceInputsRule:" + consumerRelClass.getName();
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return CallingConvention.NONE;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        RelNode consumerRel = call.rels[0];
        if (consumerRel.getClass() != consumerRelClass) {
            // require exact match on type
            return;
        }
        RelNode [] inputs = consumerRel.getInputs();
        RelNode [] newInputs = new RelNode[inputs.length];
        boolean coerce = false;
        for (int i = 0; i < inputs.length; ++i) {
            RelDataType expectedType = consumerRel.getExpectedInputRowType(i);
            RelNode input = inputs[i];
            newInputs[i] =
                RelOptUtil.createCastRel(
                    input,
                    expectedType,
                    coerceNames);
            if (newInputs[i] != input) {
                coerce = true;
            }
            assert (RelOptUtil.areRowTypesEqual(
                newInputs[i].getRowType(),
                expectedType,
                coerceNames));
        }
        if (!coerce) {
            return;
        }
        RelNode newConsumerRel = consumerRel.clone();
        for (int i = 0; i < newInputs.length; ++i) {
            newConsumerRel.replaceInput(i, newInputs[i]);
        }
        call.transformTo(newConsumerRel);
    }
}

// End CoerceInputsRule.java
