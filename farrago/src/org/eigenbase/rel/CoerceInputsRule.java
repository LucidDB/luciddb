/*
// $Id$
// Saffron preprocessor and data engine
// Copyright (C) 2002-2004 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
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

package org.eigenbase.rel;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.util.*;
import org.eigenbase.rex.*;

import java.util.*;

/**
 * CoerceInputsRule precasts inputs to a particular type.  This can be used
 * to assist operator implementations which impose requirements on their
 * input types.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class CoerceInputsRule extends RelOptRule
{
    private Class consumerRelClass;

    /**
     * Construct the rule.
     *
     * @param consumerRelClass the RelNode class which will consume
     * the inputs
     */
    public CoerceInputsRule(Class consumerRelClass)
    {
        super(
            new RelOptRuleOperand(
                consumerRelClass,
                null));
        this.consumerRelClass = consumerRelClass;
        description = "CoerceInputsRule:" + consumerRelClass.getName();
    }

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
            RelDataType expectedType =
                consumerRel.getExpectedInputRowType(i);
            RelNode input = inputs[i];
            newInputs[i] = RelOptUtil.createCastRel(input,expectedType);
            if (newInputs[i] != input) {
                coerce = true;
            }
            assert(RelOptUtil.areRowTypesEqual(
                       newInputs[i].getRowType(),
                       expectedType));
        }
        if (!coerce) {
            return;
        }
        RelNode newConsumerRel = RelOptUtil.clone(consumerRel);
        for (int i = 0; i < newInputs.length; ++i) {
            newConsumerRel.replaceInput(i,newInputs[i]);
        }
        call.transformTo(newConsumerRel);
    }
}

// End CoerceInputsRule.java
