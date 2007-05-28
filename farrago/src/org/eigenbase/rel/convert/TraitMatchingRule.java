/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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
package org.eigenbase.rel.convert;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * TraitMatchingRule adapts a converter rule, restricting it to fire only when
 * its input already matches the expected output trait. This can be used with
 * {@link org.eigenbase.relopt.hep.HepPlanner} in cases where alternate
 * implementations are available and it is desirable to minimize converters.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class TraitMatchingRule
    extends RelOptRule
{

    //~ Instance fields --------------------------------------------------------

    private final ConverterRule converter;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new TraitMatchingRule.
     *
     * @param converterRule rule to be restricted; rule must take a single
     * operand expecting a single input
     */
    public TraitMatchingRule(ConverterRule converterRule)
    {
        super(
            new RelOptRuleOperand(
                converterRule.getOperand().getMatchedClass(),
                new RelOptRuleOperand[] {
                    new RelOptRuleOperand(
                        RelNode.class,
                        null)
                }));
        assert (converterRule.getOperand().getChildOperands() == null);
        description = "TraitMatchingRule: " + converterRule;
        this.converter = converterRule;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return converter.getOutConvention();
    }

    public void onMatch(RelOptRuleCall call)
    {
        RelNode input = call.rels[1];
        if (input.getTraits().matches(converter.getOutTraits())) {
            converter.onMatch(call);
        }
    }
}

// End TraitMatchingRule.java
