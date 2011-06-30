/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2002 SQLstream, Inc.
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
package org.eigenbase.rel.convert;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * Abstract base class for a rule which converts from one calling convention to
 * another without changing semantics.
 *
 * @author jhyde
 * @version $Id$
 * @since May 5, 2003
 */
public abstract class ConverterRule
    extends RelOptRule
{
    //~ Instance fields --------------------------------------------------------

    private final RelTrait inTrait;
    private final RelTrait outTrait;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a <code>ConverterRule</code>.
     *
     * @param clazz Type of relational expression to consider converting
     * @param in Trait of relational expression to consider converting
     * @param out Trait which is converted to
     * @param description Description of rule
     *
     * @pre in != null
     * @pre out != null
     */
    public ConverterRule(
        Class<? extends RelNode> clazz,
        RelTrait in,
        RelTrait out,
        String description)
    {
        super(
            new RelOptRuleOperand(
                clazz,
                new ConverterRuleOperandPredicate(in),
                ANY),
            description == null
                ? "ConverterRule<in=" + in + ",out=" + out + ">"
                : description);
        assert (in != null);
        assert (out != null);

        // Source and target traits must have same type
        assert in.getTraitDef() == out.getTraitDef();

        this.inTrait = in;
        this.outTrait = out;
    }

    //~ Methods ----------------------------------------------------------------

    public CallingConvention getOutConvention()
    {
        return (CallingConvention) outTrait;
    }

    public RelTrait getOutTrait()
    {
        return outTrait;
    }

    public RelTrait getInTrait()
    {
        return inTrait;
    }

    public RelTraitDef getTraitDef()
    {
        return inTrait.getTraitDef();
    }

    public abstract RelNode convert(RelNode rel);

    /**
     * Returns true if this rule can convert <em>any</em> relational expression
     * of the input convention.
     *
     * <p>The union-to-java converter, for example, is not guaranteed, because
     * it only works on unions.</p>
     */
    public boolean isGuaranteed()
    {
        return false;
    }

    public void onMatch(RelOptRuleCall call)
    {
        RelNode rel = call.rels[0];
        if (rel.getTraits().contains(inTrait)) {
            final RelNode converted = convert(rel);
            if (converted != null) {
                call.transformTo(converted);
            }
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Predicate that ensures that a relexp only matches the rule if it is
     * of the correct trait.
     */
    private static class ConverterRuleOperandPredicate
        implements RelOptRuleOperand.Predicate
    {
        private final RelTrait trait;
        private final RelTraitDef traitDef;

        ConverterRuleOperandPredicate(RelTrait trait)
        {
            assert trait != null;
            this.trait = trait;
            this.traitDef = trait.getTraitDef();
        }

        public boolean evaluate(RelNode rel)
        {
            // Don't apply converters to converters that operate
            // on the same RelTraitDef -- otherwise we get
            // an n^2 effect.
            return rel.getTraits().contains(trait)
                && !(rel instanceof ConverterRel
                     && traitDef == ((ConverterRel) rel).getTraitDef());
        }

        public int hashCode()
        {
            return trait.hashCode();
        }

        public boolean equals(Object obj)
        {
            return obj instanceof ConverterRuleOperandPredicate
                && trait.equals(((ConverterRuleOperandPredicate) obj).trait);
        }
    }
}

// End ConverterRule.java
