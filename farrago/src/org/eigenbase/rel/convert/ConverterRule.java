/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.relopt.RelTrait;
import org.eigenbase.relopt.RelTraitDef;
import org.eigenbase.util.Util;


/**
 * Abstract base class for a rule which converts from one calling convention
 * to another without changing semantics.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since May 5, 2003
 */
public abstract class ConverterRule extends RelOptRule
{
    //~ Instance fields -------------------------------------------------------

    private final RelTraitSet inTraits;
    private final RelTraitSet outTraits;

    /** The RelTraitDef of traits that this ConverterRule applies to. */
    private final RelTraitDef traitDef;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a <code>ConverterRule</code>.  It is preferable to use
     * the {@link #ConverterRule(Class, RelTraitSet, RelTraitSet, String)}
     * constructor instead.
     *
     * @pre in != null
     * @pre out != null
     */
    public ConverterRule(
        Class clazz,
        CallingConvention in,
        CallingConvention out,
        String description)
    {
        this(clazz, new RelTraitSet(in), new RelTraitSet(out), description);
    }

    /**
     * Creates a <code>ConverterRule</code>
     *
     * @pre in != null
     * @pre out != null
     */
    public ConverterRule(
        Class clazz,
        RelTraitSet in,
        RelTraitSet out,
        String description)
    {
        super(new ConverterRelOptRuleOperand(clazz, in));
        assert (in != null);
        assert (out != null);

        // RelTraitSets must match in size
        assert (in.size() == out.size());

        // RelTraitSets must match in which trait is being converted.
        int traitCount = 0;
        RelTraitDef traitDef = null;
        for(int i = 0; i < in.size(); i++) {
            RelTrait inTrait = in.getTrait(i);
            RelTrait outTrait = out.getTrait(i);

            if (inTrait != null && outTrait != null) {
                traitCount++;
                traitDef = inTrait.getTraitDef();
            } else {
                Util.permAssert(
                    inTrait == null && outTrait == null,
                    "ConverterRule cannot convert one trait type to another");
            }
        }
        Util.permAssert(
            traitCount == 1,
            "ConverterRule must convert exactly one type of trait");

        this.inTraits = in;
        this.outTraits = out;
        if (description == null) {
            description = "ConverterRule<in=" + in + ",out=" + out + ">";
        }
        this.description = description;
        this.traitDef = traitDef;
    }

    //~ Methods ---------------------------------------------------------------

    public CallingConvention getOutConvention()
    {
        return (CallingConvention)outTraits.getTrait(0);
    }

    public RelTraitSet getOutTraits()
    {
        return outTraits;
    }

    public RelTraitSet getInTraits()
    {
        return inTraits;
    }

    public RelTraitDef getTraitDef()
    {
        return traitDef;
    }

    public abstract RelNode convert(RelNode rel);

    /**
     * Returns true if this rule can convert <em>any</em> relational
     * expression of the input convention.
     *
     * <p>
     * The union-to-java converter, for example, is not guaranteed, because it
     * only works on unions.
     * </p>
     */
    public boolean isGuaranteed()
    {
        return false;
    }

    public void onMatch(RelOptRuleCall call)
    {
        RelNode rel = call.rels[0];
        if (rel.getTraits().matches(inTraits)) {
            final RelNode converted = convert(rel);
            if (converted != null) {
                call.transformTo(converted);
            }
        }
    }

    private static class ConverterRelOptRuleOperand extends RelOptRuleOperand
    {
        public ConverterRelOptRuleOperand(Class clazz, RelTraitSet in)
        {
            super(clazz, in, null);
        }

        public boolean matches(RelNode rel)
        {
            // Don't apply converters to converters that operate
            // on the same RelTraitDef -- otherwise we get
            // an n^2 effect.
            if (rel instanceof ConverterRel) {
                if (((ConverterRule)getRule()).getTraitDef() ==
                    ((ConverterRel)rel).getTraitDef()) {
                    return false;
                }
            }
            return super.matches(rel);
        }
    }
}


// End ConverterRule.java
