/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
import org.eigenbase.rel.SingleRel;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.relopt.CallingConventionTraitDef;
import org.eigenbase.relopt.RelTraitDef;
import org.eigenbase.relopt.RelTrait;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.util.Util;


// REVIEW jvs 23-April-2004:  This should really be an interface
// (with a companion abstract class), providing more flexibility in
// multiple inheritance situations.

/**
 * Converts a relational expression from one {@link CallingConvention calling
 * convention} to another.
 *
 * <p>
 * Sometimes this conversion is expensive; for example, to convert a
 * non-distinct to a distinct object stream, we have to clone every object in
 * the input.
 * </p>
 */
public abstract class ConverterRel extends SingleRel
{
    //~ Instance fields -------------------------------------------------------

    protected RelTraitSet inTraits;
    public final RelTraitDef traitDef;

    //~ Constructors ----------------------------------------------------------

    /**
     * @param cluster planner's cluster
     * @param traitDef the RelTraitDef this converter converts
     * @param traits the output traits of this converter
     * @param child child rel (provides input traits)
     */
    protected ConverterRel(
        RelOptCluster cluster,
        RelTraitDef traitDef,
        RelTraitSet traits,
        RelNode child)
    {
        super(cluster, traits, child);
        this.inTraits = child.getTraits();
        this.traitDef = traitDef;
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        double dRows = child.getRows();
        double dCpu = child.getRows();
        double dIo = 0;
        return planner.makeCost(dRows, dCpu, dIo);
    }

    protected Error cannotImplement()
    {
        return Util.newInternal(getClass() + " cannot convert from "
            + inTraits + " traits");
    }

    protected CallingConvention getInputConvention()
    {
        return
            (CallingConvention)inTraits.getTrait(
                CallingConventionTraitDef.instance);
    }

    protected RelTraitSet getInputTraits()
    {
        return inTraits;
    }

    /**
     * Return a new trait set based on <code>traits</code>, with a different
     * trait for a given type of trait.  Clones <code>traits</code>, and then
     * replaces the existing trait matching <code>trait.getTraitDef()</code>
     * with <code>trait</code>.
     *
     * @param traits the set of traits to convert
     * @param trait the converted trait
     * @return a new RelTraitSet
     */
    protected static RelTraitSet convertTraits(
        RelTraitSet traits, RelTrait trait)
    {
        RelTraitSet converted = RelOptUtil.clone(traits);

        converted.setTrait(trait.getTraitDef(), trait);

        return converted;
    }

}


// End ConverterRel.java
