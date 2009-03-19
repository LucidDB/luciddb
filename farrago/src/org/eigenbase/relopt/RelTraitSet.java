/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 SQLstream, Inc.
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
package org.eigenbase.relopt;

import java.util.*;

import org.eigenbase.util.*;


/**
 * RelTraitSet represents an ordered set of {@link RelTrait}s.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public class RelTraitSet
{
    //~ Instance fields --------------------------------------------------------

    private RelTrait [] traits;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a RelTraitSet with the given set of RelTraits.
     *
     * @param traits Traits
     */
    public RelTraitSet(RelTrait ... traits)
    {
        this.traits = new RelTrait[traits.length];

        for (int i = 0; i < traits.length; i++) {
            this.traits[i] = canonize(traits[i]);
        }
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Retrieves a RelTrait from the set.
     *
     * @param index 0-based index into ordered RelTraitSet
     *
     * @return the RelTrait
     *
     * @throws ArrayIndexOutOfBoundsException if index greater than or equal to
     * {@link #size()} or less than 0.
     */
    public RelTrait getTrait(int index)
    {
        return traits[index];
    }

    /**
     * Retrieves a RelTrait of the given type from the set.
     *
     * @param traitDef the type of RelTrit to retrieve
     *
     * @return the RelTrait, or null if not found
     */
    public RelTrait getTrait(RelTraitDef traitDef)
    {
        int index = findIndex(traitDef);
        if (index >= 0) {
            return getTrait(index);
        }

        return null;
    }

    /**
     * Replaces an existing RelTrait in the set. This method <b>must not</b> be
     * used to modify the traits of a {@link org.eigenbase.rel.RelNode} that has
     * already been registered with a {@link RelOptPlanner}.
     *
     * @param index 0-based index into ordered RelTraitSet
     * @param trait the new RelTrait
     *
     * @return the old RelTrait at the index
     */
    public RelTrait setTrait(int index, RelTrait trait)
    {
        RelTrait oldTrait = traits[index];

        assert (oldTrait.getTraitDef() == trait.getTraitDef()) : "RelTrait has different RelTraitDef than replacement";

        traits[index] = canonize(trait);

        return oldTrait;
    }

    /**
     * Replaces an existing RelTrait in the set. This method <b>must not</b> be
     * used to modify the traits of a {@link org.eigenbase.rel.RelNode} that has
     * already been registered with a {@link RelOptPlanner}.
     *
     * @param traitDef the type of RelTrait to replace
     *
     * @return the RelTrait at the index
     */
    public RelTrait setTrait(RelTraitDef traitDef, RelTrait trait)
    {
        assert (trait.getTraitDef() == traitDef);

        int index = findIndex(traitDef);
        Util.permAssert(index >= 0, "Could not find RelTraitDef");

        return setTrait(index, trait);
    }

    /**
     * Adds a new RelTrait to the set. This method <b>must not</b> be used to
     * modify the traits of a {@link org.eigenbase.rel.RelNode} that has already
     * been registered with a {@link RelOptPlanner}.
     *
     * @param trait the new RelTrait
     */
    public void addTrait(RelTrait trait)
    {
        int len = traits.length;
        RelTrait [] newTraits = new RelTrait[len + 1];
        for (int i = 0; i < traits.length; i++) {
            newTraits[i] = traits[i];
        }

        newTraits[len] = canonize(trait);

        traits = newTraits;
    }

    /**
     * Returns the size of the RelTraitSet.
     *
     * @return the size of the RelTraitSet.
     */
    public int size()
    {
        return traits.length;
    }

    /**
     * Converts a trait to canonical form.
     *
     * <p>After canonization, t1.equals(t2) if and only if t1 == t2.
     *
     * @param trait Trait
     *
     * @return Trait in canonical form
     */
    private RelTrait canonize(RelTrait trait)
    {
        if (trait == null) {
            return null;
        }

        return trait.getTraitDef().canonize(trait);
    }

    /**
     * Compares two RelTraitSet objects for equality.
     *
     * @param obj another RelTraitSet
     *
     * @return true if traits are equal and in the same order, false otherwise
     */
    public boolean equals(Object obj)
    {
        if (obj instanceof RelTraitSet) {
            RelTraitSet other = (RelTraitSet) obj;
            return Arrays.equals(traits, other.traits);
        }
        return false;
    }

    /**
     * Compares two RelTraitSet objects to see if they match for the purposes of
     * firing a rule. A null RelTrait within a RelTraitSet indicates a wildcard:
     * any RelTrait in the other RelTraitSet will match. If one RelTraitSet is
     * smaller than the other, comparison stops when the last RelTrait from the
     * smaller set has been examined and the remaining RelTraits in the larger
     * set are assumed to match.
     *
     * @param that another RelTraitSet
     *
     * @return true if the RelTraitSets match, false otherwise
     */
    public boolean matches(RelTraitSet that)
    {
        final int n =
            Math.min(
                this.size(),
                that.size());

        for (int i = 0; i < n; i++) {
            RelTrait thisTrait = this.traits[i];
            RelTrait thatTrait = that.traits[i];

            if ((thisTrait == null) || (thatTrait == null)) {
                continue;
            }

            if (thisTrait != thatTrait) {
                return false;
            }
        }

        return true;
    }

    /**
     * Returns whether this trait set contains a given trait.
     *
     * @param trait Sought trait
     *
     * @return Whether set contains given trait
     */
    public boolean contains(RelTrait trait)
    {
        for (RelTrait relTrait : traits) {
            if (trait == relTrait) {
                return true;
            }
        }
        return false;
    }

    /**
     * Outputs the traits of this set as a String. Traits are output in the
     * String in order, separated by periods.
     */
    public String toString()
    {
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < traits.length; i++) {
            final RelTrait trait = traits[i];
            if (i > 0) {
                s.append('.');
            }
            if ((trait == null)
                && (traits.length == 1))
            {
                // Special format for a list containing a single null trait;
                // otherwise its string appears as "null", which is the same
                // as if the whole trait set were null, and so confusing.
                s.append("{null}");
            } else {
                s.append(trait);
            }
        }
        return s.toString();
    }

    public RelTraitSet clone()
    {
        return new RelTraitSet(traits);
    }

    /**
     * Finds the index of a trait of a given type in this set.
     *
     * @param traitDef Sought trait definition
     *
     * @return index of trait, or -1 if not found
     */
    private int findIndex(RelTraitDef traitDef)
    {
        for (int i = 0; i < traits.length; i++) {
            RelTrait trait = traits[i];
            if ((trait != null) && (trait.getTraitDef() == traitDef)) {
                return i;
            }
        }

        return -1;
    }
}

// End RelTraitSet.java
