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

package org.eigenbase.relopt;

import org.eigenbase.util.Util;

import java.util.Arrays;

/**
 * RelTraitSet represents an ordered set of {@link RelTrait}s.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public class RelTraitSet
{
    private RelTrait[] traits;

    /**
     * Construct a RelTraitSet with the given set of RelTraits.
     *
     * @param traits
     */
    public RelTraitSet(RelTrait[] traits)
    {
        this.traits = new RelTrait[traits.length];

        for(int i = 0; i < traits.length; i++) {
            this.traits[i] = canonize(traits[i]);
        }
    }

    /**
     * Construct a RelTraitSet with a single RelTrait.  Equivalent to calling
     * {@link #RelTraitSet(RelTrait[])} with a value of
     * <code>new RelTrait[] { trait }</code>.
     */
    public RelTraitSet(RelTrait trait)
    {
        this(new RelTrait[] { trait });
    }

    /**
     * Retrieve a RelTrait from the set.
     *
     * @param index 0-based index into ordered RelTraitSet
     * @return the RelTrait
     * @throws ArrayIndexOutOfBoundsException if index greater than or equal
     *             to {@link #size()} or less than 0.
     */
    public RelTrait getTrait(int index)
    {
        return traits[index];
    }


    /**
     * Retrieve a RelTrait of the given type from the set.
     *
     * @param traitDef the type of RelTrit to retrieve
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
     * Replace an existing RelTrait in the set.  This method <b>must
     * not</b> be used to modify the traits of a 
     * {@link org.eigenbase.rel.RelNode} that has already been
     * registered with a {@link RelOptPlanner}.
     *
     * @param index 0-based index into ordered RelTraitSet
     * @param trait the new RelTrait
     * @return the old RelTrait at the index
     */
    public RelTrait setTrait(int index, RelTrait trait)
    {
        RelTrait oldTrait = traits[index];

        assert(oldTrait.getTraitDef() == trait.getTraitDef()):
            "RelTrait has different RelTraitDef than replacement";

        traits[index] = canonize(trait);

        return oldTrait;
    }

    /**
     * Replace an existing RelTrait in the set.  This method <b>must
     * not</b> be used to modify the traits of a 
     * {@link org.eigenbase.rel.RelNode} that has already been
     * registered with a {@link RelOptPlanner}.
     *
     * @param traitDef the type of RelTrait to replace
     * @return trait the new RelTrait
     * @return the RelTrait at the index
     */
    public RelTrait setTrait(RelTraitDef traitDef, RelTrait trait)
    {
        assert(trait.getTraitDef() == traitDef);

        int index = findIndex(traitDef);
        Util.permAssert(index >= 0, "Could not find RelTraitDef");

        return setTrait(index, trait);
    }

    /**
     * Add a new RelTrait to the set.  This method <b>must
     * not</b> be used to modify the traits of a 
     * {@link org.eigenbase.rel.RelNode} that has already been
     * registered with a {@link RelOptPlanner}.
     *
     * @param trait the new RelTrait
     */
    public void addTrait(RelTrait trait)
    {
        int len = traits.length;
        RelTrait[] newTraits = new RelTrait[len + 1];
        for(int i = 0; i < traits.length; i++) {
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


    private RelTrait canonize(RelTrait trait)
    {
        if (trait == null) {
            return null;
        }

        return trait.getTraitDef().canonize(trait);
    }

    /**
     * Compare two RelTraitSet objects for equality.
     *
     * @param obj another RelTraitSet
     * @return true if traits are equal and in the same order, false otherwise
     * @throws ClassCastException if <code>obj</code> is not a RelTraitSet.
     */
    public boolean equals(Object obj)
    {
        RelTraitSet other = (RelTraitSet)obj;

        return Arrays.equals(traits, other.traits);
    }

    /**
     * Compare two RelTraitSet objects to see if they match for the purposes
     * of firing a rule.  A null RelTrait within a RelTraitSet indicates a
     * wildcard: any RelTrait in the other RelTraitSet will match.  If one
     * RelTraitSet is smaller than the other, comparison stops when the last
     * RelTrait from the smaller set has been examined and the remaining
     * RelTraits in the larger set are assumed to match.
     *
     * @param that another RelTraitSet
     * @return true if the RelTraitSets match, false otherwise
     */
    public boolean matches(RelTraitSet that)
    {
        final int n = Math.min(this.size(), that.size());

        for(int i = 0; i < n; i++) {
            RelTrait thisTrait = this.traits[i];
            RelTrait thatTrait = that.traits[i];

            if (thisTrait == null || thatTrait == null) {
                continue;
            }

            if (thisTrait != thatTrait) {
                return false;
            }
        }

        return true;
    }

    /**
     * Output the traits of this set as a String.  Traits are output in
     * the String in order, separated by periods.
     */
    public String toString()
    {
        StringBuffer s = new StringBuffer();
        for(int i = 0; i < traits.length; i++) {
            if (i > 0) {
                s.append('.');
            }
            s.append(traits[i]);
        }
        return s.toString();
    }

    public Object clone()
    {
        return new RelTraitSet(traits);
    }

    private int findIndex(RelTraitDef traitDef)
    {
        for(int i = 0; i < traits.length; i++) {
            RelTrait trait = traits[i];
            if (trait != null && trait.getTraitDef() == traitDef) {
                return i;
            }
        }

        return -1;
    }
}
