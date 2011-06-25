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
package org.eigenbase.relopt;

import java.util.*;

/**
 * RelTraitSet represents an ordered set of {@link RelTrait}s.
 *
 * <p>A trait set is immutable. To create a trait set from an existing one,
 * use methods such as {@link #plus} and {@link #plusAll(RelTraitSet)}.</p>
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public class RelTraitSet
{
    //~ Instance fields --------------------------------------------------------

    private final RelTrait [] traits;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a RelTraitSet with the given set of RelTraits.
     *
     * @see org.eigenbase.util.Util#deprecated(Object, boolean) ever null?
     *
     * @param traits Traits
     */
    public RelTraitSet(RelTrait ... traits)
    {
        this(traits.clone(), false);
    }

    /**
     * Internal constructor. Does not clone.
     */
    private RelTraitSet(RelTrait[] traits, boolean b)
    {
        this.traits = traits;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Checks that a trait set is valid.
     *
     * @param fail Whether to throw if set is invalid
     * @return Whether set is valid
     */
    public boolean isValid(boolean fail)
    {
        for (int i = 0; i < traits.length; i++) {
            RelTrait trait = traits[i];
            if (trait == null) {
                assert !fail : "null trait in " + this;
                return false;
            }
            if (trait != trait.getTraitDef().canonize(trait)) {
                assert !fail
                    : "trait " + trait + " in " + this + " is not canonical";
                return false;
            }
            for (int j = 0; j < i; j++) {
                final RelTrait trait2 = traits[j];
                if (trait2.getTraitDef() == trait.getTraitDef()) {
                    assert !fail
                        : "traitDefs are not distinct in " + this;
                    return false;
                }
            }
        }
        return true;
    }

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
     * Returns the size of the RelTraitSet.
     *
     * @return the size of the RelTraitSet.
     */
    public int size()
    {
        return traits.length;
    }

    /**
     * Returns a trait set with one trait modified.
     *
     * <p>Never modifies this trait set.
     *
     * <p>For now, always returns a new trait set, because some code relies upon
     * this behavior. In future, when RelTraitSet is immutable, this method will
     * return this trait set if the trait is already correct.
     *
     * @param trait Trait to add
     * @return Trait set that is the same as this except for the given trait
     */
    public RelTraitSet plus(RelTrait trait)
    {
        final int index = findIndex(trait.getTraitDef());
        if (index >= 0) {
            if (traits[index] == trait) {
                return this;
            } else {
                final RelTrait[] newTraits = traits.clone();
                newTraits[index] = trait;
                return new RelTraitSet(newTraits, false);
            }
        } else {
            final RelTrait[] newTraits =
                Arrays.copyOf(traits, traits.length + 1);
            newTraits[newTraits.length - 1] = trait;
            return new RelTraitSet(newTraits, false);
        }
    }


    /**
     * Creates a copy of this trait set, assigning all traits from the
     * other trait set.
     *
     * <p>This trait set is not modified.</p>
     *
     * @param traits Other traits
     * @return Copy of this trait set overwritten with other traits
     */
    public RelTraitSet plusAll(RelTraitSet traits)
    {
        RelTraitSet result = this;
        for (int i = 0; i < traits.size(); i++) {
            final RelTrait trait = traits.getTrait(i);
            result = result.plus(trait);
        }
        return result;
    }

    /**
     * Creates a copy of this trait set, assigning all traits from the
     * other trait set except those of a given type.
     *
     * <p>This trait set is not modified.</p>
     *
     * @param traits Other traits
     * @param traitDef Trait definition to preserve in copied trait set
     * @return Copy of this trait set overwritten in all except one type
     */
    public RelTraitSet plusAllExcept(
        RelTraitSet traits,
        RelTraitDef traitDef)
    {
        RelTraitSet result = this;
        for (int i = 0; i < traits.size(); i++) {
            RelTrait trait = traits.getTrait(i);
            if (trait.getTraitDef() != traitDef) {
                result = result.plus(trait);
            }
        }
        return result;
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
     * Compares two RelTraitSet objects for equality. Both must have the same
     * traits, but not necessarily in the same order.
     *
     * @param that RelTraitSet to compare with
     *
     * @return true if traits are equal, not necessarily in the same order
     */
    public boolean equalsUnordered(RelTraitSet that)
    {
        if (this.size() != that.size()) {
            return false;
        }
        for (int i = 0; i < this.size(); i++) {
            RelTrait thisTrait = traits[i];
            RelTrait thatTrait = that.getTrait(thisTrait.getTraitDef());
            if (thisTrait != thatTrait) {
                return false;
            }
        }
        return true;
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
