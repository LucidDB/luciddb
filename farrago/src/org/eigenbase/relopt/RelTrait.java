/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2002-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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

/**
 * RelTrait represents the manifestation of a relational expression trait within
 * a trait definition. For example, a {@link CallingConvention#JAVA} is a trait
 * of the {@link CallingConventionTraitDef} trait definition.
 *
 * <p><a name="EqualsHashCodeNote"><u>Note about equals() and hashCode()</u></a>
 * <br>
 * If all instances of RelTrait for a paritcular RelTraitDef are defined in an
 * enumeration class and no new RelTraits can be introduced at runtime, you need
 * not override {@link #hashCode()} and {@link #equals(Object)}. If, however,
 * new RelTrait instances are generated at runtime (e.g. based on state external
 * to the planner), you must implement {@link #hashCode()} and {@link
 * #equals(Object)} for proper {@link RelTraitDef#canonize canonization} of your
 * RelTrait objects.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public interface RelTrait
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the RelTraitDef that defines this RelTrait.
     *
     * @return the RelTraitDef that defines this RelTrait
     */
    abstract RelTraitDef getTraitDef();

    /**
     * See <a href="#EqualsHashCodeNote">note about equals() and hashCode()</a>.
     */
    abstract int hashCode();

    /**
     * See <a href="#EqualsHashCodeNote">note about equals() and hashCode()</a>.
     */
    abstract boolean equals(Object o);

    /**
     * Returns a succinct name for this trait. The planner may use this String
     * to describe the trait.
     */
    abstract String toString();
}

// End RelTrait.java
