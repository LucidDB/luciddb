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
 * A relational expression implements the interface <code>ConverterRel</code> to
 * indicate that it converts a physical attribute, or {@link
 * org.eigenbase.relopt.RelTrait trait}, of a relational expression from one
 * value to another.
 *
 * <p>A typical example of a trait is {@link CallingConvention calling
 * convention}, and {@link net.sf.farrago.query.FennelToIteratorConverter} is an
 * example of a relational expression which converts that trait.</p>
 *
 * <p>Sometimes this conversion is expensive; for example, to convert a
 * non-distinct to a distinct object stream, we have to clone every object in
 * the input.</p>
 *
 * <p>A converter does not change the logical expression being evaluated; after
 * conversion, the number of rows and the values of those rows will still be the
 * same. By declaring itself to be a converter, a relational expression is
 * telling the planner about this equivalence, and the planner groups
 * expressions which are logically equivalent but have different physical traits
 * into groups called <code>RelSet</code>s.
 *
 * <p>In principle one could devise converters which change multiple traits
 * simultaneously (say change the sort-order and the physical location of a
 * relational expression). In which case, the method {@link #getInputTraits()}
 * would return a {@link org.eigenbase.relopt.RelTraitSet}. But for simplicity,
 * this class only allows one trait to be converted at a time; all other traits
 * are assumed to be preserved.</p>
 *
 * @author jhyde
 * @version $Id$
 * @since Dec 12, 2007
 */
public interface ConverterRel
    extends RelNode
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the trait of the input relational expression.
     *
     * @return input trait
     */
    RelTraitSet getInputTraits();

    /**
     * Returns the definition of trait which this converter works on.
     *
     * <p>The input relational expression (matched by the rule) must possess
     * this trait and have the value given by {@link #getInputTraits()}, and the
     * traits of the output of this converter given by {@link #getTraits()} will
     * have one trait altered and the other orthogonal traits will be the same.
     *
     * @return trait which this converter modifies
     */
    RelTraitDef getTraitDef();

    /**
     * Returns the sole input relational expression
     *
     * @return child relational expression
     */
    RelNode getChild();
}

// End ConverterRel.java
