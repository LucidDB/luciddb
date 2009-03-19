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
package org.eigenbase.oj.rex;

import openjava.ptree.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.rel.*;


/**
 * Translates a call to an {@link org.eigenbase.rel.Aggregation} into OpenJava
 * code.
 *
 * <p>Implementors are held in a {@link OJRexImplementorTable}.
 *
 * @author jhyde
 * @version $Id$
 */
public interface OJAggImplementor
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Generates the expression which gets called when a new total is created.
     * For <code>sum(x)</code>, this looks like <code>new
     * saffron.runtime.Holder.int_Holder(0)</code>.
     */
    Expression implementStart(
        JavaRelImplementor implementor,
        JavaRel rel,
        AggregateCall call);

    /**
     * Generates code to create a new total and to add the first value. For
     * <code>sum(x)</code>, this looks like <code>new
     * saffron.runtime.Holder.int_Holder(x)</code>.
     */
    Expression implementStartAndNext(
        JavaRelImplementor implementor,
        JavaRel rel,
        AggregateCall call);

    /**
     * Returns whether this aggregation can merge together two accumulators.
     *
     * <p>For example, <code>COUNT</code> can (you just add the accumulators);
     * <code>AVG</code> and <code>MEDIAN</code> cannot.
     *
     * @return whether this aggregation can merge together two accumulators
     */
    boolean canMerge();

    /**
     * Generates (into the current statement list, gleaned by calling <code>
     * implementor</code>'s {@link
     * org.eigenbase.oj.rel.JavaRelImplementor#getStatementList} method) code to
     * merge two accumulators. For <code>sum(x)</code>, this looks like <code>
     * ((saffron.runtime.Holder.int_Holder) accumulator).value +=
     * ((saffron.runtime.Holder.int_Holder) other).value</code>.
     *
     * <p>The method is only called if {@link #canMerge} returns <code>
     * true</code>.</p>
     *
     * @param implementor a callback object which knows how to generate things
     * @param rel the relational expression which is generating this code
     * @param accumulator the expression which holds the total
     * @param otherAccumulator accumulator to merge in
     */
    void implementMerge(
        JavaRelImplementor implementor,
        RelNode rel,
        Expression accumulator,
        Expression otherAccumulator);

    /**
     * Generates (into the current statement list, gleaned by calling <code>
     * implementor</code>'s {@link
     * org.eigenbase.oj.rel.JavaRelImplementor#getStatementList} method) the
     * piece of code which gets called each time an extra row is seen. For
     * <code>sum(x)</code>, this looks like <code>
     * ((org.eigenbase.runtime.Holder.int_Holder) accumulator).value +=
     * x</code>.
     *
     * @param implementor a callback object which knows how to generate things
     * @param rel the relational expression which is generating this code
     * @param accumulator the expression which holds the total
     * @param call the ordinals of the fields of the child row which are
     * arguments to this aggregation
     */
    void implementNext(
        JavaRelImplementor implementor,
        JavaRel rel,
        Expression accumulator,
        AggregateCall call);

    /**
     * Generates the expression which gets called when a total is complete. For
     * <code>sum(x)</code>, this looks like <code>
     * ((saffron.runtime.Holder.int_Holder) accumulator).value</code>.
     */
    Expression implementResult(
        JavaRelImplementor implementor,
        Expression accumulator,
        AggregateCall call);
}

// End OJAggImplementor.java
