/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.saffron.rel;

import net.sf.saffron.core.*;
import net.sf.saffron.opt.RelImplementor;

import openjava.mop.OJClass;

import openjava.ptree.Expression;


/**
 * An <code>Aggregation</code> aggregates a set of values into one value.
 * 
 * <p>
 * It is used, via a {@link net.sf.saffron.rel.AggregateRel.Call}, in an {@link
 * AggregateRel} relational operator.
 * </p>
 * 
 * <p> To help you understand the terminology, here are some analogies: an
 * {@link Aggregation} is analogous to a {@link openjava.mop.OJMethod}, whereas
 * a {@link net.sf.saffron.rel.AggregateRel.Call} is analogous to a {@link
 * openjava.ptree.MethodCall}. {@link net.sf.saffron.core.AggregationExtender}
 * has no direct analog in Java: it is more like a function object in JScript.
 * </p>
 * 
 * <p> For user-defined aggregates, use you should generally use {@link
 * net.sf.saffron.core.AggregationExtender}; writing a new
 * <code>Aggregation</code> is a complicated task, akin to writing a new
 * relational operator ({@link SaffronRel}).  </p>
 *
 * @author jhyde
 * @version $Id$
 *
 * @see net.sf.saffron.core.AggregationExtender
 * @since 26 January, 2001
 */
public interface Aggregation
{
    //~ Methods ---------------------------------------------------------------

    SaffronType [] getParameterTypes(SaffronTypeFactory typeFactory);

    SaffronType getReturnType(SaffronTypeFactory typeFactory);

    /**
     * Whether this aggregation can merge together two accumulators.
     * <code>count</code> can (you just add the accumulators);
     * <code>avg</code> and {@link net.sf.saffron.ext.Nth} cannot.
     */
    boolean canMerge();

    /**
     * Generates (into the current statement list, gleaned by calling
     * <code>implementor</code>'s {@link
     * net.sf.saffron.opt.RelImplementor#getStatementList} method) code to
     * merge two accumulators. For <code>sum(x)</code>, this looks like
     * <code>((saffron.runtime.Holder.int_Holder) accumulator).value +=
     * ((saffron.runtime.Holder.int_Holder) other).value</code>.
     * 
     * <p>
     * The method is only called if {@link #canMerge} returns
     * <code>true</code>.
     * </p>
     *
     * @param implementor a callback object which knows how to generate things
     * @param rel the relational expression which is generating this code
     * @param accumulator the expression which holds the total
     * @param otherAccumulator accumulator to merge in
     */
    void implementMerge(
        RelImplementor implementor,
        SaffronRel rel,
        Expression accumulator,
        Expression otherAccumulator);

    /**
     * Generates (into the current statement list, gleaned by calling
     * <code>implementor</code>'s {@link
     * net.sf.saffron.opt.RelImplementor#getStatementList} method) the piece of code
     * which gets called each time an extra row is seen. For
     * <code>sum(x)</code>, this looks like
     * <code>((net.sf.saffron.runtime.Holder.int_Holder) accumulator).value +=
     * x</code>.
     *
     * @param implementor a callback object which knows how to generate things
     * @param rel the relational expression which is generating this code
     * @param accumulator the expression which holds the total
     * @param args the ordinals of the fields of the child row which are
     *        arguments to this aggregation
     */
    void implementNext(
        RelImplementor implementor,
        SaffronRel rel,
        Expression accumulator,
        int [] args);

    /**
     * Generates the expression which gets called when a total is complete.
     * For <code>sum(x)</code>, this looks like <code>
     * ((saffron.runtime.Holder.int_Holder) accumulator).value</code>.
     */
    Expression implementResult(Expression accumulator);

    /**
     * Generates the expression which gets called when a new total is created.
     * For <code>sum(x)</code>, this looks like <code>new
     * saffron.runtime.Holder.int_Holder(0)</code>.
     */
    Expression implementStart(
        RelImplementor implementor,
        SaffronRel rel,
        int [] args);

    /**
     * Generates code to create a new total and to add the first value. For
     * <code>sum(x)</code>, this looks like <code>new
     * saffron.runtime.Holder.int_Holder(x)</code>.
     */
    Expression implementStartAndNext(
        RelImplementor implementor,
        SaffronRel rel,
        int [] args);
}


// End Aggregation.java
