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

package net.sf.saffron.core;

/**
 * An <code>AggregationExtender</code> allows end-users to define their own
 * aggregate functions.
 * 
 * <p>
 * An aggregation extender supports one or more sets of argument types,
 * <i>T</i>. <i>T</i> can be a single type (such as {@link String} or
 * <code>int</code>), several types (such as <code>String, int</code>), or
 * empty. For each set <i>T</i>, it declares 4 methods:
 * 
 * <ul>
 * <li>
 * <code>Object start(T dummy)</code> creates an accumulator
 * </li>
 * <li>
 * <code>Object next(T dummy, Object accumulator)</code> adds a value to an
 * accumulator, and returns the accumulator
 * </li>
 * <li>
 * <code>Object merge(T dummy, Object accumulator0, Object
 * accumulator1)</code> merges the contents of <code>accumulator1</code> into
 * <code>accumulator0</code>, and returns the accumulator
 * </li>
 * <li>
 * <code>T2 result(T dummy, Object accumulator)</code> retrieves the result
 * from an accumulator
 * </li>
 * </ul>
 * 
 * (The <code>merge</code> method is actually optional. If it is not present,
 * {@link net.sf.saffron.oj.xlat.ExtenderAggregation#canMerge} returns
 * <code>false</code>.)
 * </p>
 * 
 * <p>
 * Consider the query
 * <blockquote>
 * <pre>{@link java.util.Locale} locale = Locale.getDefault();
 * {@link java.text.Collator} collator = Collator.getInstance(locale);
 * LocaleMin localeMin = new LocaleMin(collator);
 * select {localeMin.aggregate(emp.name)}
 * group by {emp.deptno}
 * from emps as emp</pre>
 * </blockquote>
 * </p>
 * 
 * <p> Here <i>T</i> is <code>String</code>, and so {@link
 * net.sf.saffron.ext.LocaleMin#start(String)} will be called when a group
 * starts, {@link net.sf.saffron.ext.LocaleMin#next(String,Object)} on each
 * row, and so forth.  </p>
 * 
 * <p>
 * Note that the aggregation object is evaluated <em>each time a group
 * starts</em>. If each department has its own locale, one could write
 * <blockquote>
 * <pre>select {new LocaleMin(Collator.getInstance(emp.dept.locale))
 *                 .aggregate(emp.name) }
 * group by {emp.dept}
 * from emps as emp</pre>
 * </blockquote>
 * </p>
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 26 January, 2001
 */
public interface AggregationExtender
{
    //~ Static fields/initializers --------------------------------------------

    /** The name of the "aggregate" method. */
    public static final String METHOD_AGGREGATE = "aggregate";

    /** The name of the "start" method. */
    public static final String METHOD_START = "start";

    /** The name of the "next" method. */
    public static final String METHOD_NEXT = "next";

    /** The name of the "result" method. */
    public static final String METHOD_RESULT = "result";

    /** The name of the "merge" method. */
    public static final String METHOD_MERGE = "merge";
}


// End AggregationExtender.java
