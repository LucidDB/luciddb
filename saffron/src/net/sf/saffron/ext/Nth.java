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

package net.sf.saffron.ext;

import net.sf.saffron.core.AggregationExtender;


/**
 * <code>Nth</code> is an example of a custom aggregation. It returns the the
 * <i>n</i><sup>th</sup> of a set of rows, or returns null (0 for a primitive
 * type) if there are not that many rows. It implements the overloaded
 * <code>T aggregate(T)</code> method for T = {<code>int</code>,
 * <code>double</code>, <code>Object</code>}. There is a start, next, and
 * result method for each.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 11 February, 2002
 */
public class Nth implements AggregationExtender
{
    //~ Instance fields -------------------------------------------------------

    private int n;

    //~ Constructors ----------------------------------------------------------

    public Nth(int n)
    {
        this.n = n;
    }

    //~ Methods ---------------------------------------------------------------

    // int methods
    public int aggregate(int value)
    {
        throw new UnsupportedOperationException();
    }

    // double methods
    public double aggregate(double value)
    {
        throw new UnsupportedOperationException();
    }

    // Object methods
    public Object aggregate(Object value)
    {
        throw new UnsupportedOperationException();
    }

    public Object next(int value,Object accumulator)
    {
        Holder_int holder = (Holder_int) accumulator;
        if (holder.count++ == n) {
            holder.result = value;
        }
        return holder;
    }

    public Object next(double value,Object accumulator)
    {
        Holder_double holder = (Holder_double) accumulator;
        if (holder.count++ == n) {
            holder.result = value;
        }
        return holder;
    }

    public Object next(Object value,Object accumulator)
    {
        Holder_Object holder = (Holder_Object) accumulator;
        if (holder.count++ == n) {
            holder.result = value;
        }
        return holder;
    }

    public int result(int value,Object accumulator)
    {
        Holder_int holder = (Holder_int) accumulator;
        return holder.result;
    }

    public double result(double value,Object accumulator)
    {
        Holder_double holder = (Holder_double) accumulator;
        return holder.result;
    }

    public Object result(Object value,Object accumulator)
    {
        Holder_Object holder = (Holder_Object) accumulator;
        return holder.result;
    }

    public Object start(int value)
    {
        return new Holder_int();
    }

    public Object start(double value)
    {
        return new Holder_double();
    }

    public Object start(Object value)
    {
        return new Holder_Object();
    }

    //~ Inner Classes ---------------------------------------------------------

    private static class Holder_Object
    {
        Object result;
        int count;
    }

    private static class Holder_double
    {
        double result;
        int count;
    }

    private static class Holder_int
    {
        int count;
        int result;
    }
}


// End Nth.java
