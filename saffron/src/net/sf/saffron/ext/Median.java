/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
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
import net.sf.saffron.runtime.SaffronUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;


/**
 * <code>Median</code> is an aggregation which returns the Median of a set of
 * points.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 26 January, 2001
 */
public class Median implements AggregationExtender
{
    //~ Methods ---------------------------------------------------------------

    // Comparable methods
    public Comparable aggregate(Comparable value)
    {
        throw new UnsupportedOperationException();
    }

    // int methods
    public int aggregate(int value)
    {
        throw new UnsupportedOperationException();
    }

    // {Comparable,Comparable} methods
    public Comparable [] aggregate(Comparable v0,Comparable v1)
    {
        throw new UnsupportedOperationException();
    }

    public Object merge(
        Comparable value,
        Object accumulator1,
        Object accumulator2)
    {
        ((ArrayList) accumulator1).addAll((ArrayList) accumulator2);
        return accumulator1;
    }

    public Object merge(int value,Object accumulator1,Object accumulator2)
    {
        ((ArrayList) accumulator1).addAll((ArrayList) accumulator2);
        return accumulator1;
    }

    public Object merge(
        Comparable v0,
        Comparable v1,
        Object accumulator1,
        Object accumulator2)
    {
        ((ArrayList) accumulator1).addAll((ArrayList) accumulator2);
        return accumulator1;
    }

    public Object next(Comparable value,Object accumulator)
    {
        ((ArrayList) accumulator).add(value);
        return accumulator;
    }

    public Object next(int value,Object accumulator)
    {
        ((ArrayList) accumulator).add(new Integer(value));
        return accumulator;
    }

    public Object next(Comparable v0,Comparable v1,Object accumulator)
    {
        ((ArrayList) accumulator).add(new Comparable [] { v0,v1 });
        return accumulator;
    }

    public Object result(Comparable value,Object accumulator)
    {
        Object [] a = ((ArrayList) accumulator).toArray();
        Arrays.sort(a);
        if (a.length < 1) {
            return null;
        }
        int i = a.length / 2;
        return a[i];
    }

    public int result(int value,Object accumulator)
    {
        return SaffronUtil.intValue(result(null,accumulator));
    }

    public Comparable [] result(
        Comparable v0,
        Comparable v1,
        Object accumulator)
    {
        Object [] a = ((ArrayList) accumulator).toArray();
        Arrays.sort(a,new ArrayComparator());
        if (a.length < 1) {
            return null;
        }
        int i = a.length / 2;
        return (Comparable []) a[i];
    }

    public Object start(Comparable value)
    {
        return new ArrayList();
    }

    public Object start(int value)
    {
        return new ArrayList();
    }

    public Object start(Comparable v0,Comparable v1)
    {
        return new ArrayList();
    }

    //~ Inner Classes ---------------------------------------------------------

    private static class ArrayComparator implements Comparator
    {
        // implement Comparator
        public int compare(Object o,Object p)
        {
            Object [] a = (Object []) o;
            Object [] b = (Object []) p;
            for (int i = 0; i < a.length; i++) {
                int c = ((Comparable) a[i]).compareTo(b[i]);
                if (c != 0) {
                    return c;
                }
            }
            return 0;
        }
    }
}


// End Median.java
