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

package net.sf.saffron.runtime;

import java.lang.reflect.Array;

import java.util.Collection;
import java.util.Iterator;
import java.util.Vector;


/**
 * Miscellaneous utility functions used by generated code.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 27 October, 2001
 */
public class SaffronUtil
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Copies a vector into a <code>boolean</code> array. Equivalent to {@link
     * Vector#copyInto}.
     */
    public static boolean [] copyInto(Vector v,boolean [] a)
    {
        int n = v.size();
        if (a.length < n) {
            a = new boolean[n];
        }
        for (int i = 0; i < n; i++) {
            a[i] = ((Boolean) v.elementAt(i)).booleanValue();
        }
        return a;
    }

    /**
     * Copies a vector into a <code>int</code> array. Equivalent to {@link
     * Vector#copyInto}.
     */
    public static int [] copyInto(Vector v,int [] a)
    {
        int n = v.size();
        if (a.length < n) {
            a = new int[n];
        }
        for (int i = 0; i < n; i++) {
            a[i] = ((Integer) v.elementAt(i)).intValue();
        }
        return a;
    }

    /**
     * Copies a vector into an array. If the array is not large enough,
     * allocates a new array with the same component type as <code>a</code>.
     */
    public static Object [] copyInto(Vector v,Object [] a)
    {
        int n = v.size();
        if (a.length < n) {
            Class clazz = a.getClass().getComponentType();
            a = (Object []) Array.newInstance(clazz,n);
        }
        v.copyInto(a);
        return a;
    }

    /**
     * Copies a collection into a <code>boolean</code> array. Equivalent to
     * {@link Collection#toArray(Object[])}.
     */
    public static boolean [] copyInto(Collection v,boolean [] a)
    {
        int n = v.size();
        if (a.length < n) {
            a = new boolean[n];
        }
        int i = 0;
        for (Iterator iter = v.iterator(); iter.hasNext();) {
            a[i++] = ((Boolean) iter.next()).booleanValue();
        }
        return a;
    }

    /**
     * Copies a collection into an <code>int</code> array. Equivalent to
     * {@link Collection#toArray(Object[])}.
     */
    public static int [] copyInto(Collection v,int [] a)
    {
        int n = v.size();
        if (a.length < n) {
            a = new int[n];
        }
        int i = 0;
        for (Iterator iter = v.iterator(); iter.hasNext();) {
            a[i++] = ((Integer) iter.next()).intValue();
        }
        return a;
    }

    /**
     * Copies a collection into an array. If the array is not large enough,
     * allocates a new array with the same component type as <code>a</code>.
     * Equivalent to {@link Collection#toArray(Object[])}.
     */
    public static Object [] copyInto(Collection c,Object [] a)
    {
        return c.toArray(a);
    }

    /**
     * Converts an Object to an double.
     */
    public static final double doubleValue(Object o)
    {
        return (o == null) ? 0.0 : ((Double) o).doubleValue();
    }

    // ------------------------------------------------------------------------
    // Methods to convert wrapped primitives (Integer, Double, etc.) to
    // primitives (int, double, etc.). Returns 0 if the object is null. Add
    // more as needed.

    /**
     * Converts an Object to an int.
     */
    public static final int intValue(Object o)
    {
        return (o == null) ? 0 : ((Integer) o).intValue();
    }

    /**
     * Looks up a class by name, returns an array of one class if the class is
     * not found, an array of zero classes if not.
     */
    public static Class [] classesForName(String name)
    {
        try {
            return new Class [] { Class.forName(name) };
        } catch (ClassNotFoundException e) {
            return new Class[0];
        }
    }

    /**
     * Returns whether two objects are equal or are both null.
     */
    public static boolean equals(Object o,Object p)
    {
        if (o == null) {
            return p == null;
        } else if (p == null) {
            return false;
        } else {
            return o.equals(p);
        }
    }
}


// End SaffronUtil.java
