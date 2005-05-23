/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

package org.eigenbase.util;

import java.util.HashMap;


/**
 * Implements a double key hash table.
 * Use this class when you have two keys mapping to a value. For example.
 * <blockquote><pre><code>
 * DoubleKeyMap areaCodeMap = new DoubleKeyMap();
 * areaCodeMap.put("San Francisco", "CA", "415");
 * areaCodeMap.put("Berkeley", "CA", "510");
 * areaCodeMap.put("Berkeley", "MO", "315"); //Yes this city really exists
 * ...
 * Object obj = areaCodeMap.get("San Francisco", "CA");
 * System.out.println(obj); //outputs "415"
 * </code></pre></blockquote>
 * @author Wael Chatila
 * @since Jul 27, 2004
 * @version $Id$
 */
public class DoubleKeyMap
{
    //~ Instance fields -------------------------------------------------------

    private HashMap root;
    private boolean enforceUniquness;

    //~ Constructors ----------------------------------------------------------

    public DoubleKeyMap()
    {
        root = new HashMap();
        enforceUniquness = false;
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Enables or disables uniqueness checking.
     */
    public void setEnforceUniqueness(boolean enforce)
    {
        enforceUniquness = enforce;
    }

    /**
     * Inserts a value into the hashmap with keys key0 and key1
     * <blockquote><pre><code>
     * DoubleKeyMap areaCodeMap = new DoubleKeyMap();
     * </code></pre></blockquote>
     * @pre null != key0
     * @pre null != key1
     * @throws RuntimeException if key pair is already defined when
     *         {@link #enforceUniquness} is set to true
     */
    public void put(
        Object key0,
        Object key1,
        Object value)
    {
        Util.pre(null != key0, "null != key0");
        Util.pre(null != key1, "null != key1");
        HashMap key0Hash = (HashMap) root.get(key0);
        if (null == key0Hash) {
            key0Hash = new HashMap();
            root.put(key0, key0Hash);
        }

        if (enforceUniquness && key0Hash.containsKey(key1)) {
            throw new RuntimeException("The key-pair <" + key0.toString()
                + ", " + key1.toString() + "> is already defined");
        }
        key0Hash.put(key1, value);
    }

    /**
     * Defines a set of key pairs at once. All pairs will
     * be created with the same value. For Example
     * <blockquote><pre><code>
     * Object[] 408SantaClaraCountyCities = new Object[]{"San Jose", "Sunnyvale",...};
     * DoubleKeyMap areaCodeMap = new DoubleKeyMap();
     * areaCodeMap.put(408SantaClaraCountyCitites, "CA", "408");
     * </code></pre></blockquote>
     * is equivalent to
     * <blockquote><pre><code>
     * DoubleKeyMap areaCodeMap = new DoubleKeyMap();
     * areaCodeMap.put("San Jose", "CA", "408");
     * areaCodeMap.put("Sunnyvale", "CA", "408");
     * ...
     * </code></pre></blockquote>
     * @pre null != key0s
     * @pre null != key1
     * @pre null != keys0[i] for all 0<= i <= keys0.length
     * @throws RuntimeException if key pairs are already defined when
     *         {@link #enforceUniquness} is set to true
     */
    public void put(
        Object [] keys0,
        Object key1,
        Object value)
    {
        Util.pre(null != keys0, "null != keys0");
        for (int i = 0; i < keys0.length; i++) {
            Object key0 = keys0[i];
            put(key0, key1, value);
        }
    }

    /**
     * Defines a set of key pairs at once. All pairs will
     * be created with the same value.
     * @see {@link #put(java.lang.Object[], java.lang.Object, java.lang.Object)}
     *
     * @pre null != key0
     * @pre null != key1s
     * @pre null != keys1[i] for all 0<= i <= keys1.length
     * @throws RuntimeException if key pairs are already defined when
     *         {@link #enforceUniquness} is set to true
     */
    public void put(
        Object key0,
        Object [] keys1,
        Object value)
    {
        Util.pre(null != keys1, "null != keys1");
        for (int i = 0; i < keys1.length; i++) {
            Object key1 = keys1[i];
            put(key0, key1, value);
        }
    }

    /**
     * Defines a set of key pairs at once. All pairs will
     * be created with the same value. For Example
     * <blockquote><pre><code>
     * Object[] a = new Object[]{objA0, objA1};
     * Object[] b = new Object[]{objB0, objB1, objB2};
     * Object v = "value";
     * DoubleKeyMap dblMap = new DoubleKeyMap();
     * dblMap.put(a, b, v);
     * </code></pre></blockquote>
     * is equivalent to
     * <blockquote><pre><code>
     * dblMap.put(objA0, objB0, v);
     * dblMap.put(objA0, objB1, v);
     * dblMap.put(objA0, objB2, v);
     * dblMap.put(objA1, objB0, v);
     * dblMap.put(objA1, objB1, v);
     * dblMap.put(objA1, objB2, v);
     * </code></pre></blockquote>
     * @pre null != key0s
     * @pre null != key1s
     * @pre null != keys0[i] for all 0<= i <= keys0.length
     * @pre null != keys1[i] for all 0<= i <= keys1.length
     * @throws RuntimeException if key pairs are already defined when
     *         {@link #enforceUniquness} is set to true
     */
    public void put(
        Object [] keys0,
        Object [] keys1,
        Object value)
    {
        Util.pre(null != keys0, "null != keys0");
        Util.pre(null != keys1, "null != keys1");
        for (int i = 0; i < keys0.length; i++) {
            Object key0 = keys0[i];
            put(key0, keys1, value);
        }
    }

    /**
     * @pre null != key0
     * @pre null != key1
     * @return Returns the value inserted with the key pair (key0, key1).
     * Returns null if key pair not defined or the value was inserted with null
     */
    public Object get(
        Object key0,
        Object key1)
    {
        HashMap key0Hash = (HashMap) root.get(key0);
        if (null == key0Hash) {
            return null;
        }

        return key0Hash.get(key1);
    }
}
