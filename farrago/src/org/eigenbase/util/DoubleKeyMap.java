/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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
 * Implements a double key hash table
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
     * Set to true if you want to ennforces the inserting of one key pair once 
     */
    public void setEnforceUniqueness(boolean enforce)
    {
        enforceUniquness = enforce;
    }

    /**
     * @pre null != key0
     * @pre null != key1
     * @throws {@link RuntimeException} if key pair is already defined when
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
     * Convenience method to define a set of key pairs at once. All pairs will
     * be created with the same value.
     * @pre null != key0
     * @pre null != key1s
     * @pre null != keys1[i] for all 0<= i <= keys1.length
     * @throws {@link RuntimeException} if key pairs are already defined when
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
     * Convenience method to define a set of key pairs at once. All pairs will
     * be created with the same value.
     * @pre null != key0s
     * @pre null != key1
     * @pre null != keys0[i] for all 0<= i <= keys0.length
     * @throws {@link RuntimeException} if key pairs are already defined when
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
     * Convenience method to define a set of key pairs at once. All pairs will
     * be created with the same value.
     * @pre null != key0s
     * @pre null != key1s
     * @pre null != keys0[i] for all 0<= i <= keys0.length
     * @pre null != keys1[i] for all 0<= i <= keys1.length
     * @throws {@link RuntimeException} if key pairs are already defined when
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
     * @return null if key-pair not defined or the value was inserted with null
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
