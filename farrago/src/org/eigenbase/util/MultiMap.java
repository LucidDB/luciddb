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

import org.eigenbase.relopt.RelTrait;

import java.util.*;


// REVIEW jvs 7-Jan-2003:  using inheritance from HashMap seems a little
// dangerous since method like entrySet() won't work as expected; should
// probably define a separate MultiMap interface and use aggregation rather
// than inheritance in the implementation

/**
 * Map which contains more than one value per key.
 *
 * <p>
 * You can either use a <code>MultiMap</code> as a regular map, or you can use
 * the additional methods {@link #putMulti} and {@link #getMulti}. Values are
 * returned in the order in which they were added.
 * </p>
 *
 * @author jhyde
 * @version $Id$
 *
 * @since May 18, 2003
 */
public class MultiMap<K,V>
{
    private final Map<K,Object> map = new HashMap<K,Object>();

    //~ Methods ---------------------------------------------------------------

    private Object get(K key)
    {
        return map.get(key);
    }

    private Object put(K key, V value)
    {
        return map.put(key, value);
    }

    /**
     * Returns a list of values for a given key; returns an empty list if not
     * found.
     *
     * @post return != null
     */
    public List<V> getMulti(K key)
    {
        Object o = get(key);
        if (o == null) {
            return Collections.emptyList();
        } else if (o instanceof ValueList) {
            return (ValueList<V>) o;
        } else {
            return Collections.singletonList((V) o);
        }
    }

    /**
     * Adds a value for this key.
     */
    public void putMulti(
        K key,
        V value)
    {
        final Object o = put(key, value);
        if (o != null) {
            // We knocked something out. It might be a list, or a singleton
            // object.
            ValueList<V> list;
            if (o instanceof ValueList) {
                list = (ValueList<V>) o;
            } else {
                list = new ValueList<V>();
                list.add((V) o);
            }
            list.add(value);
            map.put(key, list);
        }
    }

    /**
     * Removes a value for this key.
     */
    public boolean removeMulti(
        K key,
        V value)
    {
        final Object o = get(key);
        if (o == null) {
            // key not found, so nothing changed
            return false;
        } else {
            if (o instanceof ValueList) {
                ValueList<V> list = (ValueList<V>) o;
                if (list.remove(value)) {
                    if (list.size() == 1) {
                        // now just one value left, so forget the list, and
                        // keep its only element
                        put(key, list.get(0));
                    }
                    return true;
                } else {
                    // nothing changed
                    return false;
                }
            } else {
                if (o.equals(value)) {
                    // have just removed the last value belonging to this key,
                    // so remove the key.
                    remove(key);
                    return true;
                } else {
                    // the value they asked to remove was not the one present,
                    // so nothing changed
                    return false;
                }
            }
        }
    }

    /**
     * Like entrySet().iterator(), but returns one Map.Entry per value
     * rather than one per key.
     */
    public EntryIter entryIterMulti()
    {
        return new EntryIter();
    }

    public Object remove(K key)
    {
        return map.remove(key);
    }

    public boolean containsKey(K key)
    {
        return map.containsKey(key);
    }

    public void clear()
    {
        map.clear();
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Holder class, ensures that user's values are never interpreted as
     * multiple values.
     */
    private static class ValueList<V> extends ArrayList<V>
    {
    }

    /**
     * Implementation for entryIterMulti().  Note that this assumes that
     * empty ValueLists will never be encountered, and also preserves
     * this property when remove() is called.
     */
    private class EntryIter implements Iterator<Map.Entry<K,V>>
    {
        K key;
        Iterator<K> keyIter;
        List<V> valueList;
        Iterator<V> valueIter;

        EntryIter()
        {
            keyIter = map.keySet().iterator();
            if (keyIter.hasNext()) {
                nextKey();
            } else {
                valueList = Collections.emptyList();
                valueIter = valueList.iterator();
            }
        }

        private void nextKey()
        {
            key = keyIter.next();
            valueList = getMulti(key);
            valueIter = valueList.iterator();
        }

        public boolean hasNext()
        {
            return keyIter.hasNext() || valueIter.hasNext();
        }

        public Map.Entry<K,V> next()
        {
            if (!valueIter.hasNext()) {
                nextKey();
            }
            final K savedKey = key;
            final V value = valueIter.next();
            return new Map.Entry<K,V>() {
                    public K getKey()
                    {
                        return savedKey;
                    }

                    public V getValue()
                    {
                        return value;
                    }

                    public boolean equals(Object o)
                    {
                        throw new UnsupportedOperationException();
                    }

                    public int hashCode()
                    {
                        throw new UnsupportedOperationException();
                    }

                    public Object setValue(Object value)
                    {
                        throw new UnsupportedOperationException();
                    }
                };
        }

        public void remove()
        {
            if (valueList instanceof ValueList) {
                valueIter.remove();
                if (valueList.isEmpty()) {
                    keyIter.remove();
                }
            } else {
                keyIter.remove();
            }
        }
    }
}


// End MultiMap.java
