/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
public class MultiMap extends HashMap
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Returns a list of values for a given key; returns an empty list if not
     * found.
     *
     * @post return != null
     */
    public List getMulti(Object key)
    {
        Object o = get(key);
        if (o == null) {
            return Collections.EMPTY_LIST;
        } else if (o instanceof ValueList) {
            return (ValueList) o;
        } else {
            return Collections.singletonList(o);
        }
    }

    /**
     * Adds a value for this key.
     */
    public void putMulti(
        Object key,
        Object value)
    {
        final Object o = put(key, value);
        if (o != null) {
            // We knocked something out. It might be a list, or a singleton
            // object.
            ValueList list;
            if (o instanceof ValueList) {
                list = (ValueList) o;
            } else {
                list = new ValueList();
                list.add(o);
            }
            list.add(value);
            put(key, list);
        }
    }

    /**
     * Like entrySet().iterator(), but returns one Map.Entry per value
     * rather than one per key.
     */
    public Iterator entryIterMulti()
    {
        return new EntryIter();
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Holder class, ensures that user's values are never interpreted as
     * multiple values.
     */
    private static class ValueList extends ArrayList
    {
    }

    /**
     * Implementation for entryIterMulti().  Note that this assumes that
     * empty ValueLists will never be encountered, and also preserves
     * this property when remove() is called.
     */
    private class EntryIter implements Iterator
    {
        Object key;
        Iterator keyIter;
        List valueList;
        Iterator valueIter;

        EntryIter()
        {
            keyIter = keySet().iterator();
            if (keyIter.hasNext()) {
                nextKey();
            } else {
                valueList = Collections.EMPTY_LIST;
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

        public Object next()
        {
            if (!valueIter.hasNext()) {
                nextKey();
            }
            final Object savedKey = key;
            final Object value = valueIter.next();
            return new Map.Entry() {
                    public Object getKey()
                    {
                        return savedKey;
                    }

                    public Object getValue()
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
