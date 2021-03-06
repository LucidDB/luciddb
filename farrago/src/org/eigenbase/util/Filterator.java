/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package org.eigenbase.util;

import java.util.*;


/**
 * Filtered iterator class: an iterator that includes only elements that are
 * instanceof a specified class. Apologies for the dorky name.
 *
 * @author jason
 * @version $Id$
 * @see Util#cast(List, Class)
 * @see Util#cast(Iterator, Class)
 * @since November 9, 2004
 */
public class Filterator<E>
    implements Iterator<E>
{
    //~ Instance fields --------------------------------------------------------

    Class<E> includeFilter;
    Iterator<? extends Object> iterator;
    E lookAhead;
    boolean ready;

    //~ Constructors -----------------------------------------------------------

    public Filterator(Iterator<?> iterator, Class<E> includeFilter)
    {
        this.iterator = iterator;
        this.includeFilter = includeFilter;
    }

    //~ Methods ----------------------------------------------------------------

    /* (non-Javadoc)
     * @see java.util.Iterator#hasNext()
     */
    public boolean hasNext()
    {
        if (ready) {
            // Allow hasNext() to be called repeatedly.
            return true;
        }

        // look ahead to see if there are any additional elements
        try {
            lookAhead = next();
            ready = true;
            return true;
        } catch (NoSuchElementException e) {
            ready = false;
            return false;
        }
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#next()
     */
    public E next()
    {
        if (ready) {
            E o = lookAhead;
            ready = false;
            return o;
        }

        while (iterator.hasNext()) {
            Object o = iterator.next();
            if (includeFilter.isInstance(o)) {
                return includeFilter.cast(o);
            }
        }
        throw new NoSuchElementException();
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#remove()
     */
    public void remove()
    {
        iterator.remove();
    }
}

// End Filterator.java
