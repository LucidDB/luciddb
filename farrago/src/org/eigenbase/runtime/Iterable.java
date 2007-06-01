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
package org.eigenbase.runtime;

import java.util.*;


/**
 * An object is <code>Iterable</code> if it has an {@link #iterator} method to
 * create an {@link Iterator} over its elements.
 *
 * <p>Some implementations of this interface may allow only one iterator at a
 * time. For example, {@link BufferedIterator} simply restarts and returns
 * itself. Iterators received from previous calls to {@link #iterator} will also
 * restart.</p>
 *
 * <p>If an object implements this interface, it can be used as a relation in a
 * saffron relational expression. For example,
 *
 * <blockquote>
 * <pre>Iterable iterable = new Iterable() {
 *     public Iterator iterator() {
 *         ArrayList list = new ArrayList();
 *         list.add(new Integer(1));
 *         list.add(new Integer(2));
 *         return list.iterator();
 *     }
 * };
 * for (i in (Integer[]) iterable) {
 *     print(i.intValue());
 * }</pre>
 * </blockquote>
 * </p>
 *
 * @author jhyde
 * @version $Id$
 * @since 1 May, 2002
 */
public interface Iterable
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Returns an iterator over the elements in this collection. There are no
     * guarantees over the order in which the elements are returned.
     *
     * <p>If this method is called twice on the same object, and the object is
     * not modified in between times, the iterators produced may or may not be
     * the same iterator, and may or may not return the elements in the same
     * order, but must return the same objects.</p>
     */
    Iterator iterator();
}

// End Iterable.java
