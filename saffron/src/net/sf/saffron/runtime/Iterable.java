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

package net.sf.saffron.runtime;

import java.util.Iterator;


/**
 * An object is <code>Iterable</code> if it has an {@link #iterator} method to
 * create an {@link Iterator} over its elements.
 * 
 * <p>
 * Some implementations of this interface may allow only one iterator at a
 * time. For example, {@link BufferedIterator} simply restarts and returns
 * itself. Iterators received from previous calls to {@link #iterator} will
 * also restart.
 * </p>
 * 
 * <p>
 * If an object implements this interface, it can be used as a relation in a
 * saffron relational expression. For example,
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
 *
 * @since 1 May, 2002
 */
public interface Iterable
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Returns an iterator over the elements in this collection. There are no
     * guarantees over the order in which the elements are returned.
     * 
     * <p>
     * If this method is called twice on the same object, and the object is
     * not modified in between times, the iterators produced may or may not
     * be the same iterator, and may or may not return the elements in the
     * same order, but must return the same objects.
     * </p>
     */
    Iterator iterator();
}


// End Iterable.java
