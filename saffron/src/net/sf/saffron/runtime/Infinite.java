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

/**
 * <code>Infinite</code> is an empty interface which informs the optimizer
 * that an {@link java.util.Iterator} will -- or may -- never end. You can
 * then safely write code like
 * <blockquote>
 * <pre>class MultiplesOf implements Iterator {
 *     int i, n;
 *     MultiplesOf(int n) {
 *         this.n = n;
 *         this.i = 0;
 *     }
 *     public boolean hasNext() {
 *         return true;
 *     }
 *     public Object next() {
 *         return i += n;
 *     }
 *     public void remove() {
 *         throw new UnsupportedOperationException();
 *     }
 * };
 * Iterator tens = (select from new MultiplesOf(2) as twos where twos % 5 == 0);</pre>
 * </blockquote>
 * and be sure that the optimizer will not try to do anything silly like
 * writing the contents to an intermediate array.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 26 April, 2002
 */
public interface Infinite
{
}


// End Infinite.java
