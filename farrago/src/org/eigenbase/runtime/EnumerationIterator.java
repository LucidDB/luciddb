/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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

import java.util.Enumeration;
import java.util.Iterator;


/**
 * <code>EnumerationIterator</code> is an adapter which converts an {@link
 * Enumeration} into an {@link Iterator}.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 16 December, 2001
 */
public class EnumerationIterator implements Iterator
{
    //~ Instance fields -------------------------------------------------------

    Enumeration enumeration;

    //~ Constructors ----------------------------------------------------------

    public EnumerationIterator(Enumeration enumeration)
    {
        this.enumeration = enumeration;
    }

    //~ Methods ---------------------------------------------------------------

    public boolean hasNext()
    {
        return enumeration.hasMoreElements();
    }

    public Object next()
    {
        return enumeration.nextElement();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}


// End EnumerationIterator.java
