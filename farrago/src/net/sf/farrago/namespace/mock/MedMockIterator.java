/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
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
package net.sf.farrago.namespace.mock;

import java.util.*;


/**
 * MedMockIterator generates mock data.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class MedMockIterator implements Iterator
{
    //~ Instance fields -------------------------------------------------------

    private Object obj;
    private long nRows;

    //~ Constructors ----------------------------------------------------------

    /**
     * Constructor.
     *
     * @param obj the single object which is returned over and over
     *
     * @param nRows number of rows to generate
     */
    public MedMockIterator(
        Object obj,
        long nRows)
    {
        this.obj = obj;
        this.nRows = nRows;
    }

    //~ Methods ---------------------------------------------------------------

    // implement Iterator
    public Object next()
    {
        --nRows;
        return obj;
    }

    // implement Iterator
    public boolean hasNext()
    {
        return nRows > 0;
    }

    // implement Iterator
    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}


// End MedMockIterator.java
