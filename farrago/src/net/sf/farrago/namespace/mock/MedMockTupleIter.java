/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2005-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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
package net.sf.farrago.namespace.mock;

import org.eigenbase.runtime.*;


/**
 * MedMockTupleIter generates mock data.
 *
 * @author Stephan Zuercher (adapted from MedMockIterator)
 * @version $Id$
 */
public class MedMockTupleIter
    implements TupleIter
{
    //~ Instance fields --------------------------------------------------------

    private Object obj;
    private long nRows;
    private long nRowsInit;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructor.
     *
     * @param obj the single object which is returned over and over
     * @param nRows number of rows to generate
     */
    public MedMockTupleIter(
        Object obj,
        long nRows)
    {
        this.obj = obj;
        this.nRowsInit = nRows;
        this.nRows = nRows;
    }

    //~ Methods ----------------------------------------------------------------

    // implement TupleIter
    public Object fetchNext()
    {
        if (nRows > 0) {
            --nRows;
            return obj;
        }

        return NoDataReason.END_OF_DATA;
    }

    // implement TupleIter
    public void restart()
    {
        nRows = nRowsInit;
    }

    // implement TupleIter
    public void closeAllocation()
    {
    }
}

// End MedMockTupleIter.java
