/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
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
package net.sf.farrago.fennel.tuple;

import java.util.*;


/**
 * FennelTupleData is an in-memory collection of independent data values, as
 * explained in the fennel tuple <a
 * href="http://fennel.sourceforge.net/doxygen/html/structTupleDesign.html">
 * design document</a>. This class is JDK 1.4 compatible.
 */
public class FennelTupleData
{
    //~ Instance fields --------------------------------------------------------

    /**
     * the TupleDatums we are responsible for.
     */
    private final List datums = new ArrayList();

    //~ Constructors -----------------------------------------------------------

    /**
     * default constructor.
     */
    public FennelTupleData()
    {
    }

    /**
     * creates a FennelTupleData object from a FennelTupleDescriptor.
     */
    public FennelTupleData(FennelTupleDescriptor tupleDesc)
    {
        compute(tupleDesc);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * obtains a FennelTupleDatum object given the index of an entry.
     */
    public FennelTupleDatum getDatum(int n)
    {
        return (FennelTupleDatum) datums.get(n);
    }

    /**
     * returns the number of datums we have.
     */
    public int getDatumCount()
    {
        return datums.size();
    }

    /**
     * adds a new FennelTupleDatum object.
     */
    public void add(FennelTupleDatum d)
    {
        datums.add(d);
    }

    /**
     * creates our FennelTupleDatum objects from a FennelTupleDescriptor.
     */
    public void compute(FennelTupleDescriptor tupleDesc)
    {
        datums.clear();
        int i;
        for (i = 0; i < tupleDesc.getAttrCount(); ++i) {
            add(new FennelTupleDatum(tupleDesc.getAttr(i).storageSize));
        }
    }

    /**
     * indicates whether this tuple contains any null FennelTupleDatum elements.
     */
    public boolean containsNull()
    {
        int i;
        for (i = 0; i < datums.size(); ++i) {
            if (!getDatum(i).isPresent()) {
                return true;
            }
        }
        return false;
    }
}
;

// End FennelTupleData.java
