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

import java.util.ArrayList;
import java.util.List;
import java.io.Serializable;

/**
 * FennelTupleDescriptor provides the metadata describing a tuple. This
 * is used in conjunction with FennelTupleAccessor objects to marshall
 * and unmarshall data into FennelTupleData objects from external formats.
 *
 * @author Mike Bennett
 * @version $Id$
 */
public class FennelTupleDescriptor implements Serializable
{
    /** SerialVersionUID created with JDK 1.5 serialver tool. */
    private static final long serialVersionUID = -7075506007273800588L;

    /**
     * a collection of the FennelTupleAttributeDescriptor objects
     * we're keeping.
     */ 
    private final List attrs = new ArrayList();

    /**
     * default constructor
     */
    public FennelTupleDescriptor()
    {
    }

    /**
     * Returns the number of attributes we are holding.
     */ 
    public int getAttrCount()
    {
        return attrs.size();
    }

    /**
     * Gets an FennelTupleAttributeDescriptor given an ordinal index.
     */
    public FennelTupleAttributeDescriptor getAttr(int i)
    {
        return (FennelTupleAttributeDescriptor) attrs.get(i);
    }

    /**
     * Adds a new FennelTupleAttributeDescriptor.
     *
     * @return the index where it was added
     */
    public int add(FennelTupleAttributeDescriptor newDesc)
    {
        int ndx = attrs.size();
        attrs.add(newDesc);
        return ndx;
    }

    /**
     * Indicates if any descriptors we're keeping might contain nulls.
     */
    public boolean containsNullable()
    {
        int i;
        for (i = 0; i < attrs.size(); ++i) {
            if (getAttr(i).isNullable) {
                return true;
            }
        }
        return false;
    }

    /**
     * Compares two tuples.
     *
     * @return zero if they match, -1 if the first is less than the
     * second otherwise 1
     */
    public int compareTuples(FennelTupleData tuple1, FennelTupleData tuple2)
    {
        int n = tuple1.getDatumCount();
        if (n > tuple2.getDatumCount()) {
            n = tuple2.getDatumCount();
        }
        if (n >= getAttrCount()) {
            n = getAttrCount();
        }
        int i;
        for (i = 0; i < n; ++i) {
            FennelTupleDatum datum1 = tuple1.getDatum(i);
            FennelTupleDatum datum2 = tuple2.getDatum(i);
            if (! datum1.isPresent()) {
                if (! datum2.isPresent()) {
                    continue;
                }
                return -1;
            } else if (! datum2.isPresent()) {
                return 1;
            }
            int c = getAttr(i).typeDescriptor.compareValues(datum1, datum2);
            if (c != 0) {
                if (c < 0) {
                    return -1;
                }
                return 1;
            }
        }
        return 0;
    }
};

// End FennelTupleDescriptor.java
