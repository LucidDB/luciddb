/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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
package org.eigenbase.sarg;

import java.util.*;

/**
 * SargIntervalSequence represents the union of a set of disjoint
 * {@link SargInterval} instances.  (If any adjacent intervals weren't
 * disjoint, they would have been combined into one bigger one before creation
 * of the sequence.)  Intervals are maintained in coordinate order.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SargIntervalSequence
{
    private final List<SargInterval> list;

    SargIntervalSequence()
    {
        list = new ArrayList<SargInterval>();
    }

    /**
     * @return unmodifiable list representing this sequence
     */
    public List<SargInterval> getList()
    {
        return Collections.unmodifiableList(list);
    }

    void addInterval(SargInterval interval)
    {
        list.add(interval);
    }

    // override Object
    public String toString()
    {
        // Special case:  empty sequence implies empty set.
        if (list.isEmpty()) {
            return "()";
        }

        // Special case:  don't return UNION of a single interval.
        if (list.size() == 1) {
            return list.get(0).toString();
        }

        StringBuilder sb = new StringBuilder();

        sb.append(SargSetOperator.UNION);
        sb.append("(");

        for (SargInterval interval : list) {
            sb.append(" ");
            sb.append(interval);
        }
        
        sb.append(" )");
        return sb.toString();
    }
}

// End SargIntervalSequence.java
