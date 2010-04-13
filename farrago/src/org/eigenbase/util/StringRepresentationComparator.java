/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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
package org.eigenbase.util;

import java.util.*;


/**
 * StringRepresentationComparator compares two objects by comparing their {@link
 * Object#toString()} representations.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class StringRepresentationComparator<T>
    implements Comparator<T>
{
    //~ Methods ----------------------------------------------------------------

    // implement Comparator
    public int compare(T o1, T o2)
    {
        return o1.toString().compareTo(o2.toString());
    }

    // implement Comparator
    public boolean equals(Object obj)
    {
        return obj.getClass().getName().equals(getClass().getName());
    }

    public int hashCode()
    {
        return getClass().getName().hashCode();
    }
}

// End StringRepresentationComparator.java
