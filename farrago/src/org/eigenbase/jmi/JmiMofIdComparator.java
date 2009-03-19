/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2009 The Eigenbase Project
// Copyright (C) 2006-2009 SQLstream, Inc.
// Copyright (C) 2006-2009 LucidEra, Inc.
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
package org.eigenbase.jmi;

import java.util.*;

import javax.jmi.reflect.*;


/**
 * JmiMofIdComparator implements the {@link Comparator} interface by comparing
 * pairs of {@link RefBaseObject} instances according to their MOFID attribute.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class JmiMofIdComparator
    implements Comparator<RefBaseObject>
{
    //~ Static fields/initializers ---------------------------------------------

    public static final JmiMofIdComparator instance = new JmiMofIdComparator();

    //~ Constructors -----------------------------------------------------------

    private JmiMofIdComparator()
    {
    }

    //~ Methods ----------------------------------------------------------------

    public int compare(RefBaseObject o1, RefBaseObject o2)
    {
        return o1.refMofId().compareTo(o2.refMofId());
    }

    public boolean equals(Object obj)
    {
        return (obj instanceof JmiMofIdComparator);
    }
}

// End JmiMofIdComparator.java
