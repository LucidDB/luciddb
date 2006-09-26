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
package net.sf.farrago.util;

import java.util.*;
import java.util.concurrent.*;


public class FarragoDdlLockManager
{

    //~ Instance fields --------------------------------------------------------

    private final ConcurrentHashMap<Object, Set<String>> objectsInUse =
        new ConcurrentHashMap<Object, Set<String>>();

    //~ Methods ----------------------------------------------------------------

    public void addObjectsInUse(Object context, Set<String> mofIds)
    {
        if (mofIds != null) {
            objectsInUse.put(context, mofIds);
        }
    }

    public void removeObjectsInUse(Object context)
    {
        objectsInUse.remove(context);
    }

    public boolean isObjectInUse(String mofId)
    {
        for (Set<String> s : objectsInUse.values()) {
            if (s.contains(mofId)) {
                return true;
            }
        }
        return false;
    }
}

// End FarragoDdlLockManager.java
