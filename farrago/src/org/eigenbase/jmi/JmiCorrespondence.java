/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 SQLstream, Inc.
// Copyright (C) 2006-2007 LucidEra, Inc.
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
 * JmiCorrespondence keeps track of the correspondence between a "before"
 * version of a set of objects and an "after" version of the same set.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class JmiCorrespondence
{
    //~ Instance fields --------------------------------------------------------

    private final Map<RefObject, RefObject> beforeToAfterMap;
    private final Map<RefObject, RefObject> afterToBeforeMap;
    private final Set<RefObject> additionSet;
    private final Set<RefObject> deletionSet;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new correspondence (initially empty).
     */
    public JmiCorrespondence()
    {
        // NOTE jvs 7-Feb-2006:  use LinkedXXX to reduce non-determinism
        // in tests.
        beforeToAfterMap = new LinkedHashMap<RefObject, RefObject>();
        afterToBeforeMap = new LinkedHashMap<RefObject, RefObject>();
        additionSet = new LinkedHashSet<RefObject>();
        deletionSet = new LinkedHashSet<RefObject>();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Creates a mapping between a "before" version of an object and its
     * corresponding "after" version. One or the other side may be null (but not
     * both); this indicates that the object only exists in one set or the
     * other.
     *
     * @param beforeVersion "before" version to map
     * @param afterVersion corresponding "after" version
     */
    public void addMapping(RefObject beforeVersion, RefObject afterVersion)
    {
        assert (beforeVersion != afterVersion);
        assert (!beforeToAfterMap.containsKey(beforeVersion));
        if (afterVersion != null) {
            assert (!beforeToAfterMap.containsKey(afterVersion));
            afterToBeforeMap.put(afterVersion, beforeVersion);
        } else {
            deletionSet.add(beforeVersion);
        }
        assert (!afterToBeforeMap.containsKey(beforeVersion));
        if (beforeVersion != null) {
            assert (!afterToBeforeMap.containsKey(beforeVersion));
            beforeToAfterMap.put(beforeVersion, afterVersion);
        } else {
            additionSet.add(afterVersion);
        }
    }

    /**
     * Looks up the mapping for an object version.
     *
     * @param obj object to look up (can be either version)
     *
     * @return corresponding version (before for after or vice versa), or null
     * if no mapping exists
     */
    public RefObject getMapping(RefObject obj)
    {
        RefObject result = beforeToAfterMap.get(obj);
        if (result != null) {
            return result;
        }
        result = afterToBeforeMap.get(obj);
        return result;
    }

    /**
     * @return "before" version of set
     */
    public Set<RefObject> getBeforeSet()
    {
        return Collections.unmodifiableSet(beforeToAfterMap.keySet());
    }

    /**
     * @return "after" version of set
     */
    public Set<RefObject> getAfterSet()
    {
        return Collections.unmodifiableSet(afterToBeforeMap.keySet());
    }

    /**
     * @return set of "after" objects which have no corresponding "before"
     * objects
     */
    public Set<RefObject> getAdditionSet()
    {
        return Collections.unmodifiableSet(additionSet);
    }

    /**
     * @return set of "before" objects which have no corresponding "after"
     * objects
     */
    public Set<RefObject> getDeletionSet()
    {
        return Collections.unmodifiableSet(deletionSet);
    }

    /**
     * Adds a collection of "before" objects. Objects already having mappings
     * will be ignored. Others will be considered deletions.
     *
     * @param objs objects to add to "before" set
     */
    public void augmentBeforeSet(Collection<RefObject> objs)
    {
        for (RefObject obj : objs) {
            if (beforeToAfterMap.containsKey(obj)) {
                // already mapped
                continue;
            }
            addMapping(obj, null);
        }
    }
}

// End JmiCorrespondence.java
