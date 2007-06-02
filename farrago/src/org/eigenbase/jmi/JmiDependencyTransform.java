/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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
 * JmiDependencyTransform defines a transformation for use in constructing a
 * {@link JmiDependencyGraph}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface JmiDependencyTransform
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Searches a collection of candidate objects, returning only those which
     * are reachable via links corresponding to mapped incoming model edges to a
     * target object.
     *
     * @param target object to which incoming links are to be found
     * @param candidates candidate source objects
     * @param mapping mapping filter for links
     *
     * @return matching candidates
     */
    public Collection<RefObject> getSourceNeighbors(
        RefObject target,
        Collection<RefObject> candidates,
        JmiAssocMapping mapping);

    /**
     * @return true if self-loops in the dependency graph should be allowed;
     * false to automatically filter them out
     */
    public boolean shouldProduceSelfLoops();

    /**
     * @return a comparator which can be used for breaking ties in ordering, or
     * null if no tie-breaking is desired (tie-breaking provides stability
     * during diff-based testing, but adds processing overhead)
     */
    public Comparator<RefBaseObject> getTieBreaker();
}

// End JmiDependencyTransform.java
