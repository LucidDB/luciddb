/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package net.sf.farrago.db;

import java.util.*;

import javax.jmi.reflect.*;

import net.sf.farrago.session.*;

import org.eigenbase.jmi.*;


/**
 * FarragoDbSessionPrivilegeMap is a default implementation for {@link
 * FarragoSessionPrivilegeMap}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FarragoDbSessionPrivilegeMap
    implements FarragoSessionPrivilegeMap
{
    // TODO jvs 13-Aug-2005: factor out MultiMapUnique or whatever it's called

    //~ Instance fields --------------------------------------------------------

    private final JmiModelView modelView;

    private final Map mapTypeToSet;

    //~ Constructors -----------------------------------------------------------

    FarragoDbSessionPrivilegeMap(JmiModelView modelView)
    {
        this.modelView = modelView;
        mapTypeToSet = new HashMap();
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionPrivilegeMap
    public void mapPrivilegeForType(
        RefClass refClass,
        String privilegeName,
        boolean isLegal,
        boolean includeSubclasses)
    {
        if (includeSubclasses) {
            JmiClassVertex classVertex =
                modelView.getModelGraph().getVertexForRefClass(refClass);
            Iterator iter =
                modelView.getAllSubclassVertices(
                    classVertex).iterator();
            while (iter.hasNext()) {
                classVertex = (JmiClassVertex) iter.next();
                mapPrivilegeForType(
                    classVertex.getRefClass(),
                    privilegeName,
                    isLegal,
                    false);
            }
            return;
        }

        Set set = (Set) mapTypeToSet.get(refClass);
        if (!isLegal) {
            if (set == null) {
                return;
            }
            set.remove(privilegeName);
            return;
        }

        if (set == null) {
            set = new TreeSet();
            mapTypeToSet.put(refClass, set);
        }

        set.add(privilegeName);
    }

    // implement FarragoSessionPrivilegeMap
    public Set getLegalPrivilegesForType(RefClass refClass)
    {
        Set set = (Set) mapTypeToSet.get(refClass);
        if (set == null) {
            return Collections.EMPTY_SET;
        }
        return set;
    }

    void makeImmutable()
    {
        Iterator iter = mapTypeToSet.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            Set set = (Set) entry.getValue();
            if (set.isEmpty()) {
                iter.remove();
            } else if (set.size() == 1) {
                entry.setValue(
                    Collections.singleton(
                        set.iterator().next()));
            } else {
                entry.setValue(Collections.unmodifiableSet(set));
            }
        }
    }
}

// End FarragoDbSessionPrivilegeMap.java
