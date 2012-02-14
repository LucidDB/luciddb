/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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

    private final Map<RefClass, Set<String>> mapTypeToSet;

    //~ Constructors -----------------------------------------------------------

    FarragoDbSessionPrivilegeMap(JmiModelView modelView)
    {
        this.modelView = modelView;
        mapTypeToSet = new HashMap<RefClass, Set<String>>();
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
            for (
                JmiClassVertex jmiClassVertex
                : modelView.getAllSubclassVertices(classVertex))
            {
                classVertex = (JmiClassVertex) jmiClassVertex;
                mapPrivilegeForType(
                    classVertex.getRefClass(),
                    privilegeName,
                    isLegal,
                    false);
            }
            return;
        }

        Set<String> set = mapTypeToSet.get(refClass);
        if (!isLegal) {
            if (set == null) {
                return;
            }
            set.remove(privilegeName);
            return;
        }

        if (set == null) {
            set = new TreeSet<String>();
            mapTypeToSet.put(refClass, set);
        }

        set.add(privilegeName);
    }

    // implement FarragoSessionPrivilegeMap
    public Set<String> getLegalPrivilegesForType(RefClass refClass)
    {
        Set<String> set = mapTypeToSet.get(refClass);
        if (set == null) {
            return Collections.emptySet();
        }
        return set;
    }

    void makeImmutable()
    {
        Iterator<Map.Entry<RefClass, Set<String>>> iter =
            mapTypeToSet.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<RefClass, Set<String>> entry = iter.next();
            Set<String> set = entry.getValue();
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
