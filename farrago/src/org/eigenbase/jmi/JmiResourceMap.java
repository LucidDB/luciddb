/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
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

import org.jgrapht.traverse.*;


// TODO jvs 6-Sep-2005:  use this in FarragoReposImpl for class name
// localization

/**
 * JmiResourceMap allows resources to be associated with JMI classes, including
 * support for inheritance from superclasses.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class JmiResourceMap
{
    //~ Instance fields --------------------------------------------------------

    private final JmiModelView modelView;

    private final Map<JmiClassVertex, String> map;

    //~ Constructors -----------------------------------------------------------

    /**
     * Loads a new map from a resource bundle. A resource in the bundle having a
     * key "[prefix][ClassName][suffix]" will be mapped to the class
     * corresponding to ClassName.
     *
     * @param modelView JMI model whose classes are to be mapped
     * @param bundle source of mappings
     * @param prefix resource key prefix, or null for none
     * @param suffix resource key suffix, or null for none
     */
    public JmiResourceMap(
        JmiModelView modelView,
        ResourceBundle bundle,
        String prefix,
        String suffix)
    {
        this.modelView = modelView;
        map = new HashMap<JmiClassVertex, String>();
        Enumeration<String> keyEnum = bundle.getKeys();
        int prefixLength = 0;
        int suffixLength = 0;
        if (prefix != null) {
            prefixLength = prefix.length();
        }
        if (suffix != null) {
            suffixLength = suffix.length();
        }
        while (keyEnum.hasMoreElements()) {
            String key = keyEnum.nextElement();
            if (prefix != null) {
                if (!key.startsWith(prefix)) {
                    continue;
                }
            }
            if (suffix != null) {
                if (!key.endsWith(suffix)) {
                    continue;
                }
            }
            String className =
                key.substring(
                    prefixLength,
                    key.length() - suffixLength);
            String value = bundle.getString(key);
            JmiClassVertex classVertex =
                modelView.getModelGraph().getVertexForClassName(className);
            if (classVertex != null) {
                map.put(classVertex, value);
            }
        }

        // To implement mapping inheritance, walk up the inheritance graph from
        // subclasses to superclasses; for each class, fill in all of its
        // subclass mappings except for the ones that are set already.  There
        // are more efficient means, but...
        List<JmiClassVertex> topoList = new ArrayList<JmiClassVertex>();
        Iterator<JmiClassVertex> topoIter =
            new TopologicalOrderIterator<JmiClassVertex, JmiInheritanceEdge>(
                modelView.getModelGraph().getInheritanceGraph());
        while (topoIter.hasNext()) {
            topoList.add(topoIter.next());
        }
        Collections.reverse(topoList);
        for (JmiClassVertex classVertex : topoList) {
            String value = map.get(classVertex);
            if (value == null) {
                continue;
            }
            final Set<JmiClassVertex> subclassVertices =
                modelView.getAllSubclassVertices(classVertex);
            for (JmiClassVertex subclassVertex : subclassVertices) {
                if (!map.containsKey(subclassVertex)) {
                    map.put(subclassVertex, value);
                }
            }
        }
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Looks up the resource associated with a class.
     *
     * @param refClass class of interest
     *
     * @return associated resource, or null if none
     */
    public String getResource(RefClass refClass)
    {
        JmiClassVertex classVertex =
            modelView.getModelGraph().getVertexForRefClass(refClass);
        String value = map.get(classVertex);
        return value;
    }
}

// End JmiResourceMap.java
