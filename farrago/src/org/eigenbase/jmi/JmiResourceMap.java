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
