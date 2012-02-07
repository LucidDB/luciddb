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

import org.jgrapht.*;
import org.jgrapht.traverse.*;


/**
 * JmiModelView represents an annotated view of a JMI model. Instances are
 * immutable and can be accessed concurrently by multiple threads.
 *
 * <p>TODO: support transformations such as ignoring or reversing edges.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class JmiModelView
{
    //~ Instance fields --------------------------------------------------------

    /**
     * Graph of the underlying model we are querying.
     */
    private final JmiModelGraph modelGraph;

    /**
     * Map from JmiClassVertex to ClassAttributes.
     */
    private Map<JmiClassVertex, ClassAttributes> classVertexAttributes;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new view of a model.
     *
     * @param modelGraph graph for the underlying model
     */
    public JmiModelView(JmiModelGraph modelGraph)
    {
        this.modelGraph = modelGraph;
        classVertexAttributes = new HashMap<JmiClassVertex, ClassAttributes>();
        deriveAttributes();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return graph for the underlying model
     */
    public JmiModelGraph getModelGraph()
    {
        return modelGraph;
    }

    /**
     * Gets all JmiClassVertexes representing superclasses of a class (including
     * the class itself).
     *
     * @param vertex vertex representing class of interest
     *
     * @return Set of JmiClassVertex
     */
    public Set<JmiClassVertex> getAllSuperclassVertices(JmiClassVertex vertex)
    {
        return getClassAttributes(vertex).allSuperclassVertices;
    }

    /**
     * Gets all JmiClassVertexes representing subclasses of a class (including
     * the class itself).
     *
     * @param vertex vertex representing class of interest
     *
     * @return Set of JmiClassVertex
     */
    public Set<JmiClassVertex> getAllSubclassVertices(JmiClassVertex vertex)
    {
        return getClassAttributes(vertex).allSubclassVertices;
    }

    /**
     * Gets all JmiAssocEdges representing associations outgoing from any class
     * vertex returned by getAllSuperclassVertices(vertex).
     *
     * @param vertex vertex representing class of interest
     *
     * @return Set of JmiAssocEdge
     */
    public Set<JmiAssocEdge> getInheritedOutgoingAssocEdges(
        JmiClassVertex vertex)
    {
        return getClassAttributes(vertex).inheritedOutgoingAssocEdges;
    }

    /**
     * Gets all JmiAssocEdges representing associations incoming to any class
     * vertex returned by getAllSuperclassVertices(vertex).
     *
     * @param vertex vertex representing class of interest
     *
     * @return Set of JmiAssocEdge
     */
    public Set<JmiAssocEdge> getInheritedIncomingAssocEdges(
        JmiClassVertex vertex)
    {
        return getClassAttributes(vertex).inheritedIncomingAssocEdges;
    }

    /**
     * Gets the union of getInheritedOutgoingAssocEdges() for all classes
     * returned by getAllSubclassVertices(vertex).
     *
     * @param vertex vertex representing class of interest
     *
     * @return Set of JmiAssocEdge
     */
    public Set<JmiAssocEdge> getAllOutgoingAssocEdges(JmiClassVertex vertex)
    {
        return getClassAttributes(vertex).allOutgoingAssocEdges;
    }

    /**
     * Gets the union of getInheritedIncomingAssocEdges() for all classes
     * returned by getAllSubclassVertices(vertex).
     *
     * @param vertex vertex representing class of interest
     *
     * @return Set of JmiAssocEdge
     */
    public Set<JmiAssocEdge> getAllIncomingAssocEdges(JmiClassVertex vertex)
    {
        return getClassAttributes(vertex).allIncomingAssocEdges;
    }

    private ClassAttributes getClassAttributes(JmiClassVertex classVertex)
    {
        return classVertexAttributes.get(classVertex);
    }

    private void deriveAttributes()
    {
        Iterator<JmiClassVertex> topoIter =
            new TopologicalOrderIterator(
                modelGraph.getInheritanceGraph());

        // First pass: iterate in topological order from superclasses to
        // subclasses
        List<JmiClassVertex> topoList = new ArrayList<JmiClassVertex>();
        while (topoIter.hasNext()) {
            JmiClassVertex vertex = topoIter.next();
            topoList.add(vertex);

            // Include this class in its own superclass set
            ClassAttributes attrs = new ClassAttributes();
            classVertexAttributes.put(vertex, attrs);
            attrs.allSuperclassVertices.add(vertex);

            // Collect association edges at this level
            attrs.inheritedOutgoingAssocEdges.addAll(
                (Set) modelGraph.getAssocGraph().outgoingEdgesOf(vertex));
            attrs.inheritedIncomingAssocEdges.addAll(
                (Set) modelGraph.getAssocGraph().incomingEdgesOf(vertex));

            // Agglomerate superclasses and their edges
            final List<JmiClassVertex> superVertices =
                Graphs.predecessorListOf(
                    modelGraph.getInheritanceGraph(),
                    vertex);
            for (JmiClassVertex superVertex : superVertices) {
                ClassAttributes superAttrs = getClassAttributes(superVertex);
                attrs.allSuperclassVertices.addAll(
                    superAttrs.allSuperclassVertices);
                attrs.inheritedOutgoingAssocEdges.addAll(
                    superAttrs.inheritedOutgoingAssocEdges);
                attrs.inheritedIncomingAssocEdges.addAll(
                    superAttrs.inheritedIncomingAssocEdges);
            }
        }

        // Second pass: iterate in reverse order, i.e. from subclasses to
        // superclasses
        Collections.reverse(topoList);
        topoIter = topoList.iterator();
        while (topoIter.hasNext()) {
            JmiClassVertex vertex = (JmiClassVertex) topoIter.next();

            // Include this class in its own subclass set
            ClassAttributes attrs = getClassAttributes(vertex);
            attrs.allSubclassVertices.add(vertex);
            attrs.allOutgoingAssocEdges.addAll(
                attrs.inheritedOutgoingAssocEdges);
            attrs.allIncomingAssocEdges.addAll(
                attrs.inheritedIncomingAssocEdges);

            // Agglomerate subclasses
            final List<JmiClassVertex> subVertexes =
                Graphs.successorListOf(
                    modelGraph.getInheritanceGraph(),
                    vertex);
            for (JmiClassVertex subVertex : subVertexes) {
                ClassAttributes subAttrs = getClassAttributes(subVertex);
                attrs.allSubclassVertices.addAll(
                    subAttrs.allSubclassVertices);
                attrs.allOutgoingAssocEdges.addAll(
                    subAttrs.allOutgoingAssocEdges);
                attrs.allIncomingAssocEdges.addAll(
                    subAttrs.allIncomingAssocEdges);
            }
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class ClassAttributes
    {
        final Set<JmiClassVertex> allSuperclassVertices;

        final Set<JmiClassVertex> allSubclassVertices;

        final Set<JmiAssocEdge> allOutgoingAssocEdges;

        final Set<JmiAssocEdge> allIncomingAssocEdges;

        final Set<JmiAssocEdge> inheritedOutgoingAssocEdges;

        final Set<JmiAssocEdge> inheritedIncomingAssocEdges;

        ClassAttributes()
        {
            allSuperclassVertices = new HashSet<JmiClassVertex>();
            allSubclassVertices = new HashSet<JmiClassVertex>();
            allOutgoingAssocEdges = new HashSet<JmiAssocEdge>();
            allIncomingAssocEdges = new HashSet<JmiAssocEdge>();
            inheritedOutgoingAssocEdges = new HashSet<JmiAssocEdge>();
            inheritedIncomingAssocEdges = new HashSet<JmiAssocEdge>();
        }
    }
}

// End JmiModelView.java
