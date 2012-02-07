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

import org.jgrapht.*;
import org.jgrapht.graph.*;


/**
 * JmiDependencyGraph is a directed graph representation of the dependencies
 * spanning a set of JMI objects. Vertices are instances of {@link
 * JmiDependencyVertex}. Graph instances are immutable and can be accessed
 * concurrently by multiple threads.
 *
 * <p>The rules for determining dependencies from a set of JMI objects and their
 * associations are supplied to the constructor via an instance of {@link
 * JmiDependencyTransform}. This allows the caller to define the dependencies of
 * interest for a particular model or application.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class JmiDependencyGraph
    extends UnmodifiableDirectedGraph<JmiDependencyVertex, DefaultEdge>
{
    //~ Instance fields --------------------------------------------------------

    /**
     * The underlying graph structure; we hide it here so that it can only be
     * modified internally.
     */
    private final DirectedGraph<JmiDependencyVertex, DefaultEdge> mutableGraph;

    private final JmiDependencyTransform transform;

    private Map<RefObject, JmiDependencyVertex> vertexMap;

    private DirectedGraph<JmiDependencyVertex, DefaultEdge> hierarchyGraph;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new dependency graph.
     *
     * @param elements the elements among which dependencies are to be
     * determined
     * @param transform rules for finding dependencies and grouping elements
     */
    public JmiDependencyGraph(
        Collection<RefObject> elements,
        JmiDependencyTransform transform)
    {
        // don't allow multiple edges, but do allow loops
        // (to support transform.shouldProduceSelfLoops)
        this(
            elements,
            transform,
            new DefaultDirectedGraph<JmiDependencyVertex, DefaultEdge>(
                DefaultEdge.class));
    }

    private JmiDependencyGraph(
        Collection<RefObject> elements,
        JmiDependencyTransform transform,
        DirectedGraph<JmiDependencyVertex, DefaultEdge> mutableGraph)
    {
        super(mutableGraph);
        this.mutableGraph = mutableGraph;
        hierarchyGraph =
            new DefaultDirectedGraph<JmiDependencyVertex, DefaultEdge>(
                DefaultEdge.class);
        this.transform = transform;
        Comparator<RefBaseObject> tieBreaker = transform.getTieBreaker();
        vertexMap =
            (tieBreaker != null)
            ? new TreeMap<RefObject, JmiDependencyVertex>(tieBreaker)
            : new HashMap<RefObject, JmiDependencyVertex>();
        addElements(elements);
        vertexMap = Collections.unmodifiableMap(vertexMap);
        for (JmiDependencyVertex vertex : vertexMap.values()) {
            vertex.makeImmutable();
        }
        hierarchyGraph =
            new UnmodifiableDirectedGraph<JmiDependencyVertex, DefaultEdge>(
                hierarchyGraph);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return an immutable map from an object in the graph to its containing
     * {@link JmiDependencyVertex}
     */
    public Map<RefObject, JmiDependencyVertex> getVertexMap()
    {
        return vertexMap;
    }

    /**
     * @return immutable graph of hierarchy relationships imposed on vertices
     */
    public DirectedGraph<JmiDependencyVertex, DefaultEdge> getHierarchyGraph()
    {
        return hierarchyGraph;
    }

    private void addElements(Collection<RefObject> elements)
    {
        Comparator<RefBaseObject> tieBreaker = transform.getTieBreaker();
        if (tieBreaker != null) {
            List<RefObject> list = new ArrayList<RefObject>(elements);
            Collections.sort(list, tieBreaker);
            elements = list;
        }

        // Build up disconnected vertices representing elements
        // grouped by contraction.
        for (RefObject target : elements) {
            JmiDependencyVertex targetVertex = vertexMap.get(target);
            if (targetVertex == null) {
                targetVertex = new JmiDependencyVertex();
                addElementToVertex(target, targetVertex);
            }
            Collection<RefObject> sources =
                transform.getSourceNeighbors(
                    target,
                    elements,
                    JmiAssocMapping.CONTRACTION);
            for (RefObject source : sources) {
                if (source.equals(target)) {
                    continue;
                }
                JmiDependencyVertex sourceVertex = vertexMap.get(source);
                if (sourceVertex == targetVertex) {
                    continue;
                }
                if (sourceVertex == null) {
                    addElementToVertex(source, targetVertex);
                } else {
                    for (RefObject refObj : targetVertex.getElementSet()) {
                        addElementToVertex(refObj, sourceVertex);
                    }
                    targetVertex = sourceVertex;
                }
            }
        }

        // Add only those vertices which survived contraction.
        // (DefaultDirectedGraph will filter out duplicates.)
        Graphs.addAllVertices(mutableGraph, vertexMap.values());
        Graphs.addAllVertices(hierarchyGraph, vertexMap.values());

        // Create dependency edges.
        addDependencyEdges(elements, JmiAssocMapping.COPY);

        // Create reverse dependency edges.
        addDependencyEdges(elements, JmiAssocMapping.REVERSAL);

        // Create hierarchy edges.
        addHierarchyEdges(elements);
    }

    private void addDependencyEdges(
        Collection<RefObject> elements,
        JmiAssocMapping mapping)
    {
        for (RefObject target : elements) {
            JmiDependencyVertex targetVertex = vertexMap.get(target);
            Collection<RefObject> sources =
                transform.getSourceNeighbors(
                    target,
                    elements,
                    mapping);
            for (RefObject source : sources) {
                JmiDependencyVertex sourceVertex = vertexMap.get(source);
                if (sourceVertex == targetVertex) {
                    if (!transform.shouldProduceSelfLoops()) {
                        // REVIEW:  self-loops
                        continue;
                    }
                }

                // DefaultDirectedGraph will filter out duplicate edges
                if (mapping == JmiAssocMapping.COPY) {
                    mutableGraph.addEdge(sourceVertex, targetVertex);
                } else {
                    assert (mapping == JmiAssocMapping.REVERSAL);
                    mutableGraph.addEdge(targetVertex, sourceVertex);
                }
            }
        }
    }

    private void addHierarchyEdges(Collection<RefObject> elements)
    {
        for (RefObject target : elements) {
            JmiDependencyVertex targetVertex = vertexMap.get(target);
            Collection<RefObject> sources =
                transform.getSourceNeighbors(
                    target,
                    elements,
                    JmiAssocMapping.HIERARCHY);
            for (RefObject source : sources) {
                JmiDependencyVertex sourceVertex = vertexMap.get(source);
                if (sourceVertex == targetVertex) {
                    // never want loops in hierarchy
                    continue;
                }

                // DefaultDirectedGraph will filter out duplicate edges
                hierarchyGraph.addEdge(sourceVertex, targetVertex);
            }
        }

        // Check that we ended up with a forest.
        for (JmiDependencyVertex v : hierarchyGraph.vertexSet()) {
            if (hierarchyGraph.inDegreeOf(v) > 1) {
                throw new AssertionError(
                    "JmiDependencyGraph hierarchy detected vertex with "
                    + "multiple parents");
            }
        }
    }

    private void addElementToVertex(
        RefObject refObj,
        JmiDependencyVertex vertex)
    {
        vertexMap.put(refObj, vertex);
        vertex.getElementSet().add(refObj);
    }
}

// End JmiDependencyGraph.java
