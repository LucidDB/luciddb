/*
// $Id$
// Package org.eigenbase is a class library of data management components.
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
package org.eigenbase.jmi;

import java.util.*;

import javax.jmi.reflect.*;

import org._3pq.jgrapht.*;
import org._3pq.jgrapht.graph.*;


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
    extends UnmodifiableDirectedGraph<JmiDependencyVertex,
        Edge<JmiDependencyVertex>>
{

    //~ Instance fields --------------------------------------------------------

    /**
     * The underlying graph structure; we hide it here so that it can only be
     * modified internally.
     */
    private final DirectedGraph<JmiDependencyVertex, Edge<JmiDependencyVertex>> mutableGraph;

    private final JmiDependencyTransform transform;

    private Map<RefObject, JmiDependencyVertex> vertexMap;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new dependency graph.
     *
     * @param elements the elements among which dependencies are to be
     * determined
     * @param transform rules for finding dependencies and grouping elements
     */
    public JmiDependencyGraph(Collection<RefObject> elements,
        JmiDependencyTransform transform)
    {
        // don't allow multiple edges, but do allow loops
        // (to support transform.shouldProduceSelfLoops)
        this(
            elements,
            transform,
            new DefaultDirectedGraph());
    }

    private JmiDependencyGraph(Collection<RefObject> elements,
        JmiDependencyTransform transform,
        DirectedGraph<JmiDependencyVertex, Edge<JmiDependencyVertex>> mutableGraph)
    {
        super(mutableGraph);
        this.mutableGraph = mutableGraph;
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
                if (source == target) {
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
        mutableGraph.addAllVertices(vertexMap.values());

        // Create dependency edges.
        addDependencyEdges(elements, JmiAssocMapping.COPY);

        // Create reverse dependency edges.
        addDependencyEdges(elements, JmiAssocMapping.REVERSAL);
    }

    private void addDependencyEdges(Collection<RefObject> elements,
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

    private void addElementToVertex(
        RefObject refObj,
        JmiDependencyVertex vertex)
    {
        vertexMap.put(refObj, vertex);
        vertex.getElementSet().add(refObj);
    }
}

// End JmiDependencyGraph.java
