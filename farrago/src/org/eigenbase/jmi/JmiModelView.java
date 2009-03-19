/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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
