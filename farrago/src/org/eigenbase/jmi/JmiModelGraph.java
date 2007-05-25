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

import javax.jmi.model.*;
import javax.jmi.reflect.*;

import org.jgrapht.*;
import org.jgrapht.graph.*;


/**
 * JmiModelGraph is a directed graph representation of a JMI model. Vertices are
 * instances of {@link JmiClassVertex}. Edges are instances of either {@link
 * JmiInheritanceEdge} or {@link JmiAssocEdge}. Graph instances are immutable
 * and can be accessed concurrently by multiple threads.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class JmiModelGraph
    extends UnmodifiableDirectedGraph<JmiClassVertex, DefaultEdge>
{

    //~ Instance fields --------------------------------------------------------

    /**
     * Class loader to instantiate MDR classes. It's important to set this
     * correctly if there are extension models (not on the regular classpath).
     */
    private final ClassLoader classLoader;

    /**
     * The underlying graph structure; we hide it here so that it can only be
     * modified internally.
     */
    private final DirectedGraph<JmiClassVertex, DefaultEdge> combinedGraph;

    /**
     * Subgraph with just inheritance edges.
     */
    private final DirectedGraph<JmiClassVertex, JmiInheritanceEdge> inheritanceGraph;

    /**
     * Unmodifiable view of inheritanceGraph.
     */
    private final DirectedGraph<JmiClassVertex, JmiInheritanceEdge> unmodifiableInheritanceGraph;

    /**
     * Subgraph with just inheritance edges.
     */
    private final DirectedGraph<JmiClassVertex, DefaultEdge> assocGraph;

    /**
     * Unmodifiable view of assocGraph.
     */
    private final DirectedGraph<JmiClassVertex, DefaultEdge> unmodifiableAssocGraph;

    /**
     * Map from Ref and Mof instances to corresponding graph vertices and edges.
     */
    private final Map<Object, Object> map;

    private final RefPackage refRootPackage;

    private final boolean strict;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new graph based on the contents of a RefPackage and all of its
     * subpackages.
     *
     * @param refRootPackage package on which graph is based
     */
    public JmiModelGraph(RefPackage refRootPackage)
    {
        this(
            refRootPackage,
            ClassLoader.getSystemClassLoader(),
            true);
    }

    /**
     * Creates a new graph based on the contents of a RefPackage and all of its
     * subpackages, with control over strictness.
     *
     * @param refRootPackage package on which graph is based
     * @param classLoader class loader, or null to use the system default
     * @param strict true to prevent dangling references in model; false to
     * ignore them
     */
    public JmiModelGraph(
        RefPackage refRootPackage,
        ClassLoader classLoader,
        boolean strict)
    {
        this(
            refRootPackage,
            classLoader,
            new DirectedMultigraph<JmiClassVertex, DefaultEdge>(
                DefaultEdge.class),
            strict);
    }

    private JmiModelGraph(
        RefPackage refRootPackage,
        ClassLoader classLoader,
        DirectedGraph<JmiClassVertex, DefaultEdge> combinedGraph,
        boolean strict)
    {
        super(combinedGraph);

        assert classLoader != null : "pre: classLoader != null";
        this.refRootPackage = refRootPackage;
        this.classLoader = classLoader;
        this.combinedGraph = combinedGraph;
        this.strict = strict;

        inheritanceGraph =
            new DirectedMultigraph<JmiClassVertex, JmiInheritanceEdge>(
                JmiInheritanceEdge.class);
        unmodifiableInheritanceGraph =
            new UnmodifiableDirectedGraph<JmiClassVertex, JmiInheritanceEdge>(
                inheritanceGraph);

        assocGraph =
            new DirectedMultigraph<JmiClassVertex, DefaultEdge>(
                DefaultEdge.class);
        unmodifiableAssocGraph =
            new UnmodifiableDirectedGraph<JmiClassVertex, DefaultEdge>(
                assocGraph);

        map = new HashMap<Object, Object>();
        addMofPackage((MofPackage) refRootPackage.refMetaObject());
        addRefPackage(refRootPackage);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return the subgraph of only inheritance edges
     */
    public DirectedGraph<JmiClassVertex, JmiInheritanceEdge>
        getInheritanceGraph()
    {
        return unmodifiableInheritanceGraph;
    }

    /**
     * @return the subgraph of only association edges
     */
    public DirectedGraph<JmiClassVertex, DefaultEdge> getAssocGraph()
    {
        return unmodifiableAssocGraph;
    }

    /**
     * Gets the vertex representing a class from JMI reflection.
     *
     * @param refClass the class of interest
     *
     * @return corresponding vertex
     */
    public JmiClassVertex getVertexForRefClass(RefClass refClass)
    {
        return (JmiClassVertex) map.get(refClass);
    }

    /**
     * Gets the vertex representing a MOF class.
     *
     * @param mofClass the class of interest
     *
     * @return corresponding vertex
     */
    public JmiClassVertex getVertexForMofClass(MofClass mofClass)
    {
        return (JmiClassVertex) map.get(mofClass);
    }

    /**
     * Gets the vertex representing a Java interface.
     *
     * @param javaInterface the Java interface of interest
     *
     * @return corresponding vertex
     */
    public JmiClassVertex getVertexForJavaInterface(Class javaInterface)
    {
        return (JmiClassVertex) map.get(javaInterface);
    }

    /**
     * Gets the edge representing a MOF association.
     *
     * @param mofAssoc the association of interest
     *
     * @return corresponding edge
     */
    public JmiAssocEdge getEdgeForMofAssoc(Association mofAssoc)
    {
        return (JmiAssocEdge) map.get(mofAssoc);
    }

    /**
     * Gets the edge representing a JMI reflective association.
     *
     * @param refAssoc the association of interest
     *
     * @return corresponding edge
     */
    public JmiAssocEdge getEdgeForRefAssoc(RefAssociation refAssoc)
    {
        return (JmiAssocEdge) map.get(refAssoc);
    }

    /**
     * Gets the vertex representing a MOF class by name.
     *
     * @param name name of the class of interest
     *
     * @return corresponding vertex
     */
    public JmiClassVertex getVertexForClassName(String name)
    {
        return (JmiClassVertex) map.get(name);
    }

    /**
     * Gets the edge representing a MOF association by name.
     *
     * @param name name of the association of interest
     *
     * @return corresponding edge
     */
    public JmiAssocEdge getEdgeForAssocName(String name)
    {
        return (JmiAssocEdge) map.get(name);
    }

    /**
     * Gets the JMI reflective representation for a class.
     *
     * @param vertex vertex representing class of interest
     *
     * @return reflective representation for class
     */
    public RefClass getRefClassForVertex(JmiClassVertex vertex)
    {
        return vertex.getRefClass();
    }

    /**
     * Gets the MOF representation for a class.
     *
     * @param vertex vertex representing class of interest
     *
     * @return MOF representation for class
     */
    public MofClass getMofClassForVertex(JmiClassVertex vertex)
    {
        return vertex.getMofClass();
    }

    /**
     * Gets the MOF representation for an association.
     *
     * @param edge edge representing association of interest
     *
     * @return MOF representation for association
     */
    public Association getMofAssocForEdge(JmiAssocEdge edge)
    {
        return edge.getMofAssoc();
    }

    /**
     * Gets the JMI reflective representation for an association.
     *
     * @param edge edge representing association of interest
     *
     * @return JMI reflective representation for association
     */
    public RefAssociation getRefAssocForEdge(JmiAssocEdge edge)
    {
        return edge.getRefAssoc();
    }

    /**
     * @return the JMI reflective representation for the root package
     * represented by this graph
     */
    public RefPackage getRefRootPackage()
    {
        return refRootPackage;
    }

    private void addMofPackage(MofPackage mofPackage)
    {
        for (Object o : mofPackage.getContents()) {
            ModelElement modelElement = (ModelElement) o;
            if (modelElement instanceof MofPackage) {
                addMofPackage((MofPackage) modelElement);
            } else if (modelElement instanceof MofClass) {
                addMofClass((MofClass) modelElement);
            } else if (modelElement instanceof Association) {
                addMofAssoc((Association) modelElement);
            }
        }
    }

    private JmiClassVertex addMofClass(MofClass mofClass)
    {
        JmiClassVertex vertex = getVertexForMofClass(mofClass);
        if (vertex != null) {
            return vertex;
        }
        vertex = new JmiClassVertex(mofClass);
        combinedGraph.addVertex(vertex);
        inheritanceGraph.addVertex(vertex);
        assocGraph.addVertex(vertex);
        map.put(mofClass, vertex);
        map.put(
            mofClass.getName(),
            vertex);
        map.put(vertex.javaInterface, vertex);
        for (Object o : mofClass.getSupertypes()) {
            MofClass superClass = (MofClass) o;
            JmiClassVertex superVertex = addMofClass(superClass);
            JmiInheritanceEdge edge =
                new JmiInheritanceEdge(superVertex, vertex);
            combinedGraph.addEdge(superVertex, vertex, edge);
            inheritanceGraph.addEdge(superVertex, vertex, edge);
        }
        return vertex;
    }

    private void addMofAssoc(Association mofAssoc)
    {
        if (getEdgeForMofAssoc(mofAssoc) != null) {
            return;
        }

        AssociationEnd [] mofAssocEnds = new AssociationEnd[2];

        ModelPackage mofPackage = (ModelPackage) mofAssoc.refImmediatePackage();
        MofClass endType =
            (MofClass) mofPackage.getAssociationEnd().refMetaObject();
        List ends = mofAssoc.findElementsByType(endType, false);
        mofAssocEnds[0] = (AssociationEnd) ends.get(0);
        mofAssocEnds[1] = (AssociationEnd) ends.get(1);

        boolean swapEnds = false;

        if (mofAssocEnds[1].getAggregation() == AggregationKindEnum.COMPOSITE) {
            swapEnds = true;
        }

        if ((mofAssocEnds[0].getMultiplicity().getUpper() != 1)
            && (mofAssocEnds[1].getMultiplicity().getUpper() == 1)) {
            swapEnds = true;
        }

        if (mofAssocEnds[0].getMultiplicity().isOrdered()) {
            swapEnds = true;
        }

        if (swapEnds) {
            AssociationEnd tmp = mofAssocEnds[0];
            mofAssocEnds[0] = mofAssocEnds[1];
            mofAssocEnds[1] = tmp;
        }

        MofClass sourceClass = (MofClass) mofAssocEnds[0].getType();
        MofClass targetClass = (MofClass) mofAssocEnds[1].getType();
        JmiClassVertex sourceVertex = addMofClass(sourceClass);
        JmiClassVertex targetVertex = addMofClass(targetClass);
        JmiAssocEdge edge =
            new JmiAssocEdge(
                mofAssoc,
                mofAssocEnds);
        combinedGraph.addEdge(sourceVertex, targetVertex, edge);
        assocGraph.addEdge(sourceVertex, targetVertex, edge);
        map.put(mofAssoc, edge);
        map.put(
            mofAssoc.getName(),
            edge);
    }

    private void addRefPackage(RefPackage refPackage)
    {
        Iterator iter;

        iter = refPackage.refAllPackages().iterator();
        while (iter.hasNext()) {
            addRefPackage((RefPackage) iter.next());
        }

        iter = refPackage.refAllClasses().iterator();
        while (iter.hasNext()) {
            addRefClass((RefClass) iter.next());
        }

        iter = refPackage.refAllAssociations().iterator();
        while (iter.hasNext()) {
            addRefAssoc((RefAssociation) iter.next());
        }
    }

    private void addRefClass(RefClass refClass)
    {
        JmiClassVertex vertex =
            getVertexForMofClass((MofClass) refClass.refMetaObject());
        if (vertex == null) {
            if (!strict) {
                return;
            }
        }
        Class<? extends RefObject> javaInterface =
            JmiObjUtil.getClassForRefClass(classLoader, refClass, true);
        assert (vertex != null);
        vertex.refClass = refClass;
        map.put(refClass, vertex);

        if (javaInterface != null) {
            vertex.javaInterface = javaInterface;
            map.put(javaInterface, vertex);
        }
    }

    private void addRefAssoc(RefAssociation refAssoc)
    {
        JmiAssocEdge edge =
            getEdgeForMofAssoc((Association) refAssoc.refMetaObject());
        if (edge == null) {
            if (!strict) {
                return;
            }
        }
        assert (edge != null);
        edge.refAssoc = refAssoc;
        map.put(refAssoc, edge);
    }
}

// End JmiModelGraph.java
