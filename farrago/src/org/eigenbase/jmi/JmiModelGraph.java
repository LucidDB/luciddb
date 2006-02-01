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

import org.eigenbase.util.*;

import org._3pq.jgrapht.*;
import org._3pq.jgrapht.graph.*;

import javax.jmi.model.*;
import javax.jmi.reflect.*;

import java.util.*;

/**
 * JmiModelGraph is a directed graph representation of a JMI model.  Vertices
 * are instances of {@link JmiClassVertex}.  Edges are instances of either
 * {@link JmiInheritanceEdge} or {@link JmiAssocEdge}.  Graph instances are
 * immutable and can be accessed concurrently by multiple threads.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class JmiModelGraph
    extends UnmodifiableDirectedGraph
{
    /**
     * The underlying graph structure; we hide it here so that
     * it can only be modified internally.
     */
    private final DirectedGraph combinedGraph;

    /**
     * Subgraph with just inheritance edges.
     */
    private final DirectedGraph inheritanceGraph;

    /**
     * Unmodifiable view of inheritanceGraph.
     */
    private final DirectedGraph unmodifiableInheritanceGraph;

    /**
     * Subgraph with just inheritance edges.
     */
    private final DirectedGraph assocGraph;

    /**
     * Unmodifiable view of assocGraph.
     */
    private final DirectedGraph unmodifiableAssocGraph;

    /**
     * Map from Ref and Mof instances to corresponding graph vertices
     * and edges.
     */
    private final Map map;

    private final RefPackage refRootPackage;
    
    /**
     * Creates a new graph based on the contents of a RefPackage
     * and all of its subpackages.
     *
     * @param refRootPackage package on which graph is based
     */
    public JmiModelGraph(RefPackage refRootPackage)
    {
        this(
            refRootPackage,
            new DirectedMultigraph());
    }

    /**
     * @return the subgraph of only inheritance edges
     */
    public DirectedGraph getInheritanceGraph()
    {
        return unmodifiableInheritanceGraph;
    }

    /**
     * @return the subgraph of only association edges
     */
    public DirectedGraph getAssocGraph()
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
    public RefClass getRefClassForVertex(Object vertex)
    {
        return ((JmiClassVertex) vertex).getRefClass();
    }
    
    /**
     * Gets the MOF representation for a class.
     *
     * @param vertex vertex representing class of interest
     *
     * @return MOF representation for class
     */
    public MofClass getMofClassForVertex(Object vertex)
    {
        return ((JmiClassVertex) vertex).getMofClass();
    }

    /**
     * Gets the MOF representation for an association.
     *
     * @param edge edge representing association of interest
     *
     * @return MOF representation for association
     */
    public Association getMofAssocForEdge(Object edge)
    {
        return ((JmiAssocEdge) edge).getMofAssoc();
    }
    
    /**
     * Gets the JMI reflective representation for an association.
     *
     * @param edge edge representing association of interest
     *
     * @return JMI reflective representation for association
     */
    public RefAssociation getRefAssocForEdge(Object edge)
    {
        return ((JmiAssocEdge) edge).getRefAssoc();
    }

    /**
     * @return the JMI reflective representation for the root package
     * represented by this graph
     */
    public RefPackage getRefRootPackage()
    {
        return refRootPackage;
    }
    
    private JmiModelGraph(
        RefPackage refRootPackage, DirectedGraph combinedGraph)
    {
        super(combinedGraph);

        this.refRootPackage = refRootPackage;
        this.combinedGraph = combinedGraph;
        
        inheritanceGraph = new DirectedMultigraph();
        unmodifiableInheritanceGraph =
            new UnmodifiableDirectedGraph(inheritanceGraph);

        assocGraph = new DirectedMultigraph();
        unmodifiableAssocGraph =
            new UnmodifiableDirectedGraph(assocGraph);
        
        map = new HashMap();
        addMofPackage((MofPackage) refRootPackage.refMetaObject());
        addRefPackage(refRootPackage);
    }

    private void addMofPackage(MofPackage mofPackage)
    {
        Iterator iter = mofPackage.getContents().iterator();
        while (iter.hasNext()) {
            ModelElement modelElement = (ModelElement) iter.next();
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
        map.put(mofClass.getName(), vertex);
        Iterator iter = mofClass.getSupertypes().iterator();
        while (iter.hasNext()) {
            MofClass superClass = (MofClass) iter.next();
            JmiClassVertex superVertex = addMofClass(superClass);
            JmiInheritanceEdge edge = 
                new JmiInheritanceEdge(superVertex, vertex);
            combinedGraph.addEdge(edge);
            inheritanceGraph.addEdge(edge);
        }
        return vertex;
    }

    private void addMofAssoc(Association mofAssoc)
    {
        if (getEdgeForMofAssoc(mofAssoc) != null) {
            return;
        }
        
        AssociationEnd [] mofAssocEnds = new AssociationEnd[2];
        
        ModelPackage mofPackage = (ModelPackage)
            mofAssoc.refImmediatePackage();
        MofClass endType = (MofClass)
            mofPackage.getAssociationEnd().refMetaObject();
        List ends = mofAssoc.findElementsByType(endType, false);
        mofAssocEnds[0] = (AssociationEnd) ends.get(0);
        mofAssocEnds[1] = (AssociationEnd) ends.get(1);

        boolean swapEnds = false;

        if (mofAssocEnds[1].getAggregation() == AggregationKindEnum.COMPOSITE) {
            swapEnds = true;
        }

        if ((mofAssocEnds[0].getMultiplicity().getUpper() > 1)
            && (mofAssocEnds[1].getMultiplicity().getUpper() < 2))
        {
            swapEnds = true;
        }

        if (swapEnds) {
            AssociationEnd tmp = mofAssocEnds[0];
            mofAssocEnds[0] = mofAssocEnds[1];
            mofAssocEnds[1] = tmp;
        }
        
        MofClass sourceClass = (MofClass)
            mofAssocEnds[0].getType();
        MofClass targetClass = (MofClass)
            mofAssocEnds[1].getType();
        JmiClassVertex sourceVertex = addMofClass(sourceClass);
        JmiClassVertex targetVertex = addMofClass(targetClass);
        JmiAssocEdge edge = 
            new JmiAssocEdge(
                mofAssoc, sourceVertex, targetVertex, mofAssocEnds);
        combinedGraph.addEdge(edge);
        assocGraph.addEdge(edge);
        map.put(mofAssoc, edge);
        map.put(mofAssoc.getName(), edge);
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
        JmiClassVertex vertex = getVertexForMofClass(
            (MofClass) refClass.refMetaObject());
        assert(vertex != null);
        vertex.refClass = refClass;
        map.put(refClass, vertex);
    }

    private void addRefAssoc(RefAssociation refAssoc)
    {
        JmiAssocEdge edge = getEdgeForMofAssoc(
            (Association) refAssoc.refMetaObject());
        assert(edge != null);
        edge.refAssoc = refAssoc;
        map.put(refAssoc, edge);
    }
}

// End JmiModelGraph.java
