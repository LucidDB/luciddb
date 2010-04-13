/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2009 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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
package org.eigenbase.lurql;

import java.util.*;

import javax.jmi.model.*;

import org.eigenbase.jmi.*;
import org.eigenbase.util.*;

import org.jgrapht.*;
import org.jgrapht.event.*;
import org.jgrapht.graph.*;
import org.jgrapht.traverse.*;


/**
 * LurqlPlanVertex is a vertex in a LURQL plan graph.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LurqlPlanVertex
{
    //~ Instance fields --------------------------------------------------------

    /**
     * Containing plan.
     */
    private final LurqlPlan plan;

    /**
     * Name for this vertex (unique with respect to containing plan).
     */
    private final String name;

    /**
     * The alias assigned to this vertex in the parsed query, or null if none.
     */
    private final String var;

    /**
     * Set of JmiClassVertex references corresponding to classes which need to
     * be queried at this point in the plan.
     */
    private Set<JmiClassVertex> classVertexSet;

    /**
     * Unmodifiable counterpart to classVertexSet (for public consumption).
     */
    private Set<JmiClassVertex> unmodifiableClassVertexSet;

    /**
     * All filters that apply at the given node
     */
    private final List<LurqlFilter> filters;

    /**
     * Unmodifiable counterpart to filters (for public consumption).
     */
    private final List<LurqlFilter> unmodifiableFilters;

    /**
     * Set of object MofIds to use as roots.
     */
    private final Set<String> rootObjectIds;

    /**
     * If non-null, the root of a recursive cycle. We avoid creating an explicit
     * graph edge to represent the cycle; instead, we use this "weak edge" and
     * keep the graph structure acyclic.
     */
    private LurqlPlanVertex recursionRoot;

    /**
     * If non-null, a subgraph of vertices which should be executed cyclically
     * to implement dynamic recursion.
     */
    private DirectedGraph<LurqlPlanVertex, LurqlPlanEdge> recursionSubgraph;

    /**
     * True if this vertex participates in a recursion execution cycle.
     */
    private boolean recursive;

    /**
     * String representation of this vertex.
     */
    private String stringRep;

    //~ Constructors -----------------------------------------------------------

    public LurqlPlanVertex(
        LurqlPlan plan,
        String name,
        String alias,
        Set<String> rootObjectIds)
    {
        this.plan = plan;
        this.name = name;
        this.rootObjectIds = rootObjectIds;
        this.var = alias;
        classVertexSet = new HashSet<JmiClassVertex>();
        unmodifiableClassVertexSet =
            Collections.unmodifiableSet(classVertexSet);
        filters = new ArrayList<LurqlFilter>();
        unmodifiableFilters = Collections.unmodifiableList(filters);
    }

    //~ Methods ----------------------------------------------------------------

    public String getName()
    {
        return name;
    }

    public String getAlias()
    {
        return var;
    }

    public DirectedGraph<LurqlPlanVertex, LurqlPlanEdge> getRecursionSubgraph()
    {
        return recursionSubgraph;
    }

    public boolean isRecursive()
    {
        return recursive;
    }

    public LurqlPlanVertex getRecursionRoot()
    {
        return recursionRoot;
    }

    void setRecursionRoot(LurqlPlanVertex recursionRoot)
    {
        this.recursionRoot = recursionRoot;

        // identify subgraph involved in recursion
        recursionRoot.recursionSubgraph =
            recursionRoot.createReachableSubgraph(true);

        // should have traversed this vertex above
        assert (recursive);

        // REVIEW jvs 16-May-2005:  rethink freeze
        stringRep = computeStringRep();
    }

    DirectedGraph<LurqlPlanVertex, LurqlPlanEdge> createReachableSubgraph(
        final boolean setRecursive)
    {
        final DirectedGraph<LurqlPlanVertex, LurqlPlanEdge> subgraph =
            new DirectedMultigraph<LurqlPlanVertex, LurqlPlanEdge>(
                LurqlPlanEdge.class);

        // TODO jvs 16-May-2005:  submit to JGraphT
        DepthFirstIterator<LurqlPlanVertex, LurqlPlanEdge> iter =
            new DepthFirstIterator<LurqlPlanVertex, LurqlPlanEdge>(
                plan.getGraph(),
                this);
        iter.addTraversalListener(
            new TraversalListenerAdapter<LurqlPlanVertex, LurqlPlanEdge>() {
                public void edgeTraversed(
                    EdgeTraversalEvent<LurqlPlanVertex, LurqlPlanEdge> e)
                {
                    Graphs.addEdgeWithVertices(
                        subgraph,
                        plan.getGraph(),
                        e.getEdge());
                }

                public void vertexTraversed(
                    VertexTraversalEvent<LurqlPlanVertex> e)
                {
                    subgraph.addVertex(e.getVertex());
                    if (setRecursive) {
                        ((LurqlPlanVertex) e.getVertex()).recursive = true;
                    }
                }
            });

        while (iter.hasNext()) {
            iter.next();
        }

        return subgraph;
    }

    public String toString()
    {
        if (stringRep == null) {
            return computeStringRep();
        } else {
            return stringRep;
        }
    }

    private String computeStringRep()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(name);
        sb.append(" { ");
        List<Object> list = new ArrayList<Object>(classVertexSet);
        list.addAll(rootObjectIds);
        Collections.sort(
            list,
            new StringRepresentationComparator<Object>());
        Iterator iter = list.iterator();
        while (iter.hasNext()) {
            JmiClassVertex classVertex = (JmiClassVertex) iter.next();
            sb.append(classVertex.toString());
            if (iter.hasNext()) {
                sb.append(",");
            }
            sb.append(" ");
        }
        sb.append("}");
        if (!filters.isEmpty()) {
            sb.append(" where ");
            iter = filters.iterator();
            while (iter.hasNext()) {
                sb.append(iter.next().toString());
                if (iter.hasNext()) {
                    sb.append(" and ");
                }
            }
        }
        if (recursionRoot != null) {
            sb.append(" recursively to ");
            sb.append(recursionRoot.getName());
        }
        return sb.toString();
    }

    public List<LurqlFilter> getFilters()
    {
        return unmodifiableFilters;
    }

    public Set<JmiClassVertex> getClassVertexSet()
    {
        return unmodifiableClassVertexSet;
    }

    public Set<String> getRootObjectIds()
    {
        return rootObjectIds;
    }

    void addClassVertex(JmiClassVertex classVertex)
    {
        classVertexSet.add(classVertex);
    }

    void addFilters(List<LurqlFilter> filters)
    {
        this.filters.addAll(filters);
    }

    void freeze()
    {
        // first, expand to include all subclasses
        Set<JmiClassVertex> expandedSet = new HashSet<JmiClassVertex>();
        for (JmiClassVertex vertex : classVertexSet) {
            expandedSet.addAll(
                plan.getModelView().getAllSubclassVertices(vertex));
        }

        // then, find classes to which all filters apply
        Set<JmiClassVertex> filterSet = new HashSet<JmiClassVertex>();
outer:
        for (JmiClassVertex vertex : expandedSet) {
            for (LurqlFilter filter : filters) {
                if (filter.isMofId()) {
                    // id is always applicable
                    continue;
                }
                try {
                    vertex.getMofClass().lookupElementExtended(
                        filter.getAttributeName());
                } catch (NameNotFoundException ex) {
                    continue outer;
                }
            }
            filterSet.add(vertex);
        }

        // finally, apply superclass subsumption
        expandedSet = new HashSet<JmiClassVertex>();
        for (JmiClassVertex vertex : filterSet) {
            boolean present = expandedSet.contains(vertex);
            expandedSet.addAll(
                plan.getModelView().getAllSubclassVertices(vertex));
            if (!present) {
                expandedSet.remove(vertex);
            }
        }
        filterSet.removeAll(expandedSet);
        classVertexSet = new HashSet<JmiClassVertex>(filterSet);
        unmodifiableClassVertexSet =
            Collections.unmodifiableSet(classVertexSet);

        // and freeze the string representation
        stringRep = computeStringRep();
    }
}

// End LurqlPlanVertex.java
