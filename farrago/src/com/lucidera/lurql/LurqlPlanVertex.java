/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
package com.lucidera.lurql;

import java.util.*;
import java.io.*;

import org.eigenbase.jmi.*;
import org.eigenbase.util.*;

import javax.jmi.model.*;

import org._3pq.jgrapht.*;
import org._3pq.jgrapht.graph.*;
import org._3pq.jgrapht.traverse.*;
import org._3pq.jgrapht.event.*;

/**
 * LurqlPlanVertex is a vertex in a LURQL plan graph.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LurqlPlanVertex
{
    /**
     * Containing plan.
     */
    private final LurqlPlan plan;
    
    /**
     * Name for this vertex (unique with respect to containing plan).
     */
    private final String name;

    /**
     * The alias assigned to this vertex in the parsed query, 
     * or null if none.
     */
    public final String var;

    /**
     * Set of JmiClassVertex references corresponding to
     * classes which need to be queried at this point in the plan.
     */
    private Set classVertexSet;

    /**
     * Unmodifiable counterpart to classVertexSet (for public consumption).
     */
    private Set unmodifiableClassVertexSet;

    /** All filters that apply at the given node */
    private final List filters;

    /**
     * Unmodifiable counterpart to filters (for public consumption).
     */
    private final List unmodifiableFilters;

    /**
     * Set of object MofIds to use as roots.
     */
    private final Set rootObjectIds;

    /**
     * If non-null, the root of a recursive cycle.  We avoid creating an
     * explicit graph edge to represent the cycle; instead, we use this
     * "weak edge" and keep the graph structure acyclic.
     */
    private LurqlPlanVertex recursionRoot;

    /**
     * If non-null, a subgraph of vertices which should be executed
     * cyclically to implement dynamic recursion.
     */
    private DirectedGraph recursionSubgraph;

    /**
     * True if this vertex participates in a recursion execution cycle.
     */
    private boolean recursive;
        
    /**
     * String representation of this vertex.
     */
    private String stringRep;

    public LurqlPlanVertex(
        LurqlPlan plan, String name, String alias, Set rootObjectIds)
    {
        this.plan = plan;
        this.name = name;
        this.rootObjectIds = rootObjectIds;
        this.var = alias;
        classVertexSet = new HashSet();
        unmodifiableClassVertexSet =
            Collections.unmodifiableSet(classVertexSet);
        filters = new ArrayList();
        unmodifiableFilters =
            Collections.unmodifiableList(filters);
    }

    public String getName()
    {
        return name;
    }

    public String getAlias()
    {
        return var;
    }

    public DirectedGraph getRecursionSubgraph()
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
        final DirectedGraph subgraph = new DirectedMultigraph();
        recursionRoot.recursionSubgraph = subgraph;

        // TODO jvs 16-May-2005:  submit to JGraphT
        DepthFirstIterator iter = new DepthFirstIterator(
            plan.getGraph(),
            recursionRoot);
        iter.addTraversalListener(
            new TraversalListenerAdapter()
            {
                public void edgeTraversed(EdgeTraversalEvent e)
                {
                    GraphHelper.addEdgeWithVertices(
                        subgraph, 
                        e.getEdge());
                }

                public void vertexTraversed(VertexTraversalEvent e)
                {
                    subgraph.addVertex(e.getVertex());
                    ((LurqlPlanVertex) e.getVertex()).recursive = true;
                }
            });
        while (iter.hasNext()) {
            iter.next();
        }

        // should have traversed this vertex above
        assert(recursive);
        
        // REVIEW jvs 16-May-2005:  rethink freeze
        stringRep = computeStringRep();
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
        StringBuffer sb = new StringBuffer();
        sb.append(name);
        sb.append(" { ");
        List list = new ArrayList(classVertexSet);
        list.addAll(rootObjectIds);
        Collections.sort(list, new StringRepresentationComparator());
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
            sb.append (" recursively to ");
            sb.append(recursionRoot.getName());
        }
        return sb.toString();
    }

    public List getFilters()
    {
        return unmodifiableFilters;
    }

    public Set getClassVertexSet()
    {
        return unmodifiableClassVertexSet;
    }

    public Set getRootObjectIds()
    {
        return rootObjectIds;
    }

    void addClassVertex(JmiClassVertex classVertex)
    {
        classVertexSet.add(classVertex);
    }

    void addFilters(List filters)
    {
        this.filters.addAll(filters);
    }

    void freeze()
    {
        // first, expand to include all subclasses
        Set expandedSet = new HashSet();
        Iterator iter = classVertexSet.iterator();
        while (iter.hasNext()) {
            JmiClassVertex vertex = (JmiClassVertex) iter.next();
            expandedSet.addAll(
                plan.getModelView().getAllSubclassVertices(vertex));
        }

        // then, find classes to which all filters apply
        Set filterSet = new HashSet();
        iter = expandedSet.iterator();
outer:
        while (iter.hasNext()) {
            JmiClassVertex vertex = (JmiClassVertex) iter.next();
            Iterator filterIter = filters.iterator();
            while (filterIter.hasNext()) {
                LurqlFilter filter = (LurqlFilter) filterIter.next();
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
        expandedSet = new HashSet();
        iter = filterSet.iterator();
        while (iter.hasNext()) {
            JmiClassVertex vertex = (JmiClassVertex) iter.next();
            boolean present = expandedSet.contains(vertex);
            expandedSet.addAll(
                plan.getModelView().getAllSubclassVertices(vertex));
            if (!present) {
                expandedSet.remove(vertex);
            }
        }
        filterSet.removeAll(expandedSet);
        classVertexSet = new HashSet(filterSet);
        unmodifiableClassVertexSet =
            Collections.unmodifiableSet(classVertexSet);

        // and freeze the string representation
        stringRep = computeStringRep();
    }
}

// End LurqlPlanVertex.java
