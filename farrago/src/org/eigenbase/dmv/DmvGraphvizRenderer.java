/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2007-2007 The Eigenbase Project
// Copyright (C) 2007-2007 Disruptive Tech
// Copyright (C) 2007-2007 LucidEra, Inc.
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
package org.eigenbase.dmv;

import org.eigenbase.util.*;
import org.eigenbase.jmi.*;

import org.jgrapht.*;
import org.jgrapht.graph.*;

import java.util.*;
import java.io.*;

import javax.jmi.reflect.*;
import javax.jmi.model.*;

/**
 * DmvGraphvizRenderer renders a {@link DmvResponse} as a <code>.dot</code>
 * file suitable for input to <a href="http://www.graphviz.org">Graphviz</a>.
 *
 * @author John Sichi
 * @version $Id$
 */
public class DmvGraphvizRenderer
{
    private PrintWriter pw;
    private DmvResponse response;
    private JmiDependencyGraph dependencyGraph;
    private DirectedGraph<JmiDependencyVertex, DefaultEdge> hierarchyGraph;
    
    /**
     * Renders a {@link DmvResponse} in .dot format and writes the result
     * to a {@link Writer}.
     */
    public void renderDmv(
        DmvResponse response,
        Writer writer)
    {
        StackWriter stackWriter =
            new StackWriter(writer, StackWriter.INDENT_SPACE4);
        this.response = response;
        dependencyGraph = response.getTransformationResult();
        hierarchyGraph = dependencyGraph.getHierarchyGraph();
        this.pw = new PrintWriter(stackWriter);
        renderGraph();
        pw.close();
    }

    private void renderGraph()
    {
        pw.println(
            "digraph G {");
        pw.write(StackWriter.INDENT);
        pw.println(
            "graph [bgcolor=gray]");
        pw.print(
            "node ");
        pw.println(
            "[shape=record, style=filled, fillcolor=white, fontsize=10.0]");
        pw.println(
            "edge [fontsize=10.0]");
        renderVertices();
        renderEdges();
        pw.write(StackWriter.OUTDENT);
        pw.println("}");
    }

    private void renderVertices()
    {
        for (JmiDependencyVertex vertex : dependencyGraph.vertexSet()) {
            if (!isClusterChild(vertex)) {
                renderVertex(vertex);
            }
        }
    }

    private void renderVertex(JmiDependencyVertex vertex)
    {
        if (isClusterParent(vertex)) {
            pw.print("subgraph cluster");
            pw.print(getVertexId(vertex));
            pw.println(" {");
            pw.write(StackWriter.INDENT);
            pw.println("bgcolor=white;");
            pw.print("label=\"");
            // FIXME escaping
            pw.print(getVertexName(vertex));
            pw.println("\";");
            List<JmiDependencyVertex> children =
                Graphs.successorListOf(hierarchyGraph, vertex);
            for (JmiDependencyVertex child : children) {
                renderVertex(child);
            }
            pw.write(StackWriter.OUTDENT);
            pw.println("}");
        } else {
            pw.print(getVertexId(vertex));
            pw.print("[label=\"");
            // FIXME escaping
            pw.print(getVertexName(vertex));
            pw.println("\"];");
        }
    }

    private void renderEdges()
    {
        // TODO:  deal with edges between clusters by using lhead and ltail
        // and picking a representative child arbitrarily via
        // getHierarchyRep
        
        for (DefaultEdge edge : dependencyGraph.edgeSet()) {
            pw.print(getVertexId(dependencyGraph.getEdgeSource(edge)));
            pw.print("->");
            pw.print(getVertexId(dependencyGraph.getEdgeTarget(edge)));
            pw.println("[];");
        }
    }
    
    private String getVertexId(JmiDependencyVertex vertex)
    {
        return Long.toString(System.identityHashCode(vertex));
    }

    private String getVertexName(JmiDependencyVertex vertex)
    {
        // TODO:  specify rules for this as part of transform
        for (RefObject obj : vertex.getElementSet()) {
            RefFeatured parent = obj.refImmediateComposite();
            if (!vertex.getElementSet().contains(parent)) {
                String className =
                    ((ModelElement) obj.refMetaObject()).getName();
                String objName;
                try {
                    objName = (String) obj.refGetValue("name");
                } catch (InvalidNameException ex) {
                    continue;
                }
                if (isClusterParent(vertex)) {
                    return className + ":" + objName;
                } else {
                    return "{"
                        + className
                        + "|"
                        + objName
                        + "}";
                }
            }
        }
        return "anonymous";
    }

    private boolean isClusterParent(JmiDependencyVertex vertex)
    {
        return hierarchyGraph.outDegreeOf(vertex) > 0;
    }

    private boolean isClusterChild(JmiDependencyVertex vertex)
    {
        return hierarchyGraph.inDegreeOf(vertex) > 0;
    }

    private JmiDependencyVertex getHierarchyRep(JmiDependencyVertex vertex)
    {
        DefaultEdge edge =
            hierarchyGraph.outgoingEdgesOf(vertex).iterator().next();
        return hierarchyGraph.getEdgeTarget(edge);
    }
}

// End DmvGraphvizRenderer.java
