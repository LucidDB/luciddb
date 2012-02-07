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
package org.eigenbase.dmv;

import java.io.*;

import java.util.*;

import javax.jmi.model.*;
import javax.jmi.reflect.*;

import org.eigenbase.jmi.*;
import org.eigenbase.util.*;

import org.jgrapht.*;
import org.jgrapht.graph.*;


/**
 * DmvGraphvizRenderer renders a {@link DmvResponse} as a <code>.dot</code> file
 * suitable for input to <a href="http://www.graphviz.org">Graphviz</a>.
 *
 * @author John Sichi
 * @version $Id$
 */
public class DmvGraphvizRenderer
{
    //~ Instance fields --------------------------------------------------------

    private PrintWriter pw;
    private DmvResponse response;
    private JmiDependencyGraph dependencyGraph;
    private DirectedGraph<JmiDependencyVertex, DefaultEdge> hierarchyGraph;

    //~ Methods ----------------------------------------------------------------

    /**
     * Renders a {@link DmvResponse} in .dot format and writes the result to a
     * {@link Writer}.
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
