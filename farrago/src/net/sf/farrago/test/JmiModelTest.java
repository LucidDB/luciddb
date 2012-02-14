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
package net.sf.farrago.test;

import java.io.*;

import java.util.*;

import javax.jmi.reflect.*;

import junit.framework.*;

import org.eigenbase.jmi.*;
import org.eigenbase.util.*;

import org.jgrapht.*;


/**
 * JmiModelTest is a unit test for {@link JmiModelGraph} and {@link
 * JmiModelView}.
 *
 * <p>NOTE: this test lives here rather than under org.eigenbase because it
 * currently depends on MDR for a JMI implementation.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class JmiModelTest
    extends FarragoTestCase
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new JmiModelTest object.
     *
     * @param testName JUnit test name
     *
     * @throws Exception .
     */
    public JmiModelTest(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ----------------------------------------------------------------

    public static Test suite()
    {
        return wrappedSuite(JmiModelTest.class);
    }

    /**
     * Tests construction of the JmiModelGraph for the MOF model.
     */
    public void testModelGraph()
        throws Exception
    {
        JmiModelGraph graph = getMofModelGraph();
        diffGraph(graph);
    }

    /**
     * Tests construction of the inheritance graph for the MOF model.
     */
    public void testInheritanceGraph()
        throws Exception
    {
        JmiModelGraph graph = getMofModelGraph();
        diffGraph(graph.getInheritanceGraph());
    }

    /**
     * Tests construction of the association graph for the MOF model.
     */
    public void testAssocGraph()
        throws Exception
    {
        JmiModelGraph graph = getMofModelGraph();
        diffGraph(graph.getAssocGraph());
    }

    /**
     * Tests construction of a JmiModelView for the MOF model.
     */
    public void testModelView()
        throws Exception
    {
        JmiModelGraph graph = getMofModelGraph();
        JmiModelView view = new JmiModelView(graph);

        List<JmiClassVertex> list =
            new ArrayList<JmiClassVertex>(graph.vertexSet());
        Collections.sort(
            list,
            new StringRepresentationComparator<JmiClassVertex>());

        Writer writer = openTestLog();
        PrintWriter pw = new PrintWriter(writer);
        for (JmiClassVertex vertex : list) {
            dumpViewVertex(pw, view, vertex);
        }
        pw.close();
        diffTestLog();
    }

    private JmiModelGraph getMofModelGraph()
        throws Exception
    {
        RefPackage mofPackage = repos.getMdrRepos().getExtent("MOF");
        return new JmiModelGraph(mofPackage);
    }

    private <V, E> void diffGraph(DirectedGraph<V, E> graph)
        throws Exception
    {
        List<Object> list = new ArrayList<Object>();
        list.addAll(graph.vertexSet());
        list.addAll(graph.edgeSet());

        Writer writer = openTestLog();
        PrintWriter pw = new PrintWriter(writer);
        dumpList(pw, list);
        pw.close();
        diffTestLog();
    }

    private void dumpViewVertex(
        PrintWriter pw,
        JmiModelView view,
        JmiClassVertex vertex)
    {
        pw.println("Vertex:  " + vertex);
        dumpNamedSet(
            pw,
            "allSuperclassVertices",
            view.getAllSuperclassVertices(vertex));
        dumpNamedSet(
            pw,
            "allSubclassVertices",
            view.getAllSubclassVertices(vertex));
        dumpNamedSet(
            pw,
            "inheritedOutgoingAssocEdges",
            view.getInheritedOutgoingAssocEdges(vertex));
        dumpNamedSet(
            pw,
            "inheritedIncomingAssocEdges",
            view.getInheritedIncomingAssocEdges(vertex));
        dumpNamedSet(
            pw,
            "allOutgoingAssocEdges",
            view.getAllOutgoingAssocEdges(vertex));
        dumpNamedSet(
            pw,
            "allIncomingAssocEdges",
            view.getAllIncomingAssocEdges(vertex));
        pw.println();
    }

    private <T> void dumpNamedSet(
        PrintWriter pw,
        String name,
        Set<T> set)
    {
        pw.print(name);
        pw.println(" {");
        dumpList(
            pw,
            new ArrayList<T>(set));
        pw.println("}");
    }

    private <T> void dumpList(
        PrintWriter pw,
        List<T> list)
    {
        Collections.sort(
            list,
            new StringRepresentationComparator<T>());
        for (T o : list) {
            pw.println(o);
        }
    }
}

// End JmiModelTest.java
