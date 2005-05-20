/*
// $Id$
// Farrago is an extensible data management system.
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
package net.sf.farrago.test;

import org.eigenbase.jmi.*;
import org.eigenbase.util.*;

import junit.framework.*;

import javax.jmi.reflect.*;

import java.util.*;
import java.io.*;

import org._3pq.jgrapht.*;

/**
 * JmiModelTest is a unit test for {@link JmiModelGraph} and {@link
 * JmiModelView}.
 *
 *<p>
 *
 * NOTE:  this test lives here rather than under org.eigenbase because
 * it currently depends on MDR for a JMI implementation.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class JmiModelTest extends FarragoTestCase
{
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

        List list = new ArrayList(graph.vertexSet());
        Collections.sort(list, new StringRepresentationComparator());

        Writer writer = openTestLog();
        PrintWriter pw = new PrintWriter(writer);
        Iterator iter = list.iterator();
        while (iter.hasNext()) {
            JmiClassVertex vertex = (JmiClassVertex) iter.next();
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

    private void diffGraph(DirectedGraph graph)
        throws Exception
    {
        List list = new ArrayList();
        list.addAll(graph.vertexSet());
        list.addAll(graph.edgeSet());
        
        Writer writer = openTestLog();
        PrintWriter pw = new PrintWriter(writer);
        dumpList(pw, list);
        pw.close();
        diffTestLog();
    }

    private void dumpViewVertex(
        PrintWriter pw, JmiModelView view, JmiClassVertex vertex)
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

    private void dumpNamedSet(
        PrintWriter pw, String name, Set set)
    {
        pw.print(name);
        pw.println(" {");
        dumpList(pw, new ArrayList(set));
        pw.println("}");
    }

    private void dumpList(
        PrintWriter pw, List list)
    {
        Collections.sort(list, new StringRepresentationComparator());
        Iterator iter = list.iterator();
        while (iter.hasNext()) {
            pw.println(iter.next());
        }
    }
}

// End JmiModelTest.java
