/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
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

import java.util.*;

import junit.framework.*;

import org.jgrapht.*;
import org.jgrapht.graph.*;


/**
 * Jdk15Test tests language features introduced in JDK 1.5. It is excluded from
 * compilation when src="1.4" is passed to javac.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class Jdk15Test
    extends TestCase
{
    //~ Constructors -----------------------------------------------------------

    public Jdk15Test(String testName)
    {
        super(testName);
    }

    //~ Methods ----------------------------------------------------------------

    public void testGenericCollections()
    {
        List<String> words = new ArrayList<String>();
        words.add("left");
        words.add("right");
        words.add("drat");

        // remove four-letter words
        Iterator<String> iter = words.iterator();
        while (iter.hasNext()) {
            String s = iter.next();
            if (s.length() == 4) {
                iter.remove();
            }
        }

        assertEquals(
            1,
            words.size());
        assertEquals(
            "right",
            words.get(0));
    }

    public void testForeach()
    {
        List<String> words = new ArrayList<String>();
        words.add("oingo");
        words.add("boingo");
        words.add("ringo");

        for (String s : words) {
            assertTrue(s.endsWith("ingo"));
        }
    }

    public void testGraphGenerics()
    {
        Graph<String, DefaultEdge> graph =
            new SimpleGraph<String, DefaultEdge>(DefaultEdge.class);
        graph.addVertex("Absalom");
        graph.addVertex("Achitophel");
        graph.addEdge("Absalom", "Achitophel");
        String s = graph.vertexSet().iterator().next();
        assertTrue(s.startsWith("A"));
    }
}

// End Jdk15Test.java
