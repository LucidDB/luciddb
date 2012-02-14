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
