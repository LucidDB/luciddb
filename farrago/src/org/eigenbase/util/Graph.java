/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package org.eigenbase.util;

import junit.framework.TestCase;

import java.util.*;


/**
 * A <code>Graph</code> is a collection of directed arcs between nodes, and
 * supports various graph-theoretic operations.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since May 6, 2003
 */
public class Graph
{
    //~ Static fields/initializers --------------------------------------------

    public static final Arc [] noArcs = new Arc[0];

    //~ Instance fields -------------------------------------------------------

    /** Maps {@link Arc} to {@link Arc}[]. */
    private HashMap shortestPath = new HashMap();
    private HashSet arcs = new HashSet();
    private boolean mutable = true;

    //~ Methods ---------------------------------------------------------------

    /**
     * Returns an iterator of all paths between two nodes, shortest first.
     * 
     * <p>
     * The current implementation is not optimal.
     * </p>
     */
    public Iterator getPaths(Object from,Object to)
    {
        ArrayList list = new ArrayList();
        findPaths(from,to,list);
        return list.iterator();
    }

    /**
     * Returns the shortest path between two points, null if there is no path.
     *
     * @param from
     * @param to
     *
     * @return A list of arcs, null if there is no path.
     */
    public Arc [] getShortestPath(Object from,Object to)
    {
        if (from.equals(to)) {
            return noArcs;
        }
        makeImmutable();
        return (Arc []) shortestPath.get(new Arc(from,to));
    }

    public Arc createArc(Object from,Object to)
    {
        final Arc arc = new Arc(from,to);
        arcs.add(arc);
        mutable = true;
        return arc;
    }

    private void findPaths(Object from,Object to,List list)
    {
        final Arc [] shortestPath = getShortestPath(from,to);
        if (shortestPath == null) {
            return;
        }
        Arc arc = new Arc(from,to);
        if (arcs.contains(arc)) {
            list.add(new Arc [] { arc });
        }
        findPathsExcluding(from,to,list,new HashSet(),new ArrayList());
    }

    /**
     * Finds all paths from "from" to "to" of length 2 or greater, such that
     * the intermediate nodes are not contained in "excludedNodes".
     */
    private void findPathsExcluding(
        Object from,
        Object to,
        List list,
        HashSet excludedNodes,
        List prefix)
    {
        excludedNodes.add(from);
        for (Iterator arcsIter = arcs.iterator(); arcsIter.hasNext();) {
            Arc arc = (Arc) arcsIter.next();
            if (arc.from.equals(from)) {
                if (arc.to.equals(to)) {
                    // We found a path.
                    prefix.add(arc);
                    final Arc [] arcs = (Arc []) prefix.toArray(noArcs);
                    list.add(arcs);
                    prefix.remove(prefix.size() - 1);
                } else if (excludedNodes.contains(arc.to)) {
                    // ignore it
                } else {
                    prefix.add(arc);
                    findPathsExcluding(arc.to,to,list,excludedNodes,prefix);
                    prefix.remove(prefix.size() - 1);
                }
            }
        }
        excludedNodes.remove(from);
    }

    private void makeImmutable()
    {
        if (mutable) {
            mutable = false;
            shortestPath.clear();
            for (Iterator iterator = arcs.iterator(); iterator.hasNext();) {
                Arc arc = (Arc) iterator.next();
                shortestPath.put(arc,new Arc [] { arc });
            }
            while (true) {
                // Take a copy of the map's keys to avoid
                // ConcurrentModificationExceptions.
                ArrayList previous = new ArrayList(shortestPath.keySet());
                int changeCount = 0;
                for (Iterator arcsIter = arcs.iterator(); arcsIter.hasNext();) {
                    Arc arc = (Arc) arcsIter.next();
                    for (
                        Iterator prevIter = previous.iterator();
                            prevIter.hasNext();) {
                        Arc arc2 = (Arc) prevIter.next();
                        if (arc.to.equals(arc2.from)) {
                            final Arc newArc = new Arc(arc.from,arc2.to);
                            Arc [] bestPath =
                                (Arc []) shortestPath.get(newArc);
                            Arc [] arc2Path = (Arc []) shortestPath.get(arc2);
                            if (
                                (bestPath == null)
                                    || (bestPath.length > (arc2Path.length + 1))) {
                                Arc [] newPath = new Arc[arc2Path.length + 1];
                                newPath[0] = arc;
                                System.arraycopy(
                                    arc2Path,
                                    0,
                                    newPath,
                                    1,
                                    arc2Path.length);
                                shortestPath.put(newArc,newPath);
                                changeCount++;
                            }
                        }
                    }
                }
                if (changeCount == 0) {
                    break;
                }
            }
        }
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * An <code>Arc</code> is a directed link between two nodes.
     * 
     * <p>
     * The nodes are compared according to {@link Object#equals} and {@link
     * Object#hashCode}. We assume that their {@link Object#toString} works,
     * too.
     * </p>
     */
    public static class Arc
    {
        public final Object from;
        public final Object to;
        private final String string; // for debug

        /**
         * Creates an arc.
         *
         * @pre from != null
         * @pre to != null
         */
        public Arc(Object from,Object to)
        {
            this.from = from;
            this.to = to;
            this.string = toString();
            Util.discard(this.string);
        }

        public boolean equals(Object obj)
        {
            if (obj instanceof Arc) {
                Arc other = (Arc) obj;
                return from.equals(other.from) && to.equals(other.to);
            }
            return false;
        }

        public int hashCode()
        {
            return from.hashCode() ^ (to.hashCode() << 4);
        }

        public String toString()
        {
            return from + "-" + to;
        }

        private static String toString(Arc [] arcs)
        {
            StringBuffer buf = new StringBuffer("{");
            for (int i = 0; i < arcs.length; i++) {
                if (i > 0) {
                    buf.append(", ");
                }
                buf.append(arcs[i].toString());
            }
            buf.append("}");
            return buf.toString();
        }
    }

    public static class GraphTest extends TestCase
    {
        public GraphTest(String name)
        {
            super(name);
        }

        public void test()
        {
            Graph g = new Graph();
            g.createArc("A","B");
            g.createArc("B","C");
            g.createArc("D","C");
            g.createArc("C","D");
            g.createArc("E","F");
            g.createArc("C","C");
            assertEquals(
                "{A-B, B-C, C-D}",
                Arc.toString(g.getShortestPath("A","D")));
            g.createArc("B","D");
            assertEquals(
                "{A-B, B-D}",
                Arc.toString(g.getShortestPath("A","D")));
            assertNull(
                "There is no path from A to E",
                g.getShortestPath("A","E"));
            assertEquals("{}",Arc.toString(g.getShortestPath("D","D")));
            assertNull(
                "Node X is not in the graph",
                g.getShortestPath("X","A"));
            assertEquals(
                "{A-B, B-D} {A-B, B-C, C-D}",
                toString(g.getPaths("A","D")));
        }

        private static String toString(final Iterator iter)
        {
            StringBuffer buf = new StringBuffer();
            int count = 0;
            while (iter.hasNext()) {
                Arc [] path = (Arc []) iter.next();
                if (count++ > 0) {
                    buf.append(" ");
                }
                buf.append(Arc.toString(path));
            }
            return buf.toString();
        }
    }
}


// End Graph.java
