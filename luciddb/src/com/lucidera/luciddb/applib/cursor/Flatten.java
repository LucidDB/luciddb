/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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
package com.lucidera.luciddb.applib.cursor;

import java.sql.*;
import java.util.*;
import org.jgrapht.graph.*;
import org.jgrapht.*;
import org.jgrapht.traverse.*;
import org.jgrapht.alg.*;

// REVIEW jvs 16-Aug-2006:  Our convention for applib UDX names is
// SomeClassUdx.  In this case, something more descriptive than just
// Flatten would be good; maybe FlattenRecursiveHierarchyUdx?  Same
// thing at the SQL level, so flatten_recursive_hierarchy.

/**
 * Flattens hierarchical data of any depth
 *
 * @author Khanh Vu
 * @version $Id$
 */
public abstract class Flatten
{
    /** 
     * Flatten UDX takes a two-column table, calls the execute
     * function of class Flatten to return a table of 17 columns
     * containing flattened paths of the graph
     * @param inputSet the input table
     * @param resultInserter the output table
     */
    public static void execute(
        ResultSet inputSet,
        PreparedStatement resultInserter)
        throws SQLException
    {
        // operation not supported 
        if (resultInserter.getParameterMetaData().getParameterCount() != 17) {
            // REVIEW jvs 16-Aug-2006:  Good form would be to
            // use a resource for this instead of hard-coding the string,
            // even though it should never get hit because everyone
            // should just use the preinstalled UDX in applib.
            throw new SQLException("Output table must have 17 columns");
        }

        Flatten.execute(inputSet, 15, resultInserter);
    }

    /**
     * Flattens a two-column table (first column contains parent nodes, second
     * column contains child nodes), and builds a directed graph representing
     * the relationship specified by the input table and returns a table.  Each
     * row of the output table represent a flattened path from a root to a
     * leaf.  The first column contains integer values representing the actual
     * path length (each edge has length 1). The second column contains boolean
     * values true if there exists multiple paths that end at the leaf node of
     * the path. Third to last columns contain vertices along the path starting
     * from the root ending at the leaf. The path is right-padded with
     * the duplicates of the leaf value.
     * @param inputSet input table
     * @param maxDepth nominal maximal depth of paths
     * @param resultInserter output table
     */ 
    public static void execute(
        ResultSet inputSet,
        int maxDepth,
        PreparedStatement resultInserter)
        throws SQLException
    {

        //~ variables ----------------------------------------------------------

        DirectedGraph<String, DefaultEdge> inGraph = 
            new DefaultDirectedGraph<String, DefaultEdge>(DefaultEdge.class);

        // REVIEW jvs 16-Aug-2006: Using a linked list here will cause a lot of
        // garbage-collection churn.  An ArrayList would work much better,
        // because once the array hits the high-water mark, no more allocations
        // will occur.  (ArrayList.clear just sets the size back to 0 and nulls
        // out the entries, but does not deallocate the array.)  However, you
        // can't efficiently prepend to the beginning of an array, so you have
        // to either use Collections.reverse at the end, or just iterate
        // it in reverse-order.
        LinkedList<String> path = new LinkedList<String>();
        
        // REVIEW jvs 16-Aug-2006: Any reason generics can't be used here,
        // other than the fact that the signature will be huge and
        // the Java people forgot to give us typedef?
        Map pathsFound = new HashMap();


        //~ validate and build graph -------------------------------------------

        // REVIEW jvs 16-Aug-2006: Precondition validation should happen as
        // early as possible in the method; and this string resource
        // should be internationalized because this would definitely
        // be a user-level error.
        
        // validate tables' number of columns
        if (inputSet.getMetaData().getColumnCount() != 2) {
            throw new SQLException("Input table must have 2 columns");
        }   

        // build graph from input
        try {
            buildGraphFromInput(inputSet, inGraph);
        } catch (SQLException e) {
            throw e;
        }
 
        // check if graph is acyclic
        CycleDetector<String, DefaultEdge> cyc = new CycleDetector(inGraph);
        if (cyc.detectCycles()) {
            // REVIEW jvs 16-Aug-2006: Need i18n.  When this occurs, it would
            // be nice to call cyc.findCycles and dump the list of
            // participating vertices.  But the output error message could be
            // huge, so cap it after 20 but report the total number in
            // the error message.
            throw new SQLException("Graph has cycle(s), cannot be flattened.");
        }


        // REVIEW jvs 16-Aug-2006: Instead of using a DFS, you can actually
        // just walk inGraph.vertexSet().
        
        // ~ Output paths to leaves --------------------------------------------
        // the type of iterator is not of importance

        GraphIterator<String, DefaultEdge> iter = new DepthFirstIterator(inGraph);

        // REVIEW jvs 16-Aug-2006: Why is path getting reinitialized here?
        path = new LinkedList<String>();

        while (iter.hasNext()) {
            String vertex = iter.next();

            if (inGraph.outDegreeOf(vertex) == 0) {
                outputPathsToLeaf(vertex, maxDepth, inGraph, path, pathsFound, resultInserter);
            }
        }
    }


    /**
     * Builds the directed graph from input table
     * @param inputSet input table
     * @param inGraph 
     */
    private static void buildGraphFromInput(
        ResultSet inputSet,
        DirectedGraph<String, DefaultEdge> inGraph)
        throws SQLException
    {
        // REVIEW jvs 16-Aug-2006: Why is a StringBuilder required here?
        // Why can't you just access the strings directly?
        StringBuilder sb = new StringBuilder();

        while (inputSet.next()) {
            sb.setLength(0);
            sb.append(inputSet.getString(1));
            String parent = sb.toString();
            
            // REVIEW jvs 16-Aug-2006:  Don't compare against the string
            // "null", since that may be a real data value.  Instead,
            // test the direct result of getString to see if it's a
            // Java null.
            
            // if parent is null, ignore the record
            if (!parent.equals("null")) {
                inGraph.addVertex(parent);    
            }
            
            sb.setLength(0);
            sb.append(inputSet.getString(2));
            String child = sb.toString();
            inGraph.addVertex(child);

            if (!parent.equals("null")) {
                inGraph.addEdge(parent, child);
            }
        }
    }


    /**
     * Finds and outputs to output table path(s) from a leaf. The function 
     * throws an exception if it finds a path longer than maxDepth
     * @param node the leaf node
     * @param maxDepth maximal length of paths
     * @param inGraph the input graph
     * @param path
     * @param pathsFound map of pairs <node, sets-of-path-found-from-node>
     * @param resultInserter output table
     */
    private static void outputPathsToLeaf(
        String node,
        int maxDepth,
        DirectedGraph<String, DefaultEdge> inGraph,
        LinkedList<String> path,
        Map pathsFound,
        PreparedStatement resultInserter)
        throws SQLException    
    {    
        path.clear();

        // REVIEW jvs 16-Aug-2006: Any time you find yourself naming
        // a variable "tmp", stop and ask whether there's a more meaningful
        // name.  In this case, maybe something like currNode.
        
        // walk up the graph to build path
        String tmpNode = node;
        Set<DefaultEdge> pathsToParents = inGraph.incomingEdgesOf(tmpNode);
        int numParents = pathsToParents.size();
        Iterator setIter = pathsToParents.iterator();
 
        path.add(tmpNode);

        while (numParents > 0) {
            // if the node has only one parent, walk up to build path to root
            if (numParents == 1) {
                // move tmpNode up
                tmpNode = inGraph.getEdgeSource((DefaultEdge)setIter.next());
                pathsToParents = inGraph.incomingEdgesOf(tmpNode);
                numParents = pathsToParents.size();
                setIter = pathsToParents.iterator();

                path.addFirst(tmpNode);
            }
            // if the node has multiple parents, call pathsToRoot
            // to print out multiple paths. This case should be rare
            else {
                // remove tmpNode from path
                path.removeFirst();
                LinkedList<LinkedList<String>> pathList =
                    pathsToRoot(inGraph,tmpNode, pathsFound);
                
                // get set of paths from tmpNode, append with path from leaf
                // to tmpNode, print out each paths
                int i;
                int len;
                for (i=0; i<pathList.size(); i++) { 
                    len = pathList.get(i).size();
                    pathList.get(i).addAll(len,path);
                    outputOneRow(pathList.get(i), maxDepth, true, resultInserter);
                }
                return;
            }
        }        
        outputOneRow(path, maxDepth, false, resultInserter);
    }


    /**
     * takes a list representing vertices in a path and sends them
     * to one row of the output table
     * @param path list of vertices
     * @param maxDepth maximal number of vertices in the path
     * @param multiple true if there is multiple paths ending at the leaf
     * @param resultInserter out put table
     */
    private static void outputOneRow(
        LinkedList<String> path,
        int maxDepth,
        boolean multiple,
        PreparedStatement resultInserter)
        throws SQLException
    {
        int len = path.size();

        if (len > maxDepth) {
            // REVIEW jvs 16-Aug-2006:  Provide path contents as
            // error context.  But I'm wondering if it might not be useful
            // to just output the truncated row instead, adding an extra
            // output column to indicate the exception.  Check with Anil.
            // Usually, they really hate it when an entire load fails due
            // to a few bad rows.
            throw new SQLException("Path is deeper than specified maxDepth");
        }

        resultInserter.setInt(1, len-1);
        resultInserter.setBoolean(2, multiple);
        
        int i;
        for (i=0; i<len; i++) {
            resultInserter.setString(i+3, path.get(i));
        }

        String pad = path.get(i-1);
        for (; i<maxDepth; i++) {
            resultInserter.setString(i+3, pad);
        }

        resultInserter.executeUpdate();
    }

    /** 
     * Finds the list of all paths that end at node
     * @param g  the graph to be traversed
     * @param node the node where paths end
     * @param pathsFound the auxilary map for looking up paths found
     * @return a list of list; set of paths ending at the node
     */
    private static LinkedList<LinkedList<String>> pathsToRoot(
        DirectedGraph<String,DefaultEdge> g, 
        String node,
        Map pathsFound) 
    {
        List<String> parents = Graphs.predecessorListOf(g,node);

        // if the node is a root, return itself
        if (parents.isEmpty()) {
            LinkedList<String> path = new LinkedList<String>();
            path.add(node);
            LinkedList<LinkedList<String>> pathList = 
                new LinkedList<LinkedList<String>>();
            pathList.add(path);
            return pathList;
        }
        // if the node is not a root
        else {
            // if paths to the node have been found in a previous visit
            if (pathsFound.containsKey(node)) {
                LinkedList<LinkedList<String>> pathList = 
                    new LinkedList<LinkedList<String>>();
                LinkedList<LinkedList<String>> temp = 
                    (LinkedList<LinkedList<String>>) pathsFound.get(node);

                // copy value
                int k;
                for (k=0; k<temp.size(); k++) {
                    pathList.add((LinkedList<String>) (temp.get(k)).clone());
                }
                return pathList;
            }
            // if the node has not been visited before
            else {               
                LinkedList<LinkedList<String>> pathList = 
                    new LinkedList<LinkedList<String>>();

                int i;
                for (i=0; i<parents.size(); i++) {
                    LinkedList<LinkedList<String>> pathSublist = 
                        pathsToRoot(g, parents.get(i), pathsFound);

                    int j;
                    for (j=0; j<pathSublist.size(); j++) {
                        (pathSublist.get(j)).add(node);
                    }
                    pathList.addAll(0,pathSublist);
                }

                LinkedList<LinkedList<String>> temp = 
                    new LinkedList<LinkedList<String>>();

                // copy value of pathList to temp
                int k;
                for (k=0; k<pathList.size(); k++) {
                    temp.add((LinkedList<String>) (pathList.get(k)).clone());
                }

                pathsFound.put(node, temp);

                return pathList;
            }
        }
    }
}

// End Flatten.java
