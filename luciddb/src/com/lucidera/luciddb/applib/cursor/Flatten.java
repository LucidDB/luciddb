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
            throw new SQLException("Output table must have 17 columns");
        }

        Flatten.execute(inputSet, 15, resultInserter);
    }

    /**
     * Flatten function takes a two-column table (first column contains
     * parent nodes, second column contains child nodes), builds a directed
     * graph representing the relationship specified by the input table
     * and returns a table. Each row of the output table represent a flattened
     * path from a root to a leaf.The first column contains integer values 
     * representing the actual path length (each edge has length 1). The
     * second values contains boolean values true if there exists multiple
     * paths that end at the leaf node of the path. Third to last column
     * contain vertices along the path starting from the root ending at
     * the leaf. The path is right-padded with values of the leaf
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
        LinkedList<String> path = new LinkedList<String>();
        Map pathsFound = new HashMap();


        //~ validate and build graph -------------------------------------------

        // validate tables' number of columns
        if (inputSet.getMetaData().getColumnCount() != 2) {
            throw new SQLException("Input table must have 2 columns");
        }   

        // build graph from input
        try 
        {
            buildGraphFromInput(inputSet, inGraph);
        } catch (SQLException e) {
            throw e;
        }
 
        // check if graph is acyclic
        CycleDetector<String, DefaultEdge> cyc = new CycleDetector(inGraph);
        if (cyc.detectCycles()) {
            throw new SQLException("Graph has cycle(s), cannot be flattened.");
        }


        // ~ Output paths to leaves --------------------------------------------
        // the type of iterator is not of importance

        GraphIterator<String, DefaultEdge> iter = new DepthFirstIterator(inGraph);
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
        StringBuilder sb = new StringBuilder();

        while (inputSet.next()) {
            sb.setLength(0);
            sb.append(inputSet.getString(1));
            String parent = sb.toString();
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
