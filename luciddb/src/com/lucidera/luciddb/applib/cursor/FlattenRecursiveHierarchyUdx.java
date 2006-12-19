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
import com.lucidera.luciddb.applib.resource.*;

/**
 * Flattens hierarchical data of any depth
 *
 * @author Khanh Vu
 * @version $Id$
 */
public abstract class FlattenRecursiveHierarchyUdx
{
    /** 
     * Calls the below execute function to return a table of 17 columns
     * containing flattened paths of the graph
     * @param inputSet the input table
     * @param resultInserter the output table
     * @exception ApplibException
     */
    public static void execute(
        ResultSet inputSet,
        PreparedStatement resultInserter)
        throws ApplibException
    {
        FlattenRecursiveHierarchyUdx.execute(inputSet, 15, resultInserter);
    }

    /**
     * Flattens a two-column table (first column contains parent nodes, second
     * column contains child nodes) and output to another table. Each
     * row of the output table represent a flattened path from a root to a
     * leaf.  The first column contains integer values representing the actual
     * number of vertices on the path. The second column contains boolean
     * values true if there exists multiple paths that end at the leaf node of
     * the path. Third to last columns contain vertices along the path starting
     * from the root ending at the leaf. If the number of vertices is less than
     * maxDepth, the path is right-padded with value of the leaf. If the
     * number of vertices is greater than maxDepth, the path is truncated
     * (actual number of vertices on the path before truncate is shown in 
     * first column).
     * @param inputSet input table
     * @param maxDepth nominal maximal depth of paths
     * @param resultInserter output table
     * @exception ApplibException
     */ 
    public static void execute(
        ResultSet inputSet,
        int maxDepth,
        PreparedStatement resultInserter)
        throws ApplibException
    {
        executeImpl(inputSet, maxDepth, false, resultInserter);
    }
        
    /**
     * Same as execute, but returns rows for non-leaf vertices as well.
     * Produces one extra column (after the first two fixed columns)
     * containing a boolean indicating whether a given output row
     * corresponds to a non-leaf vertex.
     *
     * @param inputSet input table
     * @param maxDepth nominal maximal depth of paths
     * @param resultInserter output table
     * @exception ApplibException
     */
    public static void executeAllLevels(
        ResultSet inputSet,
        PreparedStatement resultInserter)
        throws ApplibException
    {
        executeImpl(inputSet, 15, true, resultInserter);
    }
    
    private static void executeImpl(
        ResultSet inputSet,
        int maxDepth,
        boolean allLevels,
        PreparedStatement resultInserter)
        throws ApplibException
    {

        //~ validate -----------------------------------------------------------

        int n;
        try {
            n = inputSet.getMetaData().getColumnCount();
        } catch (SQLException e) {
            throw ApplibResourceObject.get().CannotGetMetaData.ex(e);
        }

        if (n != 2) {
            throw ApplibResourceObject.get().WrongNumberInputColumns.ex(
                String.valueOf(n));
        }   


        //~ build graph---------------------------------------------------------

        DirectedGraph<String, DefaultEdge> inGraph = 
            new DefaultDirectedGraph<String, DefaultEdge>(DefaultEdge.class);

        ArrayList<String> path = new ArrayList<String>();
        HashMap<String, ArrayList<ArrayList<String>>> pathsFound = new HashMap();

        try {
            buildGraphFromInput(inputSet, inGraph);
        } catch (SQLException e) {
            throw ApplibResourceObject.get().CannotReadInput.ex(e);
        }
 
        // check if graph is acyclic
        CycleDetector<String, DefaultEdge> cyc = new CycleDetector(inGraph);
        if (cyc.detectCycles()) {
            Set<String> verticesInLoop = cyc.findCycles();
            StringBuilder sb = new StringBuilder();
            int i = 0;
            Iterator iter = verticesInLoop.iterator();

            while (iter.hasNext() && i<20) {
                sb.append((String) iter.next() + " ");
                i++;
            }

            throw ApplibResourceObject.get().GraphHasCycle.ex(
                String.valueOf(verticesInLoop.size()), sb.toString());

        }

      
        // ~ Output paths to vertices-------------------------------------------
        Iterator iter = (inGraph.vertexSet()).iterator();

        while (iter.hasNext()) {
            String vertex = (String) iter.next();

            boolean isLeaf = (inGraph.outDegreeOf(vertex) == 0);
            if (allLevels || isLeaf) {
                outputPathsToVertex(
                    vertex, maxDepth, inGraph, path, pathsFound,
                    allLevels, !isLeaf, resultInserter);
            }
        }
    }


    /**
     * Builds the directed graph from input table
     * @param inputSet input table
     * @param inGraph 
     * @exception ApplibException SQLException
     */
    private static void buildGraphFromInput(
        ResultSet inputSet,
        DirectedGraph<String, DefaultEdge> inGraph)
        throws ApplibException, SQLException
    {

        String parent, child;

        while (inputSet.next()) {
            parent = inputSet.getString(1);
            child = inputSet.getString(2);

            if (child == null) {
                throw ApplibResourceObject.get().NullChild.ex(parent);
            }
            else {
                inGraph.addVertex(child);
            }

            if (parent != null) {
                inGraph.addVertex(parent);   
                inGraph.addEdge(parent, child);
            }
        }
    }


    /**
     * Finds and outputs path(s) from a leaf.
     * @param node the leaf node
     * @param maxDepth maximal length of paths
     * @param inGraph the input graph
     * @param path
     * @param pathsFound map of pairs <node, sets-of-path-found-from-node>
     * @param allLevels
     * @param nonLeaf
     * @param resultInserter output table
     * @exception ApplibException
     */
    private static void outputPathsToVertex(
        String node,
        int maxDepth,
        DirectedGraph<String, DefaultEdge> inGraph,
        ArrayList<String> path,
        Map pathsFound,
        boolean allLevels,
        boolean nonLeaf,
        PreparedStatement resultInserter)
        throws ApplibException    
    {
        path.clear();

        if (nonLeaf) {
            assert(allLevels);
        }
        
        // walk up the graph to build path
        String currNode = node;
        Set<DefaultEdge> pathsToParents = inGraph.incomingEdgesOf(currNode);
        int numParents = pathsToParents.size();
        Iterator setIter = pathsToParents.iterator();
 
        path.add(currNode);

        while (numParents > 0) {
            // if the node has only one parent, walk up to build path to root
            if (numParents == 1) {
                // move currNode up
                currNode = inGraph.getEdgeSource((DefaultEdge)setIter.next());
                pathsToParents = inGraph.incomingEdgesOf(currNode);
                numParents = pathsToParents.size();
                setIter = pathsToParents.iterator();

                path.add(currNode);
            }
            // if the node has multiple parents, call pathsToRoot
            // to print out multiple paths. This case should be rare
            else {
                path.remove(path.size()-1);
                Collections.reverse(path);
                ArrayList<ArrayList<String>> pathList =
                    pathsToRoot(inGraph,currNode, pathsFound);
                
                // get set of paths from currNode, append with path from leaf
                // to currNode, print out each paths
                int i;
                int len;
                for (i=0; i<pathList.size(); i++) { 
                    len = pathList.get(i).size();
                    pathList.get(i).addAll(len,path);
                    try {
                        outputOneRow(
                            pathList.get(i), maxDepth, true,
                            allLevels, nonLeaf, resultInserter);
                    } catch (SQLException e) {
                        throw ApplibResourceObject.get().CannotWriteOutput.ex(
                            e);
                    }
                }
                return;
            }
        }  
        Collections.reverse(path);
        try {
            outputOneRow(path, maxDepth, false,
                allLevels, nonLeaf, resultInserter);
        } catch (SQLException e) {
            throw ApplibResourceObject.get().CannotWriteOutput.ex(e);
        }
    }


    /**
     * Takes a list representing vertices in a path and sends them
     * to one row of the output table
     * @param path list of vertices
     * @param maxDepth maximal number of vertices in the path
     * @param multiple true if there is multiple paths ending at the vertex
     * @param allLevels true if producing rows for both leaf and non-leaf
     * @param nonLeaf true if this row represents a non-leaf
     * @param resultInserter out put table
     * @exception SQLException
     */
    private static void outputOneRow(
        List<String> path,
        int maxDepth,
        boolean multiple,
        boolean allLevels,
        boolean nonLeaf,
        PreparedStatement resultInserter)
        throws SQLException
    {
        int len = path.size();

        resultInserter.setInt(1, len);
        resultInserter.setBoolean(2, multiple);
        int iBase = 3;
        if (allLevels) {
            resultInserter.setBoolean(iBase, nonLeaf);
            ++iBase;
        }
        
        int i;
        for (i=0; (i<len)&&(i<maxDepth); i++) {
            resultInserter.setString(i+iBase, path.get(i));
        }

        String pad = path.get(i-1);
        for (; i<maxDepth; i++) {
            resultInserter.setString(i+iBase, pad);
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
    private static ArrayList<ArrayList<String>> pathsToRoot(
        DirectedGraph<String,DefaultEdge> g, 
        String node,
        Map pathsFound) 
    {
        List<String> parents = Graphs.predecessorListOf(g,node);

        // if the node is a root, return itself
        if (parents.isEmpty()) {
            ArrayList<String> path = new ArrayList<String>();
            path.add(node);
            ArrayList<ArrayList<String>> pathList = 
                new ArrayList<ArrayList<String>>();
            pathList.add(path);
            return pathList;
        }
        // if the node is not a root
        else {
            // if paths to the node have been found in a previous visit
            if (pathsFound.containsKey(node)) {
                ArrayList<ArrayList<String>> pathList = 
                    new ArrayList<ArrayList<String>>();
                ArrayList<ArrayList<String>> pathsToNode = 
                    (ArrayList<ArrayList<String>>) pathsFound.get(node);

                // copy value
                int k;
                for (k=0; k<pathsToNode.size(); k++) {
                    pathList.add(
                        (ArrayList<String>) (pathsToNode.get(k)).clone());
                }
                return pathList;
            }
            // if the node has not been visited before
            else {               
                ArrayList<ArrayList<String>> pathList = 
                    new ArrayList<ArrayList<String>>();

                int i;
                for (i=0; i<parents.size(); i++) {
                    ArrayList<ArrayList<String>> pathSublist = 
                        pathsToRoot(g, parents.get(i), pathsFound);

                    int j;
                    for (j=0; j<pathSublist.size(); j++) {
                        (pathSublist.get(j)).add(node);
                    }
                    pathList.addAll(0,pathSublist);
                }

                ArrayList<ArrayList<String>> pathsToNode = 
                    new ArrayList<ArrayList<String>>();

                // copy value of pathList to pathsToNode
                int k;
                for (k=0; k<pathList.size(); k++) {
                    pathsToNode.add(
                        (ArrayList<String>) (pathList.get(k)).clone());
                }

                pathsFound.put(node, pathsToNode);

                return pathList;
            }
        }
    }
}

// End FlattenRecursiveHierarchyUdx.java
