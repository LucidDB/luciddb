/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
package com.lucidera.lurql;

import org._3pq.jgrapht.*;
import org._3pq.jgrapht.graph.*;
import org._3pq.jgrapht.traverse.*;

import org.eigenbase.util.*;
import org.eigenbase.jmi.*;

import org.netbeans.api.mdr.*;

import java.util.*;
import java.sql.*;

import javax.jmi.reflect.*;
import javax.jmi.model.*;

/**
 * LurqlReflectiveExecutor executes a {@link LurqlPlan} via calls to
 * the JMI reflective interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LurqlReflectiveExecutor
{
    private static final RefObject [] EMPTY_REFOBJ_ARRAY = new RefObject[0];

    private final MDRepository repos;
    
    private final LurqlPlan plan;

    private final Connection sqlConnection;
    
    private Map vertexToResultMap;

    private Map vertexToStashMap;

    private Map sqlMap;

    /**
     * Creates a new executor for a plan.
     *
     * @param repos the repository to be accessed
     *
     * @param plan the plan to be executed
     *
     * @param sqlConnection JDBC connection for evaluation of SQL queries,
     * or null if no SQL context is available
     */
    public LurqlReflectiveExecutor(
        MDRepository repos,
        LurqlPlan plan,
        Connection sqlConnection)
    {
        this.repos = repos;
        this.plan = plan;
        this.sqlConnection = sqlConnection;
    }

    /**
     * Executes the plan specified by the constructor.
     *
     * @return objects found (as a modifiable set of RefObjects)
     */
    public Set execute()
        throws JmiQueryException
    {
        sqlMap = new HashMap();
        vertexToResultMap = new HashMap();
        vertexToStashMap = new HashMap();
        Set finalResult = new HashSet();
        
        // execute plan
        DirectedGraph graph = plan.getGraph();
        Iterator vertexIter = new TopologicalOrderIterator(graph);
        while (vertexIter.hasNext()) {
            LurqlPlanVertex planVertex = (LurqlPlanVertex) vertexIter.next();
            Set result = getResultSet(planVertex);
            if (graph.inDegreeOf(planVertex) == 0) {
                executeRoot(planVertex, result);
            }
            if (planVertex.isRecursive()) {
                if (planVertex.getRecursionSubgraph() != null) {
                    executeRecursion(planVertex);
                    // take care of non-recursive outputs
                    executeOutgoingEdges(graph, planVertex, result, false);
                } else {
                    // execution of this vertex is handled as part of
                    // an ancestor's recursion subgraph
                }
            } else {
                executeOutgoingEdges(graph, planVertex, result, true);
            }
            if (plan.isSelected(planVertex.getAlias())) {
                finalResult.addAll(result);
            }
        }

        vertexToResultMap = null;
        vertexToStashMap = null;
        sqlMap = null;
        return finalResult;
    }

    private void executeRecursion(LurqlPlanVertex rootVertex)
        throws JmiQueryException
    {
        // materialize execution order
        List vertexList = Util.toList(
            new TopologicalOrderIterator(
                rootVertex.getRecursionSubgraph()));

        Set recursionResult = getResultSet(rootVertex);
        Set stashResult = getResultSet(vertexToStashMap, rootVertex);

        int prevSize;

        // outer loop:  until we hit a fixpoint
        do {
            // record pre-recursion result size so we can see if we picked
            // up anything new
            prevSize = stashResult.size();

            // inner loop:  execute one recursion level
            Iterator iter = vertexList.iterator();
            while (iter.hasNext()) {
                LurqlPlanVertex planVertex = (LurqlPlanVertex) iter.next();
                Set result = getResultSet(planVertex);
                executeOutgoingEdges(
                    rootVertex.getRecursionSubgraph(), planVertex, result,
                    true);
                LurqlPlanVertex recursionRoot = planVertex.getRecursionRoot();
                if (recursionRoot != null) {
                    assert(recursionRoot == rootVertex);
                    recursionResult.addAll(result);
                }
            }

            // transfer current results to stash, clearing current results for
            // next round, but preserving root deltas
            transferResults(
                vertexList, vertexToStashMap, vertexToResultMap,
                rootVertex);
            
            assert(stashResult.size() >= prevSize);
        } while (stashResult.size() != prevSize);

        // transfer stashed results to become final results
        transferResults(vertexList, vertexToResultMap, vertexToStashMap, null);
    }

    private void transferResults(
        List vertexList, Map dstMap, Map srcMap,
        LurqlPlanVertex rootVertex)
        throws JmiQueryException
    {
        Iterator iter = vertexList.iterator();
        while (iter.hasNext()) {
            LurqlPlanVertex planVertex = (LurqlPlanVertex) iter.next();
            Set srcResult = getResultSet(srcMap, planVertex);
            Set dstResult = getResultSet(dstMap, planVertex);
            if (planVertex == rootVertex) {
                // some set arithmetic to leave only the new results
                // in src
                Set delta = new HashSet(srcResult);
                delta.removeAll(dstResult);
                dstResult.addAll(delta);
                srcResult.clear();
                srcResult.addAll(delta);
            } else {
                dstResult.addAll(srcResult);
                srcResult.clear();
            }
        }
    }

    private void executeRoot(LurqlPlanVertex planVertex, Set output)
        throws JmiQueryException
    {
        LurqlFilter [] filters = getFilters(planVertex);
        
        if (planVertex.getRootObjectIds().isEmpty()) {
            Iterator iter = planVertex.getClassVertexSet().iterator();
            while (iter.hasNext()) {
                JmiClassVertex classVertex = (JmiClassVertex) iter.next();
                executeFilters(
                    classVertex.getRefClass().refAllOfType(),
                    output,
                    filters,
                    null);
            }
        } else {
            List objList = new ArrayList();
            Iterator iter = planVertex.getRootObjectIds().iterator();
            while (iter.hasNext()) {
                String mofId = (String) iter.next();
                RefBaseObject refObj = repos.getByMofId(mofId);
                if (refObj != null) {
                    objList.add(refObj);
                }
            }
            executeFilters(objList, output, filters, null);
        }
    }

    private LurqlFilter [] getFilters(LurqlPlanVertex planVertex)
    {
        return (LurqlFilter [])
            planVertex.getFilters().toArray(LurqlFilter.EMPTY_ARRAY);
    }

    private void executeOutgoingEdges(
        DirectedGraph graph, LurqlPlanVertex planVertex, Set input,
        boolean executeRecursive)
        throws JmiQueryException
    {
        // we're going to repetitively iterate the obj list, so
        // copy it as an array
        RefObject [] objArray = (RefObject [])
            input.toArray(EMPTY_REFOBJ_ARRAY);

        Iterator edgeIter =
            graph.outgoingEdgesOf(planVertex).iterator();
        while (edgeIter.hasNext()) {
            LurqlPlanEdge edge = (LurqlPlanEdge) edgeIter.next();
            if (!executeRecursive && edge.getPlanTarget().isRecursive()) {
                continue;
            }
            RefAssociation refAssoc = edge.getAssocEdge().getRefAssoc();
            AssociationEnd originEnd = edge.getOriginEnd();
            Classifier originType = originEnd.getType();
            LurqlFilter [] filters = getFilters(edge.getPlanTarget());
            Set output = getResultSet(edge.getPlanTarget());
            for (int i = 0; i < objArray.length; ++i) {
                RefObject refObj = objArray[i];
                // REVIEW:  maybe it's faster to just swallow the excns
                // that would result if we skipped this precheck?
                if (!refObj.refIsInstanceOf(originType, true)) {
                    continue;
                }
                executeFilters(
                    refAssoc.refQuery(originEnd, refObj),
                    output,
                    filters,
                    edge.getDestinationTypeFilter());
            }
        }
    }

    private void executeFilters(
        Collection input, Set output, LurqlFilter [] filters,
        JmiClassVertex typeFilter)
        throws JmiQueryException
    {
        if ((filters.length == 0) && (typeFilter == null)) {
            output.addAll(input);
            return;
        }

        Iterator iter = input.iterator();
outer:
        while (iter.hasNext()) {
            RefObject refObj = (RefObject) iter.next();
            if (typeFilter != null) {
                if (!refObj.refIsInstanceOf(
                        typeFilter.getMofClass(), true))
                {
                    continue outer;
                }
            }
            for (int i = 0; i < filters.length; ++i) {
                String value;
                if (filters[i].isMofId()) {
                    value = refObj.refMofId();
                } else {
                    Object objValue = refObj.refGetValue(
                        filters[i].getAttributeName());
                    if (objValue == null) {
                        continue outer;
                    }
                    if (objValue instanceof RefObject) {
                        value = ((RefObject) objValue).refMofId();
                    } else {
                        value = objValue.toString();
                    }
                }
                Set filterValues = getFilterValues(filters[i]);
                if (!filterValues.contains(value)) {
                    continue outer;
                }
            }
            output.add(refObj);
        }
    }

    private Set getFilterValues(LurqlFilter filter)
        throws JmiQueryException
    {
        if (filter.getValues() != null) {
            return filter.getValues();
        }

        Set set = (Set) sqlMap.get(filter.getSqlQuery());
        if (set != null) {
            return set;
        }

        if (sqlConnection == null) {
            throw plan.newException("no SQL connection available");
        }
        set = new HashSet();
        Statement stmt = null;
        try {
            stmt = sqlConnection.createStatement();
            ResultSet resultSet = stmt.executeQuery(filter.getSqlQuery());
            while (resultSet.next()) {
                String s = resultSet.getString(1);
                set.add(s);
            }
        } catch (SQLException ex) {
            throw plan.newException("error executing SQL subquery", ex);
        } finally {
            Util.squelchStmt(stmt);
        }
        sqlMap.put(filter.getSqlQuery(), set);
        
        return set;
    }
    
    private Set findResultSet(Map map, LurqlPlanVertex planVertex)
    {
        return (Set) map.get(planVertex);
    }
    
    private Set getResultSet(Map map, LurqlPlanVertex planVertex)
    {
        Set set = findResultSet(map, planVertex);
        if (set == null) {
            set = new HashSet();
            map.put(planVertex, set);
        }
        return set;
    }
    
    private Set findResultSet(LurqlPlanVertex planVertex)
    {
        return findResultSet(vertexToResultMap, planVertex);
    }
    
    private Set getResultSet(LurqlPlanVertex planVertex)
    {
        return getResultSet(vertexToResultMap, planVertex);
    }
}

// End LurqlReflectiveExecutor.java
