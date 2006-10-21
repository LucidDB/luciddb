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

import java.sql.*;

import java.util.*;

import javax.jmi.model.*;
import javax.jmi.reflect.*;

import org._3pq.jgrapht.*;
import org._3pq.jgrapht.traverse.*;

import org.eigenbase.jmi.*;
import org.eigenbase.util.*;

import org.netbeans.api.mdr.*;


/**
 * LurqlReflectiveExecutor executes a {@link LurqlPlan} via calls to the JMI
 * reflective interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LurqlReflectiveExecutor
{

    //~ Static fields/initializers ---------------------------------------------

    private static final RefObject [] EMPTY_REFOBJ_ARRAY = new RefObject[0];

    //~ Instance fields --------------------------------------------------------

    private final MDRepository repos;

    private final LurqlPlan plan;

    private final Connection sqlConnection;

    private Map<LurqlPlanVertex, Set<RefObject>> vertexToResultMap;

    private Map<LurqlPlanVertex, Set<RefObject>> vertexToStashMap;

    private Map<LurqlFilter,Set<Object>> filterMap;

    private Map<String, ?> args;

    private Set<RefObject> finalResult;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new executor for a plan.
     *
     * @param repos the repository to be accessed
     * @param plan the plan to be executed
     * @param sqlConnection JDBC connection for evaluation of SQL queries, or
     * null if no SQL context is available
     * @param args argument values for parameters
     */
    public LurqlReflectiveExecutor(
        MDRepository repos,
        LurqlPlan plan,
        Connection sqlConnection,
        Map<String,?> args)
    {
        this.repos = repos;
        this.plan = plan;
        this.sqlConnection = sqlConnection;
        this.args = args;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Executes the plan specified by the constructor.
     *
     * @return objects found (as a modifiable set of RefObjects)
     */
    public Set<RefObject> execute()
        throws JmiQueryException
    {
        filterMap = new HashMap<LurqlFilter, Set<Object>>();
        vertexToResultMap = new HashMap<LurqlPlanVertex, Set<RefObject>>();
        vertexToStashMap = new HashMap<LurqlPlanVertex, Set<RefObject>>();
        finalResult = new HashSet<RefObject>();

        // execute plan
        DirectedGraph<LurqlPlanVertex, LurqlPlanEdge> graph = plan.getGraph();
        executeGraph(graph);

        vertexToResultMap = null;
        vertexToStashMap = null;
        filterMap = null;
        Set<RefObject> result = finalResult;
        finalResult = null;
        return result;
    }

    private void executeGraph(
        DirectedGraph<LurqlPlanVertex, LurqlPlanEdge> graph)
        throws JmiQueryException
    {
        Iterator<LurqlPlanVertex> vertexIter = 
            new TopologicalOrderIterator<
                LurqlPlanVertex,
                LurqlPlanEdge,
                Object>(graph);
        while (vertexIter.hasNext()) {
            LurqlPlanVertex planVertex = vertexIter.next();
            Set<RefObject> result = getResultSet(planVertex);
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
    }

    private void executeRecursion(LurqlPlanVertex rootVertex)
        throws JmiQueryException
    {
        // materialize execution order
        List<LurqlPlanVertex> vertexList =
            Util.toList(
                new TopologicalOrderIterator<
                    LurqlPlanVertex,
                    LurqlPlanEdge,
                    Object>(
                    rootVertex.getRecursionSubgraph()));

        Set<RefObject> recursionResult = getResultSet(rootVertex);
        Set stashResult = getResultSet(vertexToStashMap, rootVertex);

        int prevSize;

        // outer loop:  until we hit a fixpoint
        do {
            // record pre-recursion result size so we can see if we picked
            // up anything new
            prevSize = stashResult.size();

            // inner loop:  execute one recursion level
            for (LurqlPlanVertex planVertex : vertexList) {
                Set<RefObject> result = getResultSet(planVertex);
                executeOutgoingEdges(
                    rootVertex.getRecursionSubgraph(),
                    planVertex,
                    result,
                    true);
                LurqlPlanVertex recursionRoot = planVertex.getRecursionRoot();
                if (recursionRoot != null) {
                    assert (recursionRoot == rootVertex);
                    recursionResult.addAll(result);
                }
            }

            // transfer current results to stash, clearing current results for
            // next round, but preserving root deltas
            transferResults(
                vertexList,
                vertexToStashMap,
                vertexToResultMap,
                rootVertex);

            assert (stashResult.size() >= prevSize);
        } while (stashResult.size() != prevSize);

        // transfer stashed results to become final results
        transferResults(vertexList, vertexToResultMap, vertexToStashMap, null);
    }

    private void transferResults(
        List<LurqlPlanVertex> vertexList,
        Map<LurqlPlanVertex, Set<RefObject>> dstMap,
        Map<LurqlPlanVertex, Set<RefObject>> srcMap,
        LurqlPlanVertex rootVertex)
        throws JmiQueryException
    {
        for (LurqlPlanVertex planVertex : vertexList) {
            Set<RefObject> srcResult = getResultSet(srcMap, planVertex);
            Set<RefObject> dstResult = getResultSet(dstMap, planVertex);
            if (planVertex == rootVertex) {
                // some set arithmetic to leave only the new results
                // in src
                Set<RefObject> delta = new HashSet<RefObject>(srcResult);
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

    private void executeRoot(LurqlPlanVertex planVertex, Set<RefObject> output)
        throws JmiQueryException
    {
        LurqlFilter [] filters = getFilters(planVertex);
        LurqlPlanExistsEdge [] existsEdges =
            getExistsEdges(
                plan.getGraph(),
                planVertex);

        if (planVertex.getRootObjectIds().isEmpty()) {
            for (JmiClassVertex classVertex : planVertex.getClassVertexSet()) {
                executeFilters(
                    classVertex.getRefClass().refAllOfType(),
                    output,
                    filters,
                    existsEdges,
                    null);
            }
        } else {
            List<RefObject> objList = new ArrayList<RefObject>();
            for (String mofId : planVertex.getRootObjectIds()) {
                RefObject refObj = (RefObject) repos.getByMofId(mofId);
                if (refObj != null) {
                    objList.add(refObj);
                }
            }
            executeFilters(
                objList,
                output,
                filters,
                existsEdges,
                null);
        }
    }

    private LurqlFilter [] getFilters(LurqlPlanVertex planVertex)
    {
        return
            (LurqlFilter []) planVertex.getFilters().toArray(
                LurqlFilter.EMPTY_ARRAY);
    }

    private LurqlPlanExistsEdge [] getExistsEdges(
        DirectedGraph graph,
        LurqlPlanVertex planVertex)
    {
        List<Object> list = new ArrayList<Object>();
        for (Object obj : graph.outgoingEdgesOf(planVertex)) {
            if (!(obj instanceof LurqlPlanExistsEdge)) {
                continue;
            }
            list.add(obj);
        }
        return
            (LurqlPlanExistsEdge []) list.toArray(
                LurqlPlanExistsEdge.EMPTY_ARRAY);
    }

    private void executeOutgoingEdges(
        DirectedGraph<LurqlPlanVertex, LurqlPlanEdge> graph,
        LurqlPlanVertex planVertex,
        Set<RefObject> input,
        boolean executeRecursive)
        throws JmiQueryException
    {
        // we're going to repetitively iterate the obj list, so
        // copy it as an array
        RefObject [] objArray =
            (RefObject []) input.toArray(EMPTY_REFOBJ_ARRAY);

        for (LurqlPlanEdge edgeObj : graph.outgoingEdgesOf(planVertex))
        {
            if (!(edgeObj instanceof LurqlPlanFollowEdge)) {
                // dummy edge for exists
                continue;
            }
            LurqlPlanFollowEdge edge = (LurqlPlanFollowEdge) edgeObj;
            if (!executeRecursive && edge.getPlanTarget().isRecursive()) {
                continue;
            }
            RefAssociation refAssoc = edge.getAssocEdge().getRefAssoc();
            AssociationEnd originEnd = edge.getOriginEnd();
            Classifier originType = originEnd.getType();
            LurqlFilter [] filters = getFilters(edge.getPlanTarget());
            LurqlPlanExistsEdge [] existsEdges =
                getExistsEdges(
                    graph,
                    edge.getPlanTarget());
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
                    existsEdges,
                    edge.getDestinationTypeFilter());
            }
        }
    }

    private void executeFilters(
        Collection<RefObject> input,
        Set<RefObject> output,
        LurqlFilter [] filters,
        LurqlPlanExistsEdge [] existsEdges,
        JmiClassVertex typeFilter)
        throws JmiQueryException
    {
        if ((filters.length == 0) && (existsEdges.length == 0)
            && (typeFilter == null)) {
            output.addAll(input);
            return;
        }

        Iterator iter = input.iterator();
outer:
        while (iter.hasNext()) {
            RefObject refObj = (RefObject) iter.next();
            if (typeFilter != null) {
                if (!refObj.refIsInstanceOf(
                        typeFilter.getMofClass(),
                        true)) {
                    continue outer;
                }
            }
            for (int i = 0; i < filters.length; ++i) {
                String value;
                if (filters[i].isMofId()) {
                    value = refObj.refMofId();
                } else {
                    Object objValue =
                        refObj.refGetValue(
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
                if (filters[i].isPattern()) {
                    assert(filterValues.size() == 1);
                    boolean match = filters[i].patternMatch(
                        (String) filterValues.iterator().next(),
                        value);
                    if (!match) {
                        continue outer;
                    }
                } else {
                    if (!filterValues.contains(value)) {
                        continue outer;
                    }
                }
            }
            for (int i = 0; i < existsEdges.length; ++i) {
                if (!testExists(refObj, existsEdges[i])) {
                    continue outer;
                }
            }
            output.add(refObj);
        }
    }

    private boolean testExists(
        RefObject refObj,
        LurqlPlanExistsEdge existsEdge)
        throws JmiQueryException
    {
        // First, set object id in exists root.
        LurqlPlanVertex root = existsEdge.getPlanTarget();
        root.getRootObjectIds().clear();
        root.getRootObjectIds().add(refObj.refMofId());

        DirectedGraph graph = existsEdge.getSubgraph();

        // Execute sub-plan.
        executeGraph(graph);

        // Walk subgraph, clearing and checking results.
        Set projectSet = existsEdge.getProjectSet();
        int nResultsTotal = 0;
        for (Object obj : graph.vertexSet()) {
            LurqlPlanVertex planVertex = (LurqlPlanVertex) obj;
            Set result = findResultSet(vertexToResultMap, planVertex);
            if (result != null) {
                int nResults = result.size();
                result.clear();

                // Never count the root; it always has the seed as a result.
                if (planVertex == root) {
                    continue;
                }
                if (projectSet != null) {
                    // Check whether this is one of the variables selected
                    // by the exists.
                    if (!projectSet.contains(planVertex.getAlias())) {
                        continue;
                    }
                }
                nResultsTotal += nResults;
            }
        }

        return nResultsTotal > 0;
    }

    private Set getFilterValues(LurqlFilter filter)
        throws JmiQueryException
    {
        if ((filter.getValues() != null) && !filter.hasDynamicParams()) {
            return filter.getValues();
        }

        Set<Object> set = filterMap.get(filter);
        if (set != null) {
            return set;
        }
        set = new HashSet<Object>();

        if (filter.hasDynamicParams()) {
            if (filter.getSetParam() != null) {
                set = (Set<Object>) args.get(filter.getSetParam().getId());
                filterMap.put(filter, set);
                return set;
            }
            for (Object obj : filter.getValues()) {
                if (obj instanceof LurqlDynamicParam) {
                    LurqlDynamicParam param = (LurqlDynamicParam) obj;
                    set.add(args.get(param.getId()));
                } else {
                    set.add(obj);
                }
            }
            filterMap.put(filter, set);
            return set;
        }

        if (sqlConnection == null) {
            throw plan.newException("no SQL connection available");
        }
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
        filterMap.put(filter, set);

        return set;
    }

    private Set<RefObject> findResultSet(
        Map<LurqlPlanVertex, Set<RefObject>> map,
        LurqlPlanVertex planVertex)
    {
        return map.get(planVertex);
    }

    private Set<RefObject> getResultSet(
        Map<LurqlPlanVertex, Set<RefObject>> map,
        LurqlPlanVertex planVertex)
    {
        Set<RefObject> set = findResultSet(map, planVertex);
        if (set == null) {
            set = new HashSet<RefObject>();
            map.put(planVertex, set);
        }
        return set;
    }

    private Set findResultSet(LurqlPlanVertex planVertex)
    {
        return findResultSet(vertexToResultMap, planVertex);
    }

    private Set<RefObject> getResultSet(LurqlPlanVertex planVertex)
    {
        return getResultSet(vertexToResultMap, planVertex);
    }
}

// End LurqlReflectiveExecutor.java
