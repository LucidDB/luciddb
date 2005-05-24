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

import org.eigenbase.util.*;
import org.eigenbase.jmi.*;

import java.util.*;
import java.io.*;

import javax.jmi.reflect.*;
import javax.jmi.model.*;

/**
 * LurqlPlan represents a prepared plan for executing a LURQL query.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LurqlPlan
{
    /** Map from alias name to corresponding LurqlPathBranch */
    private final Map aliasToBranchMap;
    
    /** All variables part of a recursion */
    private final Set recVars;
	
    /** The model view we are querying */
    private final JmiModelView modelView;

    /** Query we've planned to execute */
    private final LurqlQuery query;
	
    /** Directed acyclic graph of LurqlPlanVertexes */
    private final DirectedGraph graph;

    /** All project elements, keyed by alias */
    private Set projectSet;

    private int idGen;

    private Map paramMap;

    public LurqlPlan(
        JmiModelView modelView,
        LurqlQuery query)
        throws JmiQueryException
    {
        this.modelView = modelView;
        this.query = query;
        aliasToBranchMap = new HashMap();
        recVars = new HashSet();
        graph = new DirectedMultigraph();
        idGen = 0;
        paramMap = new HashMap();
        
        prepareQuery();
    }

    public JmiModelView getModelView()
    {
        return modelView;
    }

    public boolean isSelectAll()
    {
        return projectSet == null;
    }

    public boolean isSelected(String varName)
    {
        return isSelectAll() || projectSet.contains(varName);
    }

    public void explain(PrintWriter pw)
    {
        List list = new ArrayList();
        list.addAll(graph.vertexSet());
        list.addAll(graph.edgeSet());
        Collections.sort(list, new StringRepresentationComparator());
        Iterator iter = list.iterator();
        while (iter.hasNext()) {
            pw.println(iter.next());
        }
        pw.println();
    }

    void prepareQuery()
        throws JmiQueryException
    {
        if (query.getRoot() instanceof LurqlRoot) {
            prepareRoot((LurqlRoot) query.getRoot(), new ArrayList());
        } else {
            preparePathSpec(
                Collections.EMPTY_LIST,
                (LurqlPathSpec) query.getRoot(),
                new ArrayList());
        }
        
        if ((query.getSelectList().size() == 1)
            && (query.getSelectList().get(0).equals("*")))
        {
            projectSet = null;
        } else {
            projectSet = new HashSet(query.getSelectList());
            List list = new ArrayList(query.getSelectList());
            list.removeAll(aliasToBranchMap.keySet());
            if (!list.isEmpty()) {
                throw newException(
                    "unknown alias reference in select:  " + list);
            }
        }

        // TODO:  pruneQuery()?
    }

    public DirectedGraph getGraph()
    {
        return new UnmodifiableDirectedGraph(graph);
    }

    public Map getParamMap()
    {
        return Collections.unmodifiableMap(paramMap);
    }

    private void prepareRoot(LurqlRoot root, List leafVertexList)
        throws JmiQueryException
    {
        Set rootObjectIds = Collections.EMPTY_SET;
        JmiClassVertex classVertex = findClassVertex(root.getClassName());
        
        LurqlPlanVertex planVertex = newPlanVertex(
            root, rootObjectIds);

        planVertex.addClassVertex(classVertex);
        addFilters(planVertex, root.getFilterList());
        planVertex.freeze();
        
        preparePathSpec(
            Collections.singletonList(planVertex),
            root.getThenSpec(),
            leafVertexList);
    }

    private LurqlPlanVertex newPlanVertex(
        LurqlPathBranch branch, Set rootObjectIds)
        throws JmiQueryException
    {
        String name = validateAlias(branch);
        LurqlPlanVertex planVertex = new LurqlPlanVertex(
            this, name, branch.getAliasName(), rootObjectIds);
        if (branch.getAliasName() != null) {
            aliasToBranchMap.put(branch.getAliasName(), branch);
        }
        graph.addVertex(planVertex);
        return planVertex;
    }

    private void preparePathSpec(
        List parentVertexList,
        LurqlPathSpec pathSpec,
        List leafVertexList)
        throws JmiQueryException
    {
        if (pathSpec == null) {
            leafVertexList.addAll(parentVertexList);
            return;
        }
        
        List mergedLeaves = leafVertexList;
        if (pathSpec.isGather()) {
            mergedLeaves = new ArrayList();
        }

        Iterator iter = pathSpec.getBranches().iterator();
        while (iter.hasNext()) {
            Object obj = iter.next();
            if (obj instanceof LurqlFollow) {
                if (parentVertexList.isEmpty()) {
                    throw newException(
                        "follow requires at least one parent");
                }
                prepareFollow(
                    (LurqlFollow) obj,
                    parentVertexList,
                    mergedLeaves);
            } else if (obj instanceof LurqlRoot) {
                if (!parentVertexList.isEmpty()) {
                    throw newException(
                        "root cannot have a parent");
                }
                prepareRoot((LurqlRoot) obj, mergedLeaves);
            } else if (obj instanceof LurqlPathSpec) {
                preparePathSpec(
                    parentVertexList,
                    (LurqlPathSpec) obj,
                    leafVertexList);
            } else {
                if (parentVertexList.size() != 1) {
                    throw newException(
                        "recursion must have exactly one parent");
                }
                prepareRecurse(
                    (LurqlRecurse) obj,
                    parentVertexList,
                    mergedLeaves);
            }
        }
        
        if (pathSpec.isGather()) {
            if (pathSpec.isGatherParent()) {
                if (parentVertexList.isEmpty()) {
                    throw newException(
                        "with parent requires at least one parent");
                }
                mergedLeaves.addAll(parentVertexList);
            }
            preparePathSpec(
                mergedLeaves, pathSpec.getGatherThenSpec(), leafVertexList);
        }
    }

    private void prepareRecurse(
        LurqlRecurse recurse,
        List parentVertexList,
        List leafVertexList)
        throws JmiQueryException
    {
        assert(parentVertexList.size() == 1);
        
        // This map keeps track of what we see while expanding recursion so
        // that we can detect a model fixpoint.  The key is the class set
        // associated with a LurqlPlanVertex; the value is the LurqlPlanVertex
        // itself.
        Map fixpointMap = new HashMap();
        for (;;) {
            List recursionLeaves = new ArrayList();
            preparePathSpec(
                parentVertexList,
                recurse.getPathSpec(),
                recursionLeaves);
            if (recursionLeaves.size() > 1) {
                throw newException(
                    "recursion does not support trees");
            }
            if (recursionLeaves.isEmpty()) {
                break;
            }
            LurqlPlanVertex leafVertex = (LurqlPlanVertex)
                recursionLeaves.get(0);
            Set classVertexSet = leafVertex.getClassVertexSet();
            LurqlPlanVertex recursionVertex = (LurqlPlanVertex)
                fixpointMap.get(classVertexSet);
            if (recursionVertex != null) {
                // We have reached a "model fixpoint"; further expansion would
                // go into an infinite loop.  Instead, set up the structure
                // necessary to perform recursion at runtime, and terminate
                // model expansion.
                leafVertex.setRecursionRoot(recursionVertex);
                break;
            } else {
                // Remember the model subset represented by this new vertex.
                fixpointMap.put(classVertexSet, leafVertex);
            }
            parentVertexList = recursionLeaves;
        }

        // TODO: figure out what recursion results to use as
        // input to THEN clause
        
        preparePathSpec(
            parentVertexList,
            recurse.getThenSpec(),
            leafVertexList);
    }

    private void prepareFollow(
        LurqlFollow follow,
        List parentVertexList,
        List leafVertexList)
        throws JmiQueryException
    {
        LurqlPlanVertex planVertex = newPlanVertex(
            follow, Collections.EMPTY_SET);

        Iterator iter = parentVertexList.iterator();
        while (iter.hasNext()) {
            LurqlPlanVertex parentVertex = (LurqlPlanVertex) iter.next();
            prepareFollowEdges(parentVertex, planVertex, follow);
        }
        
        addFilters(planVertex, follow.getFilterList());
        planVertex.freeze();

        if (planVertex.getClassVertexSet().isEmpty()) {
            // prune this empty tip
            graph.removeVertex(planVertex);
            return;
        }
        
        preparePathSpec(
            Collections.singletonList(planVertex), 
            follow.getThenSpec(),
            leafVertexList);
    }

    private void prepareFollowEdges(
        LurqlPlanVertex sourceVertex, 
        LurqlPlanVertex targetVertex, 
        LurqlFollow follow)
        throws JmiQueryException
    {
        boolean forward = 
            follow.getAssociationFilters().containsKey(LurqlFollow.AF_FORWARD);
        boolean backward = 
            follow.getAssociationFilters().containsKey(LurqlFollow.AF_BACKWARD);

        // When no direction is specified, it means both, not neither.
        if (!forward && !backward) {
            forward = true;
            backward = true;
        }

        // If origin and/or destination classes are specified, use them to
        // filter out associations
        Map assocFilters = follow.getAssociationFilters();
        String originClassName = (String) assocFilters.get(
            LurqlFollow.AF_ORIGIN_CLASS);
        String destinationClassName = (String) assocFilters.get(
            LurqlFollow.AF_DESTINATION_CLASS);

        Collection outgoingFilterEdgeSet = null;
        Collection incomingFilterEdgeSet = null;
        if (originClassName != null) {
            JmiClassVertex originClassVertex = findClassVertex(originClassName);
            outgoingFilterEdgeSet = modelView.getAllOutgoingAssocEdges(
                originClassVertex);
            incomingFilterEdgeSet = modelView.getAllIncomingAssocEdges(
                originClassVertex);
        }

        JmiClassVertex destinationClassVertex = null;
        if (destinationClassName != null) {
            destinationClassVertex = findClassVertex(destinationClassName);
            // from the point of view of the destination, the incoming/outgoing
            // sense is reversed
            Set outgoingFilterEdgeSet2 =
                modelView.getAllIncomingAssocEdges(
                    destinationClassVertex);
            Set incomingFilterEdgeSet2 =
                modelView.getAllOutgoingAssocEdges(
                    destinationClassVertex);
            if (originClassName == null) {
                // no intersection required
                outgoingFilterEdgeSet = outgoingFilterEdgeSet2;
                incomingFilterEdgeSet = incomingFilterEdgeSet2;
            } else {
                // intersect
                outgoingFilterEdgeSet = new ArrayList(outgoingFilterEdgeSet);
                outgoingFilterEdgeSet.retainAll(outgoingFilterEdgeSet2);
                incomingFilterEdgeSet = new ArrayList(incomingFilterEdgeSet);
                incomingFilterEdgeSet.retainAll(incomingFilterEdgeSet2);
            }
        }

        Iterator sourceClassIter = sourceVertex.getClassVertexSet().iterator();
        while (sourceClassIter.hasNext()) {
            JmiClassVertex sourceClassVertex =
                (JmiClassVertex) sourceClassIter.next();
            if (forward) {
                addTraversals(
                    sourceVertex,
                    targetVertex, 
                    follow,
                    filterEdgeSet(
                        modelView.getAllOutgoingAssocEdges(sourceClassVertex),
                        outgoingFilterEdgeSet),
                    destinationClassVertex,
                    0);
            }
            if (backward) {
                addTraversals(
                    sourceVertex,
                    targetVertex, 
                    follow,
                    filterEdgeSet(
                        modelView.getAllIncomingAssocEdges(sourceClassVertex),
                        incomingFilterEdgeSet),
                    destinationClassVertex,
                    1);
            }
        }
    }

    private Collection filterEdgeSet(
        Collection edgeSet,
        Collection filterEdgeSet)
    {
        if (filterEdgeSet == null) {
            return edgeSet;
        }

        // intersect
        List list = new ArrayList(edgeSet);
        list.retainAll(filterEdgeSet);
        return list;
    }

    private void addTraversals(
        LurqlPlanVertex sourceVertex, 
        LurqlPlanVertex targetVertex, 
        LurqlFollow follow,
        Collection assocEdges,
        JmiClassVertex destinationClassVertex,
        int iOriginEnd)
        throws JmiQueryException
    {
        Map assocFilters = follow.getAssociationFilters();

        String assocName = (String) assocFilters.get(
            LurqlFollow.AF_ASSOCIATION);

        String originEndName = (String) assocFilters.get(
            LurqlFollow.AF_ORIGIN_END);

        String destinationEndName = (String) assocFilters.get(
            LurqlFollow.AF_DESTINATION_END);

        boolean composite = assocFilters.containsKey(
            LurqlFollow.AF_COMPOSITE);
        boolean noncomposite = assocFilters.containsKey(
            LurqlFollow.AF_NONCOMPOSITE);

        // When composite is unspecified, it means any, not none.
        if (!composite && !noncomposite) {
            composite = true;
            noncomposite = true;
        }
        
        Iterator iter = assocEdges.iterator();
        while (iter.hasNext()) {
            JmiAssocEdge assocEdge = (JmiAssocEdge) iter.next();
            Association mofAssoc = assocEdge.getMofAssoc();
            if (!testAssocFilter(assocName, mofAssoc.getName())) {
                continue;
            }
            
            AssociationEnd originEnd = assocEdge.getEnd(iOriginEnd);
            AssociationEnd destinationEnd = assocEdge.getEnd(1 - iOriginEnd);

            if ((originEnd.getAggregation() == AggregationKindEnum.COMPOSITE)
                || (destinationEnd.getAggregation()
                    == AggregationKindEnum.COMPOSITE))
            {
                if (!composite) {
                    continue;
                }
            } else {
                if (!noncomposite) {
                    continue;
                }
            }

            if (!testAssocEnd(originEnd, originEndName)) {
                continue;
            }

            if (!testAssocEnd(destinationEnd, destinationEndName)) {
                continue;
            }

            JmiClassVertex destinationEndVertex = 
                modelView.getModelGraph().getVertexForMofClass(
                    (MofClass) destinationEnd.getType());

            JmiClassVertex destinationTypeFilter = null;
            if (destinationClassVertex != null) {
                if (destinationClassVertex == destinationEndVertex) {
                    // nothing to do
                } else if (modelView.getAllSuperclassVertices(
                               destinationClassVertex).contains(
                                   destinationEndVertex))
                {
                    // the end is a superclass of the requested class,
                    // so we'll need to filter during execution
                    destinationTypeFilter = destinationClassVertex;
                    // also record the refinement in the target plan vertex
                    destinationEndVertex = destinationClassVertex;
                }
            }
            
            LurqlPlanEdge edge = new LurqlPlanEdge(
                sourceVertex,
                targetVertex,
                assocEdge,
                iOriginEnd,
                destinationTypeFilter);
            graph.addEdge(edge);

            targetVertex.addClassVertex(destinationEndVertex);
        }
    }

    private JmiClassVertex findClassVertex(String className)
        throws JmiQueryException
    {
        JmiClassVertex classVertex =
            modelView.getModelGraph().getVertexForClassName(className);
        if (classVertex == null) {
            throw newException("unknown class " + className);
        }
        return classVertex;
    }

    private boolean testAssocEnd(
        AssociationEnd end,
        String endFilterValue)
    {
        if (!testAssocFilter(endFilterValue, end.getName())) {
            return false;
        }
        return true;
    }

    private boolean testAssocFilter(
        String filterValue, 
        String actualValue)
    {
        if (filterValue != null) {
            if (!filterValue.equals(actualValue)) {
                return false;
            }
        }
        return true;
    }

    JmiQueryException newException(String err)
    {
        // FIXME:  i18n everywhere this is used
        return new JmiQueryException(err);
    }

    JmiQueryException newException(String err, Throwable cause)
    {
        return new JmiQueryException(err, cause);
    }

    private void addFilters(LurqlPlanVertex planVertex, List filters)
        throws JmiQueryException
    {
        Iterator iter = filters.iterator();
        while (iter.hasNext()) {
            LurqlFilter filter = (LurqlFilter) iter.next();
            if (!filter.hasDynamicParams()) {
                continue;
            }
            if (filter.getSetParam() != null) {
                addParam(filter.getSetParam(), Set.class);
            } else {
                Iterator valuesIter = filter.getValues().iterator();
                while (valuesIter.hasNext()) {
                    Object obj = valuesIter.next();
                    if (obj instanceof LurqlDynamicParam) {
                        addParam((LurqlDynamicParam) obj, String.class);
                    }
                }
            }
        }
        
        planVertex.addFilters(filters);
    }

    private void addParam(LurqlDynamicParam param, Class paramType)
        throws JmiQueryException
    {
        Object obj = paramMap.get(param.getId());
        if (obj != null) {
            if (obj != paramType) {
                throw newException(
                    "conflicting type for parameter " + param.getId());
            }
        }
        paramMap.put(param.getId(), paramType);
    }

    private String validateAlias(LurqlPathBranch branch)
        throws JmiQueryException
    {
        ++idGen;
        if (branch.getAliasName() != null) {
            Object obj = aliasToBranchMap.get(branch.getAliasName());
            if ((obj != null) && (obj != branch)) {
                throw newException("duplicate definition for alias "
                    + branch.getAliasName());
            }
            return branch.getAliasName() + "_" + idGen;
        }
        return "anon_" + idGen;
    }
}

// End LurqlPlan.java
