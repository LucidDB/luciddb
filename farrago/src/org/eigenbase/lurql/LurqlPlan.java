/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2009-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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
package org.eigenbase.lurql;

import java.io.*;

import java.util.*;

import javax.jmi.model.*;

import org.eigenbase.jmi.*;
import org.eigenbase.util.*;

import org.jgrapht.*;
import org.jgrapht.graph.*;


/**
 * LurqlPlan represents a prepared plan for executing a LURQL query.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LurqlPlan
{
    //~ Instance fields --------------------------------------------------------

    /**
     * Map from alias name to corresponding LurqlPathBranch
     */
    private final Map<String, LurqlPathBranch> aliasToBranchMap;

    /**
     * All variables part of a recursion
     */
    private final Set recVars;

    /**
     * The model view we are querying
     */
    private final JmiModelView modelView;

    /**
     * Query we've planned to execute
     */
    private final LurqlQuery query;

    /**
     * Directed acyclic graph of {@link LurqlPlanVertex}es.
     */
    private final DirectedGraph<LurqlPlanVertex, LurqlPlanEdge> graph;

    /**
     * All project elements, keyed by alias
     */
    private Set<String> projectSet;

    /**
     * For select *, all vertices which are defined inside of exists (meaning
     * they should not contribute to the final result).
     */
    private Set<LurqlPlanVertex> existsSet;

    private int idGen;

    private Map<String, Class> paramMap;

    private int existsDepth;

    //~ Constructors -----------------------------------------------------------

    public LurqlPlan(
        JmiModelView modelView,
        LurqlQuery query)
        throws JmiQueryException
    {
        this.modelView = modelView;
        this.query = query;
        aliasToBranchMap = new LinkedHashMap<String, LurqlPathBranch>();
        recVars = new LinkedHashSet();
        graph =
            new DirectedMultigraph<LurqlPlanVertex, LurqlPlanEdge>(
                LurqlPlanEdge.class);
        idGen = 0;
        paramMap = new LinkedHashMap<String, Class>();
        existsSet = new HashSet<LurqlPlanVertex>();

        prepareQuery();
    }

    //~ Methods ----------------------------------------------------------------

    public JmiModelView getModelView()
    {
        return modelView;
    }

    public boolean isSelectAll()
    {
        return projectSet == null;
    }

    public boolean isSelected(LurqlPlanVertex vertex)
    {
        if (isSelectAll()) {
            return !existsSet.contains(vertex);
        } else {
            return projectSet.contains(vertex.getAlias());
        }
    }

    public void explain(PrintWriter pw)
    {
        List<Object> list = new ArrayList<Object>();
        list.addAll(graph.vertexSet());
        list.addAll(graph.edgeSet());
        Collections.sort(
            list,
            new StringRepresentationComparator<Object>());
        for (Object o : list) {
            pw.println(o);
        }
        pw.println();
    }

    void prepareQuery()
        throws JmiQueryException
    {
        if (query.getRoot() instanceof LurqlRoot) {
            prepareRoot(
                (LurqlRoot) query.getRoot(),
                new ArrayList<LurqlPlanVertex>());
        } else {
            preparePathSpec(
                Collections.EMPTY_LIST,
                (LurqlPathSpec) query.getRoot(),
                new ArrayList<LurqlPlanVertex>());
        }

        if (isStar(query.getSelectList())) {
            projectSet = null;
        } else {
            projectSet = new LinkedHashSet<String>(query.getSelectList());
            List<String> list = new ArrayList<String>(query.getSelectList());
            list.removeAll(aliasToBranchMap.keySet());
            if (!list.isEmpty()) {
                throw newException(
                    "unknown alias reference in select:  " + list);
            }
        }
    }

    private boolean isStar(List<String> selectList)
    {
        return ((selectList.size() == 1)
            && (selectList.get(0).equals("*")));
    }

    public DirectedGraph<LurqlPlanVertex, LurqlPlanEdge> getGraph()
    {
        return new UnmodifiableDirectedGraph<LurqlPlanVertex, LurqlPlanEdge>(
            graph);
    }

    public Map<String, Class> getParamMap()
    {
        return Collections.unmodifiableMap(paramMap);
    }

    private void prepareRoot(
        LurqlRoot root,
        List<LurqlPlanVertex> leafVertexList)
        throws JmiQueryException
    {
        Set<String> rootObjectIds = Collections.emptySet();
        JmiClassVertex classVertex = findClassVertex(root.getClassName());

        LurqlPlanVertex planVertex =
            newPlanVertex(
                root,
                rootObjectIds);

        planVertex.addClassVertex(classVertex);
        addFilters(
            planVertex,
            root.getFilterList());
        planVertex.freeze();

        preparePathSpec(
            Collections.singletonList(planVertex),
            root.getThenSpec(),
            leafVertexList);
    }

    private LurqlPlanVertex newPlanVertex(
        LurqlPathBranch branch,
        Set<String> rootObjectIds)
        throws JmiQueryException
    {
        String name = validateAlias(branch);
        LurqlPlanVertex planVertex =
            new LurqlPlanVertex(
                this,
                name,
                branch.getAliasName(),
                rootObjectIds);
        if (branch.getAliasName() != null) {
            aliasToBranchMap.put(
                branch.getAliasName(),
                branch);
        }
        if (existsDepth > 0) {
            existsSet.add(planVertex);
        }
        graph.addVertex(planVertex);
        return planVertex;
    }

    private void preparePathSpec(
        List<LurqlPlanVertex> parentVertexList,
        LurqlPathSpec pathSpec,
        List<LurqlPlanVertex> leafVertexList)
        throws JmiQueryException
    {
        if (pathSpec == null) {
            leafVertexList.addAll(parentVertexList);
            return;
        }

        List<LurqlPlanVertex> mergedLeaves = leafVertexList;
        if (pathSpec.isGather()) {
            mergedLeaves = new ArrayList<LurqlPlanVertex>();
        }

        for (LurqlQueryNode branch : pathSpec.getBranches()) {
            if (branch instanceof LurqlFollow) {
                if (parentVertexList.isEmpty()) {
                    throw newException(
                        "follow requires at least one parent");
                }
                prepareFollow(
                    (LurqlFollow) branch,
                    parentVertexList,
                    mergedLeaves);
            } else if (branch instanceof LurqlRoot) {
                if (!parentVertexList.isEmpty()) {
                    throw newException(
                        "root cannot have a parent");
                }
                prepareRoot((LurqlRoot) branch, mergedLeaves);
            } else if (branch instanceof LurqlPathSpec) {
                preparePathSpec(
                    parentVertexList,
                    (LurqlPathSpec) branch,
                    leafVertexList);
            } else {
                if (parentVertexList.size() != 1) {
                    throw newException(
                        "recursion must have exactly one parent");
                }
                prepareRecurse(
                    (LurqlRecurse) branch,
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
                mergedLeaves,
                pathSpec.getGatherThenSpec(),
                leafVertexList);
        }
    }

    private void prepareRecurse(
        LurqlRecurse recurse,
        List<LurqlPlanVertex> parentVertexList,
        List<LurqlPlanVertex> leafVertexList)
        throws JmiQueryException
    {
        assert (parentVertexList.size() == 1);

        // This map keeps track of what we see while expanding recursion so
        // that we can detect a model fixpoint.  The key is the class set
        // associated with a LurqlPlanVertex; the value is the LurqlPlanVertex
        // itself.
        Map<Set<JmiClassVertex>, LurqlPlanVertex> fixpointMap =
            new HashMap<Set<JmiClassVertex>, LurqlPlanVertex>();
        for (;;) {
            List<LurqlPlanVertex> recursionLeaves =
                new ArrayList<LurqlPlanVertex>();
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
            LurqlPlanVertex leafVertex = recursionLeaves.get(0);
            Set<JmiClassVertex> classVertexSet = leafVertex.getClassVertexSet();
            LurqlPlanVertex recursionVertex = fixpointMap.get(classVertexSet);
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
        List<LurqlPlanVertex> parentVertexList,
        List<LurqlPlanVertex> leafVertexList)
        throws JmiQueryException
    {
        LurqlPlanVertex planVertex =
            newPlanVertex(
                follow,
                Collections.EMPTY_SET);

        for (LurqlPlanVertex parentVertex : parentVertexList) {
            prepareFollowEdges(parentVertex, planVertex, follow);
        }

        addFilters(
            planVertex,
            follow.getFilterList());
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
        Map<String, String> assocFilters = follow.getAssociationFilters();
        String originClassName =
            (String) assocFilters.get(
                LurqlFollow.AF_ORIGIN_CLASS);
        String destinationClassName =
            (String) assocFilters.get(
                LurqlFollow.AF_DESTINATION_CLASS);

        Collection<JmiAssocEdge> outgoingFilterEdgeSet = null;
        Collection<JmiAssocEdge> incomingFilterEdgeSet = null;
        if (originClassName != null) {
            JmiClassVertex originClassVertex = findClassVertex(originClassName);
            outgoingFilterEdgeSet =
                modelView.getAllOutgoingAssocEdges(
                    originClassVertex);
            incomingFilterEdgeSet =
                modelView.getAllIncomingAssocEdges(
                    originClassVertex);
        }

        JmiClassVertex destinationClassVertex = null;
        if (destinationClassName != null) {
            destinationClassVertex = findClassVertex(destinationClassName);

            // from the point of view of the destination, the incoming/outgoing
            // sense is reversed
            Set<JmiAssocEdge> outgoingFilterEdgeSet2 =
                modelView.getAllIncomingAssocEdges(
                    destinationClassVertex);
            Set<JmiAssocEdge> incomingFilterEdgeSet2 =
                modelView.getAllOutgoingAssocEdges(
                    destinationClassVertex);
            if (originClassName == null) {
                // no intersection required
                outgoingFilterEdgeSet = outgoingFilterEdgeSet2;
                incomingFilterEdgeSet = incomingFilterEdgeSet2;
            } else {
                // intersect
                outgoingFilterEdgeSet =
                    new ArrayList<JmiAssocEdge>(outgoingFilterEdgeSet);
                outgoingFilterEdgeSet.retainAll(outgoingFilterEdgeSet2);
                incomingFilterEdgeSet =
                    new ArrayList<JmiAssocEdge>(incomingFilterEdgeSet);
                incomingFilterEdgeSet.retainAll(incomingFilterEdgeSet2);
            }
        }

        for (
            JmiClassVertex sourceClassVertex : sourceVertex.getClassVertexSet())
        {
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

    private Collection<JmiAssocEdge> filterEdgeSet(
        Collection<JmiAssocEdge> edgeSet,
        Collection<JmiAssocEdge> filterEdgeSet)
    {
        if (filterEdgeSet == null) {
            return edgeSet;
        }

        // intersect
        List<JmiAssocEdge> list = new ArrayList<JmiAssocEdge>(edgeSet);
        list.retainAll(filterEdgeSet);
        return list;
    }

    private void addTraversals(
        LurqlPlanVertex sourceVertex,
        LurqlPlanVertex targetVertex,
        LurqlFollow follow,
        Collection<JmiAssocEdge> assocEdges,
        JmiClassVertex destinationClassVertex,
        int iOriginEnd)
        throws JmiQueryException
    {
        Map<String, String> assocFilters = follow.getAssociationFilters();

        String assocName =
            assocFilters.get(
                LurqlFollow.AF_ASSOCIATION);

        String originEndName =
            assocFilters.get(
                LurqlFollow.AF_ORIGIN_END);

        String destinationEndName =
            assocFilters.get(
                LurqlFollow.AF_DESTINATION_END);

        boolean composite =
            assocFilters.containsKey(
                LurqlFollow.AF_COMPOSITE);
        boolean noncomposite =
            assocFilters.containsKey(
                LurqlFollow.AF_NONCOMPOSITE);

        // When composite is unspecified, it means any, not none.
        if (!composite && !noncomposite) {
            composite = true;
            noncomposite = true;
        }

        for (JmiAssocEdge assocEdge : assocEdges) {
            Association mofAssoc = assocEdge.getMofAssoc();
            if (!testAssocFilter(
                    assocName,
                    mofAssoc.getName()))
            {
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
                } else if (
                    modelView.getAllSuperclassVertices(
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

            LurqlPlanFollowEdge edge =
                new LurqlPlanFollowEdge(
                    sourceVertex,
                    targetVertex,
                    assocEdge,
                    iOriginEnd,
                    destinationTypeFilter);
            graph.addEdge(sourceVertex, targetVertex, edge);

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
        if (!testAssocFilter(
                endFilterValue,
                end.getName()))
        {
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

    private void addFilters(
        LurqlPlanVertex planVertex,
        List<LurqlFilter> filters)
        throws JmiQueryException
    {
        filters = new ArrayList<LurqlFilter>(filters);
        Iterator<LurqlFilter> iter = filters.iterator();
        while (iter.hasNext()) {
            LurqlFilter filter = iter.next();
            LurqlExists exists = filter.getExists();
            if (exists != null) {
                ++existsDepth;
                prepareExists(planVertex, exists, filter.isNegated());
                --existsDepth;

                // don't need exists filter at runtime; edge added
                // by prepareExists will represent it instead
                iter.remove();
                continue;
            }
            if (!filter.hasDynamicParams()) {
                continue;
            }
            if (filter.getSetParam() != null) {
                addParam(
                    filter.getSetParam(),
                    Set.class);
            } else {
                for (Object obj : filter.getValues()) {
                    if (obj instanceof LurqlDynamicParam) {
                        addParam((LurqlDynamicParam) obj, String.class);
                    }
                }
            }
        }

        planVertex.addFilters(filters);
    }

    private void prepareExists(
        LurqlPlanVertex planVertex,
        LurqlExists exists,
        boolean isNegated)
        throws JmiQueryException
    {
        Set<String> existsProjectSet = null;

        // Verify that exists does not reference variables defined
        // outside of it.
        if (!isStar(exists.getSelectList())) {
            existsProjectSet =
                new LinkedHashSet<String>(exists.getSelectList());
            List<String> list = new ArrayList<String>(exists.getSelectList());
            list.retainAll(aliasToBranchMap.keySet());
            if (!list.isEmpty()) {
                throw newException(
                    "exists list aliases defined outside of exists:  " + list);
            }
        }

        // Create a new "root" only reachable explicitly from filtering
        // on planVertex.  We'll give it an empty set of object ID's
        // now, and bind to each real object ID at execution time.
        LurqlRoot dummyRoot =
            new LurqlRoot(
                null,
                null,
                Collections.EMPTY_LIST,
                null);
        LurqlPlanVertex existsRoot =
            newPlanVertex(
                dummyRoot,
                new HashSet<String>());

        // Copy class set from original planVertex.
        for (JmiClassVertex classVertex : planVertex.getClassVertexSet()) {
            existsRoot.addClassVertex(classVertex);
        }

        // Prepare exists subgraph reachable from class set.
        List<LurqlPlanVertex> parentVertexList =
            new ArrayList<LurqlPlanVertex>();
        parentVertexList.add(existsRoot);
        preparePathSpec(
            parentVertexList,
            exists.getPathSpec(),
            new ArrayList<LurqlPlanVertex>());

        // Validate that all variables referenced by exists select list
        // were defined.
        if (!isStar(exists.getSelectList())) {
            List<String> list = new ArrayList<String>(exists.getSelectList());
            list.removeAll(aliasToBranchMap.keySet());
            if (!list.isEmpty()) {
                throw newException(
                    "unknown alias reference in exists:  " + list);
            }
        }

        // Collect subgraph nodes.
        DirectedGraph<LurqlPlanVertex, LurqlPlanEdge> subgraph =
            existsRoot.createReachableSubgraph(false);

        // Attach new vertex with a non-follow edge
        LurqlPlanExistsEdge edge =
            new LurqlPlanExistsEdge(
                planVertex,
                existsRoot,
                subgraph,
                existsProjectSet,
                isNegated);
        graph.addEdge(planVertex, existsRoot, edge);
    }

    private void addParam(LurqlDynamicParam param, Class paramType)
        throws JmiQueryException
    {
        Class obj = paramMap.get(param.getId());
        if (obj != null) {
            if (obj != paramType) {
                throw newException(
                    "conflicting type for parameter " + param.getId());
            }
        }
        paramMap.put(
            param.getId(),
            paramType);
    }

    private String validateAlias(LurqlPathBranch branch)
        throws JmiQueryException
    {
        ++idGen;
        if (branch.getAliasName() != null) {
            LurqlPathBranch obj = aliasToBranchMap.get(branch.getAliasName());
            if ((obj != null) && (obj != branch)) {
                throw newException(
                    "duplicate definition for alias "
                    + branch.getAliasName());
            }
            return branch.getAliasName() + "_" + idGen;
        }
        return "anon_" + idGen;
    }
}

// End LurqlPlan.java
