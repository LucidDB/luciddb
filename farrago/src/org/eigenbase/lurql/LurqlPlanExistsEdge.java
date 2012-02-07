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
package org.eigenbase.lurql;

import java.io.*;

import java.util.*;

import javax.jmi.model.*;

import org.eigenbase.jmi.*;

import org.jgrapht.*;


/**
 * LurqlPlanExistsEdge implements the exists predicate within a LURQL plan
 * graph.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LurqlPlanExistsEdge
    extends LurqlPlanEdge
{
    //~ Static fields/initializers ---------------------------------------------

    public static final LurqlPlanExistsEdge [] EMPTY_ARRAY =
        new LurqlPlanExistsEdge[0];

    //~ Instance fields --------------------------------------------------------

    private final DirectedGraph subgraph;

    private final Set projectSet;

    private final boolean isNegated;

    //~ Constructors -----------------------------------------------------------

    LurqlPlanExistsEdge(
        LurqlPlanVertex source,
        LurqlPlanVertex target,
        DirectedGraph subgraph,
        Set projectSet,
        boolean isNegated)
    {
        super(source, target);

        this.subgraph = subgraph;
        this.projectSet = projectSet;
        this.isNegated = isNegated;

        StringBuffer sb = new StringBuffer();
        sb.append(getPlanSource().getName());
        if (isNegated) {
            sb.append("->notexists");
        } else {
            sb.append("->exists");
        }
        if (projectSet != null) {
            sb.append(projectSet.toString());
        }
        sb.append("->");
        sb.append(getPlanTarget().getName());
        stringRep = sb.toString();
    }

    //~ Methods ----------------------------------------------------------------

    DirectedGraph getSubgraph()
    {
        return subgraph;
    }

    Set getProjectSet()
    {
        return projectSet;
    }

    boolean isNegated()
    {
        return isNegated;
    }
}

// End LurqlPlanExistsEdge.java
