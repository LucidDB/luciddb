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


/**
 * LurqlPlanEdge is a follow edge in a LURQL plan graph.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LurqlPlanFollowEdge
    extends LurqlPlanEdge
{
    //~ Instance fields --------------------------------------------------------

    /**
     * The model edge representing the association to be traversed.
     */
    private final JmiAssocEdge assocEdge;

    /**
     * The end number (0 or 1) of the origin of the traversal.
     */
    private final int iOriginEnd;

    /**
     * If non-null, traverse to only those destination objects which instantiate
     * the given class.
     */
    private final JmiClassVertex destinationTypeFilter;

    //~ Constructors -----------------------------------------------------------

    LurqlPlanFollowEdge(
        LurqlPlanVertex source,
        LurqlPlanVertex target,
        JmiAssocEdge assocEdge,
        int iOriginEnd,
        JmiClassVertex destinationTypeFilter)
    {
        super(source, target);

        this.assocEdge = assocEdge;
        this.iOriginEnd = iOriginEnd;
        this.destinationTypeFilter = destinationTypeFilter;

        StringBuffer sb = new StringBuffer();
        sb.append(getPlanSource().getName());
        sb.append(":");
        sb.append(getOriginEnd().getName());
        sb.append("->");
        sb.append(getAssocEdge().getMofAssoc().getName());
        sb.append("->");
        sb.append(getPlanTarget().getName());
        sb.append(":");
        sb.append(getDestinationEnd().getName());
        if (destinationTypeFilter != null) {
            sb.append(" { ");
            sb.append(destinationTypeFilter);
            sb.append(" }");
        }
        stringRep = sb.toString();
    }

    //~ Methods ----------------------------------------------------------------

    public JmiAssocEdge getAssocEdge()
    {
        return assocEdge;
    }

    public JmiClassVertex getDestinationTypeFilter()
    {
        return destinationTypeFilter;
    }

    public AssociationEnd getOriginEnd()
    {
        return assocEdge.getEnd(iOriginEnd);
    }

    public AssociationEnd getDestinationEnd()
    {
        return assocEdge.getEnd(1 - iOriginEnd);
    }
}

// End LurqlPlanFollowEdge.java
