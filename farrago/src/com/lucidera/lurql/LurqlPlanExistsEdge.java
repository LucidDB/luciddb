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
package com.lucidera.lurql;

import java.io.*;

import java.util.*;

import javax.jmi.model.*;

import org.jgrapht.*;

import org.eigenbase.jmi.*;


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

    //~ Constructors -----------------------------------------------------------

    LurqlPlanExistsEdge(
        LurqlPlanVertex source,
        LurqlPlanVertex target,
        DirectedGraph subgraph,
        Set projectSet)
    {
        super(source, target);

        this.subgraph = subgraph;
        this.projectSet = projectSet;

        StringBuffer sb = new StringBuffer();
        sb.append(getPlanSource().getName());
        sb.append("->exists");
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
}

// End LurqlPlanExistsEdge.java
