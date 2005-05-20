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

import java.util.*;
import java.io.*;

import javax.jmi.model.*;

import org.eigenbase.jmi.*;

import org._3pq.jgrapht.edge.*;

/**
 * LurqlPlanEdge is an edge in a LURQL plan graph.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LurqlPlanEdge extends DirectedEdge
{
    public static final LurqlPlanEdge [] EMPTY_ARRAY = new LurqlPlanEdge[0];
    
    /**
     * The model edge representing the association to be traversed.
     */
    private final JmiAssocEdge assocEdge;

    /**
     * The end number (0 or 1) of the origin of the traversal.
     */
    private final int iOriginEnd;

    /**
     * If non-null, traverse to only those destination objects which
     * instantiate the given class.
     */
    private final JmiClassVertex destinationTypeFilter;

    /**
     * String representation of this edge.
     */
    private final String stringRep;

    LurqlPlanEdge(
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

    public LurqlPlanVertex getPlanSource()
    {
        return (LurqlPlanVertex) getSource();
    }
    
    public LurqlPlanVertex getPlanTarget()
    {
        return (LurqlPlanVertex) getTarget();
    }

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
    
    public String toString()
    {
        return stringRep;
    }

    public boolean equals(Object obj)
    {
        if (!(obj instanceof LurqlPlanEdge)) {
            return false;
        }
        return stringRep.equals(obj.toString());
    }

    public int hashCode()
    {
        return stringRep.hashCode();
    }
}

// End LurqlPlanEdge.java
