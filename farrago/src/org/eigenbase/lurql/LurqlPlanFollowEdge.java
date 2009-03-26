/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2009 The Eigenbase Project
// Copyright (C) 2009-2009 SQLstream, Inc.
// Copyright (C) 2006-2009 LucidEra, Inc.
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
