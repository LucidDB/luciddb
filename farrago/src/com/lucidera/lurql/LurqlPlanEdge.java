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
 * LurqlPlanEdge is a follow edge in a LURQL plan graph.  (TODO:  factor
 * out subclass.)
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LurqlPlanEdge extends DirectedEdge
{
    /**
     * String representation of this edge.
     */
    protected String stringRep;

    LurqlPlanEdge(
        LurqlPlanVertex source,
        LurqlPlanVertex target)
    {
        super(source, target);
    }

    public LurqlPlanVertex getPlanSource()
    {
        return (LurqlPlanVertex) getSource();
    }
    
    public LurqlPlanVertex getPlanTarget()
    {
        return (LurqlPlanVertex) getTarget();
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
