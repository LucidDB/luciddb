/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/
package net.sf.farrago.session;

import net.sf.farrago.fennel.*;
import net.sf.farrago.util.*;


/**
 * FarragoSessionRuntimeContext defines runtime support routines needed by
 * generated code.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSessionRuntimeContext extends FarragoAllocationOwner
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Loads the Fennel portion of an execution plan (either creating
     * a new XO graph or reusing a cached instance).
     *
     * @param xmiFennelPlan XMI representation of plan definition
     */
    public void loadFennelPlan(final String xmiFennelPlan);

    /**
     * Opens all streams, including the Fennel portion of the execution plan.
     * This should only be called after all Java TupleStreams have been
     * created.
     */
    public void openStreams();

    /**
     * @return FennelStreamGraph pinned by loadFennelPlan
     */
    public FennelStreamGraph getFennelStreamGraph();
}


// End FarragoSessionRuntimeContext.java
