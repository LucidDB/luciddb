/*
// $Id$
// Farrago is a relational database management system.
// Copyright (C) 2002-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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
package com.disruptivetech.farrago.volcano;


/**
 * A <code>VolcanoPlannerFactory</code> produces new uninitialized instances
 * of VolcanoPlanner.
 */
public abstract class VolcanoPlannerFactory
{
    //~ Static fields/initializers --------------------------------------------

    // TODO:  get rid of threadInstances and pass PlannerFactory around instead
    private static ThreadLocal threadInstances = new ThreadLocal();

    //~ Methods ---------------------------------------------------------------

    public static void setThreadInstance(VolcanoPlannerFactory plannerFactory)
    {
        threadInstances.set(plannerFactory);
    }

    public static VolcanoPlannerFactory threadInstance()
    {
        return (VolcanoPlannerFactory) threadInstances.get();
    }

    public abstract VolcanoPlanner newPlanner();
}


// End VolcanoPlannerFactory.java
