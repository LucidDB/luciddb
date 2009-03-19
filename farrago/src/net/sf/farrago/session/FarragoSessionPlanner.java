/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2004-2007 The Eigenbase Project
// Copyright (C) 2004-2007 SQLstream, Inc.
// Copyright (C) 2004-2007 LucidEra, Inc.
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
package net.sf.farrago.session;

import org.eigenbase.relopt.*;


/**
 * FarragoSessionPlanner represents a query planner/optimizer associated with a
 * specific FarragoPreparingStmt.
 *
 * @author stephan
 * @version $Id$
 */
public interface FarragoSessionPlanner
    extends RelOptPlanner
{
    //~ Methods ----------------------------------------------------------------

    /**
     * @return the FarragoSessionPreparingStmt associated with this planner.
     */
    public FarragoSessionPreparingStmt getPreparingStmt();

    /**
     * Notifies this planner that registration for a particular SQL/MED plugin
     * is about to start, meaning the plugin might call the planner via methods
     * such as {@link RelOptPlanner#addRule}.
     *
     * @param serverClassName name of class implementing FarragoMedDataServer
     */
    public void beginMedPluginRegistration(String serverClassName);

    /**
     * Notifies this planner that registration has ended for the SQL/MED plugin
     * whose identity was last passed to beginMedPluginRegistration.
     */
    public void endMedPluginRegistration();
}

// End FarragoSessionPlanner.java
